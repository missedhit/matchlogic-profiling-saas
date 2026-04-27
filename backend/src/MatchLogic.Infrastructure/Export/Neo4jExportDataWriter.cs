using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export.Writers;

/// <summary>
/// Neo4j export writer using the official Neo4j.Driver.
/// Exports match results as graph nodes with optional relationships.
/// Settings read from ConnectionConfig.Parameters using Neo4j.* keys.
/// </summary>
[HandlesExportWriter(DataSourceType.Neo4j)]
public class Neo4jExportDataWriter : BaseExportDataWriter
{
    private readonly Neo4jConnectionConfig _connectionConfig;

    // Settings from Parameters
    private readonly string _exportMode;
    private readonly string _nodeLabel;
    private readonly string _relationshipType;
    private readonly int _batchSize;
    private readonly bool _createIndexes;
    private readonly bool _clearExisting;

    private IDriver? _driver;
    private IAsyncSession? _writeSession;
    private string? _writeCypher;
    private Dictionary<string, string>? _sanitizedColumnNames;

    public override string Name => "Neo4j Writer";
    public override DataSourceType Type => DataSourceType.Neo4j;

    public Neo4jExportDataWriter(ConnectionConfig connectionConfig, Microsoft.Extensions.Logging.ILogger logger)
        : base(logger, 1000)
    {
        _connectionConfig = connectionConfig as Neo4jConnectionConfig
            ?? throw new ArgumentException("Invalid configuration type for Neo4jExportDataWriter", nameof(connectionConfig));

        var p = _connectionConfig.Parameters;

        _exportMode = p.GetString(Neo4jConnectionConfig.ExportModeKey, Neo4jConnectionConfig.DefaultExportMode);
        _nodeLabel = p.GetString(Neo4jConnectionConfig.NodeLabelKey, Neo4jConnectionConfig.DefaultNodeLabel);
        _relationshipType = p.GetString(Neo4jConnectionConfig.RelationshipTypeKey, Neo4jConnectionConfig.DefaultRelationshipType);
        _batchSize = p.GetInt(Neo4jConnectionConfig.BatchSizeKey, Neo4jConnectionConfig.DefaultBatchSize);
        _createIndexes = p.GetBool(Neo4jConnectionConfig.CreateIndexesKey, Neo4jConnectionConfig.DefaultCreateIndexes);
        _clearExisting = p.GetBool(Neo4jConnectionConfig.ClearExistingKey, Neo4jConnectionConfig.DefaultClearExisting);
    }

    #region IExportDataWriter Implementation

    public override async Task InitializeAsync(ExportSchema schema, CancellationToken ct = default)
    {
        await base.InitializeAsync(schema, ct);

        try
        {
            _driver = CreateDriver();

            // Verify connectivity
            await _driver.VerifyConnectivityAsync();
            _logger.LogInformation("Neo4j connection verified: {Uri}", _connectionConfig.Uri);

            var session = _driver.AsyncSession(o => o.WithDatabase(_connectionConfig.Database));
            try
            {
                if (_createIndexes)
                {
                    await CreateIndexesAndConstraintsAsync(session, ct);
                }

                if (_clearExisting)
                {
                    await ClearExistingNodesAsync(session, ct);
                }
            }
            finally
            {
                await session.CloseAsync();
            }

            // Create a reusable session for WriteBatchAsync calls
            _writeSession = _driver.AsyncSession(o => o.WithDatabase(_connectionConfig.Database));

            // Pre-compute sanitized column names to avoid regex on every row
            _sanitizedColumnNames = schema.Columns.ToDictionary(
                c => c.Name, c => SanitizePropertyName(c.Name));

            // Pre-build the Cypher string for node creation (guarantees plan cache hits)
            // Use CREATE when clearExisting (DB was wiped, no duplicates possible) — faster than MERGE
            _writeCypher = _clearExisting
                ? $@"UNWIND $rows AS row
                    CREATE (r:{_nodeLabel})
                    SET r = row"
                : $@"UNWIND $rows AS row
                    MERGE (r:{_nodeLabel} {{_matchlogic_id: row._matchlogic_id}})
                    SET r += row";

            _logger.LogInformation(
                "Neo4j writer initialized: label={Label}, mode={Mode}, columns={Columns}",
                _nodeLabel, _exportMode, schema.Columns.Count);
        }
        catch (Exception ex)
        {
            AddError($"Failed to initialize Neo4j writer: {ex.Message}");
            throw;
        }
    }

    public override async Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        if (_writeSession == null || _schema == null)
            throw new InvalidOperationException("Writer not initialized");

        try
        {
            var sanitizedRows = new List<Dictionary<string, object>>(batch.Count);

            foreach (var row in batch)
            {
                ct.ThrowIfCancellationRequested();
                var sanitized = SanitizeRow(row);
                sanitizedRows.Add(sanitized);
            }

            // Batch upsert nodes using pre-built Cypher — reuse persistent session
            await _writeSession.ExecuteWriteAsync(async tx =>
            {
                await tx.RunAsync(_writeCypher!, new { rows = sanitizedRows });
            });

            _rowsWritten += batch.Count;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            AddError($"Error writing batch at row {_rowsWritten}: {ex.Message}");
            throw;
        }
    }

    public override async Task<ExportWriteResult> FinalizeAsync(CancellationToken ct = default)
    {
        try
        {
            // Close the reusable write session before finalization
            if (_writeSession != null)
            {
                await _writeSession.CloseAsync();
                _writeSession = null;
            }

            // Post-processing: create relationships in graph mode
            if (_exportMode.Equals("graph", StringComparison.OrdinalIgnoreCase) && _driver != null)
            {
                var session = _driver.AsyncSession(o => o.WithDatabase(_connectionConfig.Database));
                try
                {
                    await CreateRelationshipsAsync(session, ct);
                    await CreateGroupSummaryNodesAsync(session, ct);
                }
                finally
                {
                    await session.CloseAsync();
                }
            }

            _logger.LogInformation(
                "Neo4j export completed: {Rows} rows as :{Label} nodes, mode={Mode}",
                _rowsWritten, _nodeLabel, _exportMode);
        }
        catch (Exception ex)
        {
            AddError($"Error finalizing Neo4j export: {ex.Message}");
        }
        finally
        {
            // Always dispose driver regardless of success/failure
            if (_writeSession != null)
            {
                await _writeSession.CloseAsync();
                _writeSession = null;
            }
            if (_driver != null)
            {
                await _driver.DisposeAsync();
                _driver = null;
            }
        }

        // Always call base to set _finalized and stop stopwatch
        return _errors.Count > 0
            ? ExportWriteResult.Failed(_errors)
            : await base.FinalizeAsync(ct);
    }

    #endregion

    #region Legacy ExportAsync Support

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        _driver = CreateDriver();
        await _driver.VerifyConnectivityAsync();

        var session = _driver.AsyncSession(o => o.WithDatabase(_connectionConfig.Database));
        try
        {
            if (_createIndexes)
            {
                await CreateIndexesAndConstraintsAsync(session, cancellationToken);
            }

            if (_clearExisting)
            {
                await ClearExistingNodesAsync(session, cancellationToken);
            }
        }
        finally
        {
            await session.CloseAsync();
        }
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        if (_driver == null)
            throw new InvalidOperationException("Writer not initialized");

        var sanitizedRows = new List<Dictionary<string, object>>(batch.Count);

        foreach (var row in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sanitized = SanitizeRow(row);
            sanitizedRows.Add(sanitized);
        }

        var session = _driver.AsyncSession(o => o.WithDatabase(_connectionConfig.Database));
        try
        {
            var cypher = $@"
                UNWIND $rows AS row
                MERGE (r:{_nodeLabel} {{_matchlogic_id: row._matchlogic_id}})
                SET r += row";

            await session.ExecuteWriteAsync(async tx =>
            {
                await tx.RunAsync(cypher, new { rows = sanitizedRows });
            });

            _rowsWritten += batch.Count;
        }
        finally
        {
            await session.CloseAsync();
        }
    }

    protected override async Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        if (_driver != null && _exportMode.Equals("graph", StringComparison.OrdinalIgnoreCase))
        {
            var session = _driver.AsyncSession(o => o.WithDatabase(_connectionConfig.Database));
            try
            {
                await CreateRelationshipsAsync(session, cancellationToken);
                await CreateGroupSummaryNodesAsync(session, cancellationToken);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        if (_driver != null)
        {
            await _driver.DisposeAsync();
            _driver = null;
        }
    }

    #endregion

    #region Helpers

    private IDriver CreateDriver()
    {
        var uri = _connectionConfig.Uri;
        var authToken = AuthTokens.Basic(_connectionConfig.Username, _connectionConfig.Password);

        return GraphDatabase.Driver(uri, authToken, o =>
        {
            o.WithConnectionTimeout(_connectionConfig.ConnectionTimeout);

            if (_connectionConfig.Encryption)
            {
                o.WithEncryptionLevel(EncryptionLevel.Encrypted);

                if (_connectionConfig.TrustCertificate)
                {
                    o.WithTrustManager(TrustManager.CreateInsecure());
                }
            }
            else if (!_connectionConfig.Protocol.Contains("+s://"))
            {
                // Disable encryption for plain bolt:// and neo4j://
                o.WithEncryptionLevel(EncryptionLevel.None);
            }
        });
    }

    private async Task CreateIndexesAndConstraintsAsync(IAsyncSession session, CancellationToken ct)
    {
        try
        {
            // Create unique constraint on _matchlogic_id for idempotent re-export
            var constraintCypher = $"CREATE CONSTRAINT IF NOT EXISTS FOR (r:{_nodeLabel}) REQUIRE (r._matchlogic_id) IS UNIQUE";
            await session.ExecuteWriteAsync(async tx =>
            {
                await tx.RunAsync(constraintCypher);
            });
            _logger.LogInformation("Created unique constraint on {Label}._matchlogic_id", _nodeLabel);
        }
        catch (Exception ex)
        {
            // Constraint may already exist or not be supported (e.g. Neo4j Community)
            _logger.LogWarning("Could not create unique constraint (may already exist or not supported): {Error}", ex.Message);
        }

        try
        {
            // Create index on GroupId for relationship creation
            var indexCypher = $"CREATE INDEX IF NOT EXISTS FOR (r:{_nodeLabel}) ON (r.GroupId)";
            await session.ExecuteWriteAsync(async tx =>
            {
                await tx.RunAsync(indexCypher);
            });
            _logger.LogInformation("Created index on {Label}.GroupId", _nodeLabel);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not create GroupId index: {Error}", ex.Message);
        }
    }

    private async Task ClearExistingNodesAsync(IAsyncSession session, CancellationToken ct)
    {
        _logger.LogInformation("Clearing existing {Label} nodes...", _nodeLabel);

        // Try APOC first for efficient batch deletion (3-second timeout to avoid long waits)
        bool hasApoc = false;
        try
        {
            using var apocCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            apocCts.CancelAfter(TimeSpan.FromSeconds(3));

            var apocTask = session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync("RETURN apoc.version() AS version");
                return await cursor.SingleAsync();
            });

            await apocTask.WaitAsync(apocCts.Token);
            hasApoc = true;
        }
        catch
        {
            // APOC not available or timed out
        }

        if (hasApoc)
        {
            try
            {
                await session.ExecuteWriteAsync(async tx =>
                {
                    await tx.RunAsync(
                        $"CALL apoc.periodic.iterate('MATCH (n:{_nodeLabel}) RETURN n', 'DETACH DELETE n', {{batchSize: 5000}})");
                });
                _logger.LogInformation("Cleared existing nodes using APOC batch delete");
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("APOC batch delete failed, falling back to manual: {Error}", ex.Message);
            }
        }

        // Fallback: loop delete in batches
        int deleted;
        do
        {
            ct.ThrowIfCancellationRequested();
            deleted = await session.ExecuteWriteAsync(async tx =>
            {
                var cursor = await tx.RunAsync(
                    $"MATCH (n:{_nodeLabel}) WITH n LIMIT 5000 DETACH DELETE n RETURN count(*) AS deleted");
                var record = await cursor.SingleAsync();
                return record["deleted"].As<int>();
            });

            if (deleted > 0)
                _logger.LogDebug("Deleted {Count} nodes in batch", deleted);

        } while (deleted > 0);

        // Also clear MatchGroup nodes if in graph mode
        if (_exportMode.Equals("graph", StringComparison.OrdinalIgnoreCase))
        {
            do
            {
                ct.ThrowIfCancellationRequested();
                deleted = await session.ExecuteWriteAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(
                        "MATCH (n:MatchGroup) WITH n LIMIT 5000 DETACH DELETE n RETURN count(*) AS deleted");
                    var record = await cursor.SingleAsync();
                    return record["deleted"].As<int>();
                });
            } while (deleted > 0);
        }

        _logger.LogInformation("Cleared all existing nodes");
    }

    private async Task CreateRelationshipsAsync(IAsyncSession session, CancellationToken ct)
    {
        _logger.LogInformation("Creating {RelType} relationships between matched records...", _relationshipType);

        try
        {
            // Step 1: Get distinct GroupIds (as strings for type-safe comparison)
            var groupIds = new List<string>();
            await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(
                    $"MATCH (r:{_nodeLabel}) WHERE r.GroupId IS NOT NULL RETURN DISTINCT toString(r.GroupId) AS gid");
                while (await cursor.FetchAsync())
                    groupIds.Add(cursor.Current["gid"].As<string>());
            });

            _logger.LogInformation("Found {GroupCount} distinct groups for relationship creation", groupIds.Count);
            if (groupIds.Count > 0)
                _logger.LogDebug("Sample GroupIds: {Samples}", string.Join(", ", groupIds.Take(5)));

            // Step 2: Process in batches of 500 groups using COLLECT-based pair generation
            // Uses toString() for type-safe GroupId comparison (int vs string mismatch)
            int totalRelationships = 0;
            const int batchSize = 500;
            for (int i = 0; i < groupIds.Count; i += batchSize)
            {
                ct.ThrowIfCancellationRequested();
                var batch = groupIds.Skip(i).Take(batchSize).ToList();

                var batchCreated = await session.ExecuteWriteAsync(async tx =>
                {
                    var cypher = $@"
                        UNWIND $groupIds AS gid
                        MATCH (r:{_nodeLabel}) WHERE toString(r.GroupId) = gid
                        WITH gid, COLLECT(r) AS members
                        WHERE size(members) > 1
                        UNWIND range(0, size(members)-2) AS i
                        UNWIND range(i+1, size(members)-1) AS j
                        WITH members[i] AS a, members[j] AS b
                        MERGE (a)-[rel:{_relationshipType}]->(b)
                        RETURN count(rel) AS created";
                    var cursor = await tx.RunAsync(cypher, new { groupIds = batch });
                    var record = await cursor.SingleAsync();
                    return record["created"].As<int>();
                });

                totalRelationships += batchCreated;
                _logger.LogDebug("Processed relationship batch {Batch}/{Total}, created {Created} relationships",
                    Math.Min(i + batchSize, groupIds.Count), groupIds.Count, batchCreated);
            }

            _logger.LogInformation("Relationships created successfully: {Total} total", totalRelationships);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create relationships: {Error}", ex.Message);
            AddWarning($"Failed to create relationships: {ex.Message}");
        }
    }

    private async Task CreateGroupSummaryNodesAsync(IAsyncSession session, CancellationToken ct)
    {
        _logger.LogInformation("Creating MatchGroup summary nodes...");

        try
        {
            // Single-pass: COLLECT nodes per group, then UNWIND to create relationships
            // Uses toString() for type-safe GroupId comparison
            await session.ExecuteWriteAsync(async tx =>
            {
                var cypher = $@"
                    MATCH (r:{_nodeLabel}) WHERE r.GroupId IS NOT NULL
                    WITH toString(r.GroupId) AS gid, COLLECT(r) AS members,
                         count(r) AS size, avg(toFloat(r.Score)) AS avgScore
                    MERGE (g:MatchGroup {{groupId: gid}})
                    SET g.size = size, g.avgScore = avgScore
                    WITH g, members
                    UNWIND members AS r
                    MERGE (r)-[:BELONGS_TO_GROUP]->(g)";

                await tx.RunAsync(cypher);
            });

            _logger.LogInformation("MatchGroup summary nodes created successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create group summary nodes: {Error}", ex.Message);
            AddWarning($"Failed to create group summary nodes: {ex.Message}");
        }
    }

    /// <summary>
    /// Sanitize a row for Neo4j compatibility:
    /// - Replace dots/spaces in property names with underscores
    /// - Skip null values (Neo4j property limitation)
    /// - Flatten nested objects to string
    /// - Compute _matchlogic_id for MERGE key
    /// </summary>
    private Dictionary<string, object> SanitizeRow(IDictionary<string, object> row)
    {
        var sanitized = new Dictionary<string, object>();

        foreach (var kvp in row)
        {
            if (kvp.Value == null || kvp.Value == DBNull.Value)
                continue;

            // Use pre-computed mapping if available, fall back to regex for unknown columns
            var key = _sanitizedColumnNames != null && _sanitizedColumnNames.TryGetValue(kvp.Key, out var cached)
                ? cached
                : SanitizePropertyName(kvp.Key);
            var value = kvp.Value;

            // Flatten complex types to string
            if (value is IDictionary<string, object> || value is System.Collections.ICollection && value is not string && value is not byte[])
            {
                sanitized[key] = value.ToString() ?? string.Empty;
            }
            else
            {
                sanitized[key] = value;
            }
        }

        // Compute _matchlogic_id for MERGE key
        var dataSourceName = row.TryGetValue("DataSourceName", out var dsn) ? dsn?.ToString() ?? "" : "";
        var record = row.TryGetValue("Record", out var rec) ? rec?.ToString() ?? "" : "";
        sanitized["_matchlogic_id"] = $"{dataSourceName}_{record}";

        return sanitized;
    }

    private static readonly Regex InvalidPropNameChars = new Regex(@"[.\s\-]", RegexOptions.Compiled);

    /// <summary>
    /// Replace dots, spaces, and hyphens with underscores for Neo4j property names.
    /// </summary>
    private static string SanitizePropertyName(string name)
    {
        return InvalidPropNameChars.Replace(name, "_");
    }

    public override void Dispose()
    {
        if (!_disposed)
        {
            _writeSession?.CloseAsync().GetAwaiter().GetResult();
            _driver?.Dispose();
        }
        base.Dispose();
    }

    #endregion
}
