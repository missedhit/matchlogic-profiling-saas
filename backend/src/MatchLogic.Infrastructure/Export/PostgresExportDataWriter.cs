using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Export.Helpers;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export.Writers;

/// <summary>
/// PostgreSQL export writer using COPY command for optimal performance.
/// Settings read from ConnectionConfig.Parameters using DatabaseExportKeys.
/// </summary>
[HandlesExportWriter(DataSourceType.PostgreSQL)]
public class PostgresExportDataWriter : BaseExportDataWriter
{
    private readonly PostgresConnectionConfig _connectionConfig;

    // Settings from Parameters
    private readonly string _tableName;
    private readonly string _schemaName;
    private readonly int _batchSize;
    private readonly bool _truncateExisting;
    private readonly bool _dropAndRecreate;
    private readonly int _commandTimeout;
    private readonly bool _createIndexes;
    private readonly string? _indexColumns;

    private NpgsqlConnection? _connection;
    private NpgsqlBinaryImporter? _importer;
    private string? _fullTableName;
    private List<NpgsqlDbType>? _columnTypes;

    public override string Name => "PostgreSQL Writer";
    public override DataSourceType Type => DataSourceType.PostgreSQL;

    public PostgresExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(logger, 5000)
    {
        _connectionConfig = connectionConfig as PostgresConnectionConfig
            ?? throw new ArgumentException("Invalid configuration type for PostgresExportDataWriter", nameof(connectionConfig));

        var p = _connectionConfig.Parameters;

        // From Parameters
        _tableName = p.GetString(DatabaseExportKeys.TableName, "export");
        _schemaName = p.GetString(DatabaseExportKeys.SchemaName, DatabaseExportKeys.Defaults.SchemaName);
        _batchSize = p.GetInt(DatabaseExportKeys.BatchSize, DatabaseExportKeys.Defaults.BatchSize);
        _truncateExisting = p.GetBool(DatabaseExportKeys.TruncateExisting, DatabaseExportKeys.Defaults.TruncateExisting);
        _dropAndRecreate = p.GetBool(DatabaseExportKeys.DropAndRecreate, DatabaseExportKeys.Defaults.DropAndRecreate);
        _commandTimeout = p.GetInt(DatabaseExportKeys.CommandTimeout, DatabaseExportKeys.Defaults.CommandTimeout);
        _createIndexes = p.GetBool(DatabaseExportKeys.CreateIndexes, DatabaseExportKeys.Defaults.CreateIndexes);
        _indexColumns = p.GetString(DatabaseExportKeys.IndexColumns, string.Empty);

    }

    #region IExportDataWriter Implementation

    public override async Task InitializeAsync(ExportSchema schema, CancellationToken ct = default)
    {
        await base.InitializeAsync(schema, ct);

        try
        {
            _connection = new NpgsqlConnection(_connectionConfig.ConnectionString);
            await _connection.OpenAsync(ct);

            var tableName = !string.IsNullOrEmpty(_tableName) ? _tableName : schema.TableName ?? "export";
            _fullTableName = $"\"{_schemaName}\".\"{tableName}\"";

            var tableExists = await TableExistsAsync(_schemaName, tableName, ct);

            if (tableExists)
            {
                if (_dropAndRecreate)
                {
                    _logger.LogInformation("Dropping existing table: {Table}", _fullTableName);
                    await ExecuteNonQueryAsync($"DROP TABLE {_fullTableName}", ct);
                    await CreateTableWithTypesAsync(schema, ct);
                }
                else if (_truncateExisting)
                {
                    _logger.LogInformation("Truncating existing table: {Table}", _fullTableName);
                    await ExecuteNonQueryAsync($"TRUNCATE TABLE {_fullTableName}", ct);
                }
            }
            else
            {
                await CreateTableWithTypesAsync(schema, ct);
            }

            _columnTypes = schema.Columns.Select(c => c.ToNpgsqlDbType()).ToList();

            var columns = string.Join(", ", schema.Columns.Select(c => $"\"{c.Name}\""));
            _importer = _connection.BeginBinaryImport(
                $"COPY {_fullTableName} ({columns}) FROM STDIN (FORMAT BINARY)");

            _logger.LogInformation(
                "PostgreSQL writer initialized: {Table} with {Columns} typed columns",
                _fullTableName, schema.Columns.Count);
        }
        catch (Exception ex)
        {
            AddError($"Failed to initialize PostgreSQL writer: {ex.Message}");
            throw;
        }
    }

    public override async Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        if (_importer == null || _schema == null || _columnTypes == null)
            throw new InvalidOperationException("Writer not initialized");

        try
        {
            foreach (var row in batch)
            {
                ct.ThrowIfCancellationRequested();

                await _importer.StartRowAsync(ct);

                for (int i = 0; i < _schema.Columns.Count; i++)
                {
                    var col = _schema.Columns[i];
                    var rawValue = row.TryGetValue(col.Name, out var v) ? v : null;
                    var converted = ExportTypeMapper.ConvertForPostgres(rawValue, _columnTypes[i]);

                    if (converted == null)
                    {
                        await _importer.WriteNullAsync(ct);
                    }
                    else
                    {
                        await _importer.WriteAsync(converted, _columnTypes[i], ct);
                    }
                }

                _rowsWritten++;
            }
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
            if (_importer != null)
            {
                await _importer.CompleteAsync(ct);
                await _importer.DisposeAsync();
                _importer = null;
            }

            if (_createIndexes && !string.IsNullOrEmpty(_indexColumns))
            {
                await CreateIndexesAsync(ct);
            }

            if (_connection != null)
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
                _connection = null;
            }

            _logger.LogInformation(
                "PostgreSQL export completed: {Rows} rows to {Table}",
                _rowsWritten, _fullTableName);

            return await base.FinalizeAsync(ct);
        }
        catch (Exception ex)
        {
            AddError($"Error finalizing PostgreSQL export: {ex.Message}");
            return ExportWriteResult.Failed(_errors);
        }
    }

    #endregion

    #region Legacy ExportAsync Support

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        _connection = new NpgsqlConnection(_connectionConfig.ConnectionString);
        await _connection.OpenAsync(cancellationToken);

        var tableName = !string.IsNullOrEmpty(_tableName) ? _tableName : "export";
        _fullTableName = $"\"{_schemaName}\".\"{tableName}\"";

        var tableExists = await TableExistsAsync(_schemaName, tableName, cancellationToken);

        if (tableExists && _dropAndRecreate)
        {
            await ExecuteNonQueryAsync($"DROP TABLE {_fullTableName}", cancellationToken);
            await CreateTableFromContextAsync(context, cancellationToken);
        }
        else if (tableExists && _truncateExisting)
        {
            await ExecuteNonQueryAsync($"TRUNCATE TABLE {_fullTableName}", cancellationToken);
        }
        else if (!tableExists)
        {
            await CreateTableFromContextAsync(context, cancellationToken);
        }

        var columns = string.Join(", ", context.OrderedColumnNames.Select(c => $"\"{c}\""));
        _importer = _connection.BeginBinaryImport(
            $"COPY {_fullTableName} ({columns}) FROM STDIN (FORMAT BINARY)");
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        if (_importer == null)
            throw new InvalidOperationException("Writer not initialized");

        foreach (var row in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await _importer.StartRowAsync(cancellationToken);

            foreach (var colName in context.OrderedColumnNames)
            {
                var value = row.TryGetValue(colName, out var v) ? v : null;
                if (value == null || value == DBNull.Value)
                {
                    await _importer.WriteNullAsync(cancellationToken);
                }
                else
                {
                    await _importer.WriteAsync(value.ToString() ?? string.Empty, NpgsqlDbType.Text, cancellationToken);
                }
            }
        }
    }

    protected override async Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        if (_importer != null)
        {
            await _importer.CompleteAsync(cancellationToken);
            await _importer.DisposeAsync();
        }

        if (_connection != null)
        {
            await _connection.CloseAsync();
            await _connection.DisposeAsync();
        }
    }

    private async Task CreateTableFromContextAsync(ExportContext context, CancellationToken ct)
    {
        var columnDefs = context.OrderedColumnNames.Select(col => $"\"{col}\" TEXT");
        var sql = $"CREATE TABLE {_fullTableName} ({string.Join(", ", columnDefs)})";
        await ExecuteNonQueryAsync(sql, ct);
    }

    #endregion

    #region Helpers    

    private async Task<bool> TableExistsAsync(string schema, string table, CancellationToken ct)
    {
        const string sql = @"
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = @Schema AND table_name = @Table";

        using var cmd = new NpgsqlCommand(sql, _connection);
        cmd.Parameters.AddWithValue("@Schema", schema);
        cmd.Parameters.AddWithValue("@Table", table);

        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result) > 0;
    }

    private async Task CreateTableWithTypesAsync(ExportSchema schema, CancellationToken ct)
    {
        var columnDefs = schema.Columns.Select(col =>
        {
            var sqlType = col.ToPostgresType();
            return $"\"{col.Name}\" {sqlType}";
        });

        var sql = $"CREATE TABLE {_fullTableName} ({string.Join(", ", columnDefs)})";

        await ExecuteNonQueryAsync(sql, ct);
        _logger.LogInformation("Created typed PostgreSQL table: {Table}", _fullTableName);
    }

    private async Task CreateIndexesAsync(CancellationToken ct)
    {
        var columns = _indexColumns!.Split(',', StringSplitOptions.RemoveEmptyEntries);
        foreach (var column in columns)
        {
            var indexName = $"ix_{_tableName}_{column.Trim().ToLowerInvariant()}";
            var sql = $"CREATE INDEX \"{indexName}\" ON {_fullTableName} (\"{column.Trim()}\")";
            try
            {
                await ExecuteNonQueryAsync(sql, ct);
                _logger.LogInformation("Created index: {Index}", indexName);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to create index {Index}: {Error}", indexName, ex.Message);
            }
        }
    }

    private async Task ExecuteNonQueryAsync(string sql, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(sql, _connection);
        cmd.CommandTimeout = _commandTimeout;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public override void Dispose()
    {
        if (!_disposed)
        {
            _importer?.Dispose();
            _connection?.Dispose();
        }
        base.Dispose();
    }

    #endregion
}