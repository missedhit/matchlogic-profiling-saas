using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Export.Helpers;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export.Writers;

/// <summary>
/// SQL Server export writer using SqlBulkCopy for optimal performance.
/// Settings read from ConnectionConfig.Parameters using DatabaseExportKeys.
/// </summary>
[HandlesExportWriter(DataSourceType.SQLServer)]
public class SqlServerExportDataWriter : BaseExportDataWriter
{
    private readonly SQLServerConnectionConfig _connectionConfig;

    // Settings from Parameters
    private readonly string _tableName;
    private readonly string _schemaName;
    private readonly bool _useBulkCopy;
    private readonly int _batchSize;
    private readonly bool _truncateExisting;
    private readonly bool _dropAndRecreate;
    private readonly int _commandTimeout;
    private readonly bool _createIndexes;
    private readonly string? _indexColumns;

    private SqlConnection? _connection;
    private SqlBulkCopy? _bulkCopy;
    private DataTable? _dataTable;
    private string? _fullTableName;

    public override string Name => "SQL Server Writer";
    public override DataSourceType Type => DataSourceType.SQLServer;

    public SqlServerExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(logger, 5000)
    {
        _connectionConfig = connectionConfig as SQLServerConnectionConfig
            ?? throw new ArgumentException("Invalid configuration type for SqlServerExportDataWriter", nameof(connectionConfig));

        var p = _connectionConfig.Parameters;

        // From Parameters
        _tableName = p.GetString(DatabaseExportKeys.TableName, "Export");
        _schemaName = p.GetString(DatabaseExportKeys.SchemaName, DatabaseExportKeys.Defaults.SchemaName);
        _useBulkCopy = p.GetBool(DatabaseExportKeys.UseBulkCopy, DatabaseExportKeys.Defaults.UseBulkCopy);
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
            _connection = new SqlConnection(_connectionConfig.ConnectionString);
            await _connection.OpenAsync(ct);

            var tableName = !string.IsNullOrEmpty(_tableName) ? _tableName : schema.TableName ?? "Export";
            _fullTableName = $"[{_schemaName}].[{tableName}]";

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

            _bulkCopy = new SqlBulkCopy(_connection)
            {
                DestinationTableName = _fullTableName,
                BatchSize = _batchSize,
                BulkCopyTimeout = _commandTimeout,
                EnableStreaming = true
            };

            _dataTable = new DataTable();
            foreach (var col in schema.Columns)
            {
                var clrType = col.GetClrType();
                _dataTable.Columns.Add(col.Name, clrType);
                _bulkCopy.ColumnMappings.Add(col.Name, col.Name);
            }

            _logger.LogInformation(
                "SQL Server writer initialized: {Table} with {Columns} typed columns",
                _fullTableName, schema.Columns.Count);
        }
        catch (Exception ex)
        {
            AddError($"Failed to initialize SQL Server writer: {ex.Message}");
            throw;
        }
    }

    public override async Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        if (_dataTable == null || _bulkCopy == null || _schema == null)
            throw new InvalidOperationException("Writer not initialized");

        try
        {
            foreach (var row in batch)
            {
                ct.ThrowIfCancellationRequested();

                var dataRow = _dataTable.NewRow();
                foreach (var col in _schema.Columns)
                {
                    var rawValue = row.TryGetValue(col.Name, out var v) ? v : null;
                    dataRow[col.Name] = ExportTypeMapper.ConvertForSqlServer(rawValue, col.DataType);
                }
                _dataTable.Rows.Add(dataRow);
                _rowsWritten++;
            }

            if (_dataTable.Rows.Count >= _batchSize)
            {
                await FlushBufferAsync(ct);
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
            if (_dataTable?.Rows.Count > 0)
            {
                await FlushBufferAsync(ct);
            }

            if (_createIndexes && !string.IsNullOrEmpty(_indexColumns))
            {
                await CreateIndexesAsync(ct);
            }

            _bulkCopy?.Close();
            _bulkCopy = null;

            if (_connection != null)
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
                _connection = null;
            }

            _logger.LogInformation(
                "SQL Server export completed: {Rows} rows to {Table}",
                _rowsWritten, _fullTableName);

            return await base.FinalizeAsync(ct);
        }
        catch (Exception ex)
        {
            AddError($"Error finalizing SQL Server export: {ex.Message}");
            return ExportWriteResult.Failed(_errors);
        }
    }

    #endregion

    #region Legacy ExportAsync Support

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        _connection = new SqlConnection(_connectionConfig.ConnectionString);
        await _connection.OpenAsync(cancellationToken);

        var tableName = !string.IsNullOrEmpty(_tableName) ? _tableName : "Export";
        _fullTableName = $"[{_schemaName}].[{tableName}]";

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

        _bulkCopy = new SqlBulkCopy(_connection)
        {
            DestinationTableName = _fullTableName,
            BatchSize = _batchSize,
            BulkCopyTimeout = _commandTimeout,
            EnableStreaming = true
        };

        _dataTable = new DataTable();
        foreach (var colName in context.OrderedColumnNames)
        {
            _dataTable.Columns.Add(colName, typeof(object));
            _bulkCopy.ColumnMappings.Add(colName, colName);
        }
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        if (_dataTable == null || _bulkCopy == null)
            throw new InvalidOperationException("Writer not initialized");

        foreach (var row in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var dataRow = _dataTable.NewRow();
            foreach (var colName in context.OrderedColumnNames)
            {
                dataRow[colName] = row.TryGetValue(colName, out var v) ? v ?? DBNull.Value : DBNull.Value;
            }
            _dataTable.Rows.Add(dataRow);
        }

        if (_dataTable.Rows.Count >= _batchSize)
        {
            await _bulkCopy.WriteToServerAsync(_dataTable, cancellationToken);
            _dataTable.Clear();
        }
    }

    protected override async Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        if (_dataTable?.Rows.Count > 0 && _bulkCopy != null)
        {
            await _bulkCopy.WriteToServerAsync(_dataTable, cancellationToken);
        }

        _bulkCopy?.Close();
        if (_connection != null)
        {
            await _connection.CloseAsync();
            await _connection.DisposeAsync();
        }
    }

    private async Task CreateTableFromContextAsync(ExportContext context, CancellationToken ct)
    {
        var columnDefs = context.OrderedColumnNames.Select(col => $"[{col}] NVARCHAR(MAX) NULL");
        var sql = $"CREATE TABLE {_fullTableName} ({string.Join(", ", columnDefs)})";
        await ExecuteNonQueryAsync(sql, ct);
    }

    #endregion

    #region Helpers

    private async Task FlushBufferAsync(CancellationToken ct)
    {
        if (_dataTable == null || _bulkCopy == null || _dataTable.Rows.Count == 0)
            return;

        await _bulkCopy.WriteToServerAsync(_dataTable, ct);
        _logger.LogDebug("Flushed {Count} rows to SQL Server", _dataTable.Rows.Count);
        _dataTable.Clear();
    }

    private async Task<bool> TableExistsAsync(string schema, string table, CancellationToken ct)
    {
        const string sql = @"
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @Table";

        using var cmd = new SqlCommand(sql, _connection);
        cmd.Parameters.AddWithValue("@Schema", schema);
        cmd.Parameters.AddWithValue("@Table", table);
        cmd.CommandTimeout = _commandTimeout;

        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result) > 0;
    }

    private async Task CreateTableWithTypesAsync(ExportSchema schema, CancellationToken ct)
    {
        var columnDefs = schema.Columns.Select(col =>
        {
            var sqlType = col.ToSqlServerType();
            var nullable = col.IsNullable ? "NULL" : "NOT NULL";
            return $"[{col.Name}] {sqlType} {nullable}";
        });

        var sql = $"CREATE TABLE {_fullTableName} ({string.Join(", ", columnDefs)})";

        await ExecuteNonQueryAsync(sql, ct);
        _logger.LogInformation("Created typed table: {Table}", _fullTableName);
    }

    private async Task CreateIndexesAsync(CancellationToken ct)
    {
        var columns = _indexColumns!.Split(',', StringSplitOptions.RemoveEmptyEntries);
        foreach (var column in columns)
        {
            var indexName = $"IX_{_tableName}_{column.Trim()}";
            var sql = $"CREATE INDEX [{indexName}] ON {_fullTableName} ([{column.Trim()}])";
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
        using var cmd = new SqlCommand(sql, _connection);
        cmd.CommandTimeout = _commandTimeout;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public override void Dispose()
    {
        if (!_disposed)
        {
            _bulkCopy?.Close();
            _connection?.Dispose();
            _dataTable?.Dispose();
        }
        base.Dispose();
    }

    #endregion
}