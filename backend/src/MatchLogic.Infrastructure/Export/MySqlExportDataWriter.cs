using MatchLogic.Application.Features.Export;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Export.Helpers;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export.Writers;

/// <summary>
/// MySQL export writer using MySqlBulkCopy for optimal performance.
/// Settings read from ConnectionConfig.Parameters using DatabaseExportKeys.
/// </summary>
[HandlesExportWriter(DataSourceType.MySQL)]
public class MySqlExportDataWriter : BaseExportDataWriter
{
    private readonly MySQLConnectionConfig _connectionConfig;

    // Settings from Parameters
    private readonly string _tableName;
    private readonly int _batchSize;
    private readonly bool _truncateExisting;
    private readonly bool _dropAndRecreate;
    private readonly int _commandTimeout;
    private readonly bool _createIndexes;
    private readonly string? _indexColumns;

    private MySqlConnection? _connection;
    private MySqlBulkCopy? _bulkCopy;
    private DataTable? _dataTable;
    private string? _escapedTableName;
    private bool _bulkCopyFailed = false;

    public override string Name => "MySQL Writer";
    public override DataSourceType Type => DataSourceType.MySQL;

    public MySqlExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(logger, 5000)
    {
        _connectionConfig = connectionConfig as MySQLConnectionConfig
            ?? throw new ArgumentException("Invalid configuration type for MySqlExportDataWriter", nameof(connectionConfig));

        var p = _connectionConfig.Parameters;

        // From Parameters
        _tableName = p.GetString(DatabaseExportKeys.TableName, "Export");
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
            // Build connection string with AllowLoadLocalInfile enabled
            var connStringBuilder = new MySqlConnectionStringBuilder(_connectionConfig.ConnectionString)
            {
                AllowLoadLocalInfile = true,
                AllowUserVariables = true
            };

            _connection = new MySqlConnection(connStringBuilder.ConnectionString);
            await _connection.OpenAsync(ct);

            var tableName = !string.IsNullOrEmpty(_tableName) ? _tableName : schema.TableName ?? "Export";
            _escapedTableName = $"`{tableName}`";

            var tableExists = await TableExistsAsync(tableName, ct);

            if (tableExists)
            {
                if (_dropAndRecreate)
                {
                    _logger.LogInformation("Dropping existing table: {Table}", tableName);
                    await ExecuteNonQueryAsync($"DROP TABLE {_escapedTableName}", ct);
                    await CreateTableWithTypesAsync(schema, ct);
                }
                else if (_truncateExisting)
                {
                    _logger.LogInformation("Truncating existing table: {Table}", tableName);
                    await ExecuteNonQueryAsync($"TRUNCATE TABLE {_escapedTableName}", ct);
                }
            }
            else
            {
                await CreateTableWithTypesAsync(schema, ct);
            }

            // Try to initialize bulk copy
            try
            {
                _bulkCopy = new MySqlBulkCopy(_connection)
                {
                    DestinationTableName = _escapedTableName,
                    BulkCopyTimeout = _commandTimeout
                };

                // Set up column mappings - source index to destination column name
                for (int i = 0; i < schema.Columns.Count; i++)
                {
                    _bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(i, schema.Columns[i].Name));
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to initialize bulk copy, will use batch inserts: {Error}", ex.Message);
                _bulkCopyFailed = true;
                _bulkCopy = null;
            }

            _dataTable = new DataTable();
            foreach (var col in schema.Columns)
            {
                var clrType = col.GetClrType();
                _dataTable.Columns.Add(col.Name, clrType);
            }

            _logger.LogInformation(
                "MySQL writer initialized: {Table} with {Columns} columns (BulkCopy: {UseBulk})",
                tableName, schema.Columns.Count, _bulkCopy != null);
        }
        catch (Exception ex)
        {
            AddError($"Failed to initialize MySQL writer: {ex.Message}");
            throw;
        }
    }

    public override async Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        if (_dataTable == null || _schema == null)
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
                    dataRow[col.Name] = ExportTypeMapper.ConvertForMySql(rawValue, col.DataType);
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

            // Clean up bulk copy - just set to null
            _bulkCopy = null;

            if (_connection != null)
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
                _connection = null;
            }

            _logger.LogInformation(
                "MySQL export completed: {Rows} rows to {Table}",
                _rowsWritten, _tableName);

            return await base.FinalizeAsync(ct);
        }
        catch (Exception ex)
        {
            AddError($"Error finalizing MySQL export: {ex.Message}");
            return ExportWriteResult.Failed(_errors);
        }
    }

    #endregion

    #region Legacy ExportAsync Support

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        var connStringBuilder = new MySqlConnectionStringBuilder(_connectionConfig.ConnectionString)
        {
            AllowLoadLocalInfile = true,
            AllowUserVariables = true
        };

        _connection = new MySqlConnection(connStringBuilder.ConnectionString);
        await _connection.OpenAsync(cancellationToken);

        var tableName = !string.IsNullOrEmpty(_tableName) ? _tableName : "Export";
        _escapedTableName = $"`{tableName}`";

        var tableExists = await TableExistsAsync(tableName, cancellationToken);

        if (tableExists && _dropAndRecreate)
        {
            await ExecuteNonQueryAsync($"DROP TABLE {_escapedTableName}", cancellationToken);
            await CreateTableFromContextAsync(context, cancellationToken);
        }
        else if (tableExists && _truncateExisting)
        {
            await ExecuteNonQueryAsync($"TRUNCATE TABLE {_escapedTableName}", cancellationToken);
        }
        else if (!tableExists)
        {
            await CreateTableFromContextAsync(context, cancellationToken);
        }

        try
        {
            _bulkCopy = new MySqlBulkCopy(_connection)
            {
                DestinationTableName = _escapedTableName,
                BulkCopyTimeout = _commandTimeout
            };

            for (int i = 0; i < context.OrderedColumnNames.Count; i++)
            {
                _bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(i, context.OrderedColumnNames[i]));
            }
        }
        catch
        {
            _bulkCopyFailed = true;
            _bulkCopy = null;
        }

        _dataTable = new DataTable();
        foreach (var colName in context.OrderedColumnNames)
        {
            _dataTable.Columns.Add(colName, typeof(object));
        }
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        if (_dataTable == null)
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
            await FlushBufferAsync(cancellationToken);
        }
    }

    protected override async Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        if (_dataTable?.Rows.Count > 0)
        {
            await FlushBufferAsync(cancellationToken);
        }

        _bulkCopy = null;
        if (_connection != null)
        {
            await _connection.CloseAsync();
            await _connection.DisposeAsync();
        }
    }

    private async Task CreateTableFromContextAsync(ExportContext context, CancellationToken ct)
    {
        var columnDefs = context.OrderedColumnNames.Select(col => $"`{col}` TEXT");
        var sql = $"CREATE TABLE {_escapedTableName} ({string.Join(", ", columnDefs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        await ExecuteNonQueryAsync(sql, ct);
    }

    #endregion

    #region Helpers

    private async Task FlushBufferAsync(CancellationToken ct)
    {
        if (_dataTable == null || _dataTable.Rows.Count == 0 || _connection == null)
            return;

        // Try bulk copy first if available and not previously failed
        if (_bulkCopy != null && !_bulkCopyFailed)
        {
            try
            {
                var result = await _bulkCopy.WriteToServerAsync(_dataTable, ct);
                _logger.LogDebug("Bulk copied {Count} rows to MySQL", result.RowsInserted);
                _dataTable.Clear();
                return;
            }
            catch (MySqlException ex) when (ex.Message.Contains("Loading local data is disabled"))
            {
                _logger.LogWarning(
                    "Bulk copy failed (local_infile disabled). Falling back to batch inserts. " +
                    "To enable bulk copy, run: SET GLOBAL local_infile = 1;");

                _bulkCopyFailed = true;
                _bulkCopy = null;
                // Fall through to batch insert
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Bulk copy failed: {Error}. Falling back to batch inserts.", ex.Message);
                _bulkCopyFailed = true;
                _bulkCopy = null;
                // Fall through to batch insert
            }
        }

        // Fallback to batch INSERT
        await FlushWithBatchInsertAsync(ct);
    }

    private async Task FlushWithBatchInsertAsync(CancellationToken ct)
    {
        if (_dataTable == null || _dataTable.Rows.Count == 0 || _connection == null || _schema == null)
            return;

        var columnNames = string.Join(", ", _dataTable.Columns.Cast<DataColumn>().Select(c => $"`{c.ColumnName}`"));
        var maxRowsPerBatch = 1000; // MySQL max_allowed_packet limit

        for (int i = 0; i < _dataTable.Rows.Count; i += maxRowsPerBatch)
        {
            ct.ThrowIfCancellationRequested();

            var batchRows = _dataTable.Rows.Cast<DataRow>()
                .Skip(i)
                .Take(maxRowsPerBatch)
                .ToList();

            var valuesSql = new StringBuilder();
            var parameters = new List<MySqlParameter>();

            for (int rowIdx = 0; rowIdx < batchRows.Count; rowIdx++)
            {
                if (rowIdx > 0) valuesSql.Append(", ");

                valuesSql.Append('(');
                for (int colIdx = 0; colIdx < _dataTable.Columns.Count; colIdx++)
                {
                    if (colIdx > 0) valuesSql.Append(", ");

                    var paramName = $"@p{rowIdx}_{colIdx}";
                    valuesSql.Append(paramName);

                    var value = batchRows[rowIdx][colIdx];
                    parameters.Add(new MySqlParameter(paramName, value == DBNull.Value ? null : value));
                }
                valuesSql.Append(')');
            }

            var sql = $"INSERT INTO {_escapedTableName} ({columnNames}) VALUES {valuesSql}";

            using var cmd = new MySqlCommand(sql, _connection);
            cmd.CommandTimeout = _commandTimeout;
            cmd.Parameters.AddRange(parameters.ToArray());

            await cmd.ExecuteNonQueryAsync(ct);
        }

        _logger.LogDebug("Batch inserted {Count} rows to MySQL", _dataTable.Rows.Count);
        _dataTable.Clear();
    }

    private async Task<bool> TableExistsAsync(string table, CancellationToken ct)
    {
        const string sql = @"
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @Table";

        using var cmd = new MySqlCommand(sql, _connection);
        cmd.Parameters.AddWithValue("@Table", table);
        cmd.CommandTimeout = _commandTimeout;

        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result) > 0;
    }

    private async Task CreateTableWithTypesAsync(ExportSchema schema, CancellationToken ct)
    {
        var columnDefs = schema.Columns.Select(col =>
        {
            var sqlType = col.ToMySqlType();
            var nullable = col.IsNullable ? "NULL" : "NOT NULL";
            return $"`{col.Name}` {sqlType} {nullable}";
        });

        var sql = $"CREATE TABLE {_escapedTableName} ({string.Join(", ", columnDefs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        await ExecuteNonQueryAsync(sql, ct);
        _logger.LogInformation("Created typed MySQL table: {Table}", _tableName);
    }

    private async Task CreateIndexesAsync(CancellationToken ct)
    {
        var columns = _indexColumns!.Split(',', StringSplitOptions.RemoveEmptyEntries);
        foreach (var column in columns)
        {
            var indexName = $"IX_{_tableName}_{column.Trim()}";
            var sql = $"CREATE INDEX `{indexName}` ON {_escapedTableName} (`{column.Trim()}`)";
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
        using var cmd = new MySqlCommand(sql, _connection);
        cmd.CommandTimeout = _commandTimeout;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public override void Dispose()
    {
        if (!_disposed)
        {
            // MySqlBulkCopy doesn't implement IDisposable in all versions
            // Just set to null and let GC handle it
            _bulkCopy = null;
            _connection?.Dispose();
            _dataTable?.Dispose();
        }
        base.Dispose();
    }

    #endregion
}