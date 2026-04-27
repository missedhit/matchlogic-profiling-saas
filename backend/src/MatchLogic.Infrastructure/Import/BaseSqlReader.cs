using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import;

// Database connection configuration
public class DatabaseConfig
{    
    public string ConnectionString { get; set; }
    public string Query { get; set; }
    public string TableName { get; set; }  // For table-specific operations
    public string? SchemaName { get; set; } // For schema-specific operations, if applicable
    public Dictionary<string, object> Parameters { get; set; } = new Dictionary<string, object>();
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromMinutes(30);
    public int BatchSize { get; set; } = 1000;
    public int MaxRetries { get; set; } = 3;
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public bool EnableConnectionPooling { get; set; } = true;
    public int MaxPoolSize { get; set; } = 100;
    public int MinPoolSize { get; set; } = 0;
    public bool HandleDuplicateHeaders { get; set; } = true;

    // Helper property to determine if we're using table or custom query
    public bool IsTableBasedQuery => !string.IsNullOrEmpty(TableName) && string.IsNullOrEmpty(Query);
}
// Abstract base class for all database readers
public abstract class BaseDatabaseReader : IDataBaseConnectionReaderStrategy
{
    public virtual ConnectionConfig Config { get; }

    public virtual string Name { get; }
    public virtual DatabaseConfig _config { get; set; }

    protected readonly ILogger _logger;
    protected string[] _headers;
    protected string[] _originalHeaders; // Store original headers before duplicate handling
    protected long? _rowCount;
    protected long? _duplicateHeaderCount;
    protected readonly SemaphoreSlim _semaphore;
    protected bool _disposed = false;

    protected bool IsTableBasedQuery => _config.IsTableBasedQuery;

    public List<string> ErrorMessage { get; } = new List<string>();
    public abstract DataSourceType SourceType { get; }
    public virtual long RowCount
        => _rowCount ??= CalculateRowCountAsync().GetAwaiter().GetResult(); // Lazy load row count

    public virtual long DuplicateHeaderCount
    {
        get
        {
            // First make sure we've read the headers to detect if we have header row
            GetHeaders();
            return _duplicateHeaderCount ??= _headers?.Length - _originalHeaders?.Length ?? 0; // Calculate duplicate headers
        }
    }
    protected BaseDatabaseReader(ConnectionConfig connectionConfig,DatabaseConfig databaseConfig, ILogger logger) //: base(connectionConfig, logger)
    {
        Config = connectionConfig ?? throw new ArgumentNullException(nameof(connectionConfig));
        _config = databaseConfig ?? throw new ArgumentNullException(nameof(databaseConfig));

        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _semaphore = new SemaphoreSlim(1, 1);

        // Configure connection pooling
        ConfigureConnectionPooling();
    }

    
    // Create database-specific connection string builder    
    protected virtual DbConnectionStringBuilder CreateConnectionStringBuilder(string connectionString) => new DbConnectionStringBuilder{ ConnectionString = connectionString };
    // Factory method pattern for creating database-specific connections
    protected abstract DbConnection CreateConnection();
    protected abstract DbCommand CreateCommand(DbConnection connection, string query);

    protected abstract string OpenIdentifierDelimiter { get; }
    protected abstract string CloseIdentifierDelimiter { get; }
    /// <summary>
    /// Escape identifier for database-specific syntax
    /// Wrap an identifier in whatever quoting the database needs.
    /// e.g. for SQL Server: "[MyTable]", for PostgreSQL: 'MyTable'"
    /// </summary>
    //protected abstract string EscapeIdentifier(string name);
    protected string EscapeIdentifier(string name)
    {
        if(name.Contains(OpenIdentifierDelimiter) || name.Contains(CloseIdentifierDelimiter))
        {
            // Already escaped
            return name;
        }
        return $"{OpenIdentifierDelimiter}{name}{CloseIdentifierDelimiter}";
    }
   

    // Configure connection pooling in connection string
    protected virtual void ConfigureConnectionPooling()
    {
        if (!_config.EnableConnectionPooling)
            return;

        var builder = CreateConnectionStringBuilder(_config.ConnectionString);
        ApplyConnectionPoolingSettings(builder);
        _config.ConnectionString = builder.ConnectionString;
    }

    #region Query Building Specifics to Database Type 
    
    
    protected virtual string BuildQuery() 
    {
        EnsureTableOrQuerySpecified();

        if (_config.IsTableBasedQuery)
        {
            return BuildTableQuery();
        }
        return _config.Query;
    }
    protected virtual string BuildCountQuery()
    {
        EnsureTableOrQuerySpecified();
        if (_config.IsTableBasedQuery)
        {
            return $"SELECT COUNT(*) FROM {GetTableNameWithSchema()}";
        }
        return $"SELECT COUNT(*) FROM ({_config.Query}) AS count_query";
    }
    /// <summary>
    /// Builds the database enumeration query specific to the database type.
    /// </summary>
    protected abstract string BuildDatabaseNamesQuery();
    protected abstract string BuildCustomQueryLimit(int rowCount, string customQuery);
    protected abstract string BuildTableQueryLimit(int rowCount, string TableNameWithSchema);
    protected string BuildLimitQuery(int rowCount)
    {
        if (rowCount <= 0)
        {
            throw new ArgumentException("Top count must be greater than zero.", nameof(rowCount));
        }
        EnsureTableOrQuerySpecified();
        if (_config.IsTableBasedQuery)
        {
            return BuildTableQueryLimit(rowCount, GetTableNameWithSchema());
        }
        return BuildCustomQueryLimit(rowCount, _config.Query);
    }
    #endregion
    
    private string BuildTableQuery()
    {
        //if (!string.IsNullOrEmpty(config?.Query))
        //    return config.Query;

        var query = new StringBuilder("SELECT ");

        if (Config.SourceConfig?.ColumnMappings != null && Config.SourceConfig?.ColumnMappings.Count != 0)
        {
            var selectedColumns = Config.SourceConfig?.ColumnMappings
                .Where(cm => cm.Value.Include)
                .Select(cm => EscapeIdentifier(cm.Value.SourceColumn));

            query.Append(string.Join(", ", selectedColumns));
        }
        else
        {
            query.Append('*');
        }

        query.Append($" FROM {GetTableNameWithSchema()}");
        return query.ToString();
    }

    // Enhanced parameterized command creation for SQL injection prevention
    protected virtual DbCommand CreateParameterizedCommand(
        DbConnection connection,
        string query,
        IDictionary<string, object> parameters = null)
    {
        var command = CreateCommand(connection, query);

        if (parameters != null)
        {
            foreach (var param in parameters)
            {
                var dbParameter = command.CreateParameter();
                dbParameter.ParameterName = NormalizeParameterName(param.Key);
                dbParameter.Value = param.Value ?? DBNull.Value;

                // Database-specific parameter type handling
                SetParameterType(dbParameter, param.Value);
                command.Parameters.Add(dbParameter);
            }
        }

        return command;
    }

    // Normalize parameter names for different database systems
    protected virtual string NormalizeParameterName(string parameterName)
    {
        return parameterName.StartsWith("@") ? parameterName : $"@{parameterName}";
    }    
    // Set parameter type based on value (can be overridden for database-specific behavior)
    protected virtual void SetParameterType(DbParameter parameter, object value)
    {
        if (value == null) return;

        // Basic type mapping - can be enhanced per database
        switch (value)
        {
            case string s:
                parameter.DbType = DbType.String;
                parameter.Size = s.Length > 4000 ? -1 : 4000; // Handle large strings
                break;
            case int:
                parameter.DbType = DbType.Int32;
                break;
            case long:
                parameter.DbType = DbType.Int64;
                break;
            case DateTime:
                parameter.DbType = DbType.DateTime;
                break;
            case bool:
                parameter.DbType = DbType.Boolean;
                break;
            case decimal:
                parameter.DbType = DbType.Decimal;
                break;
            case double:
                parameter.DbType = DbType.Double;
                break;
            case Guid:
                parameter.DbType = DbType.Guid;
                break;
            default:
                parameter.DbType = DbType.Object;
                break;
        }
    }

    // Apply connection pooling settings to the builder
    protected virtual void ApplyConnectionPoolingSettings(DbConnectionStringBuilder builder)
    {
        if(builder.ContainsKey("Pooling"))
        {
            builder["Pooling"] = _config.EnableConnectionPooling;
        }           
    }
    // Database-specific query modifications for compatibility
    protected virtual string AdaptQuery(string originalQuery) => originalQuery;

    // Database-specific type mapping
    protected virtual object ConvertValue(object value, Type targetType) => value;

    // Calculate row count with proper parameterization
    protected virtual async Task<long> CalculateRowCountAsync(CancellationToken cancellationToken = default)
    {
        if (_rowCount.HasValue)
            return _rowCount.Value;

        try
        {
            var countQuery = BuildCountQuery();

            using var connection = CreateConnection();
            using var command = CreateParameterizedCommand(connection, AdaptQuery(countQuery), _config.Parameters);
            command.CommandTimeout = (int)_config.CommandTimeout.TotalSeconds;

            await connection.OpenAsync(cancellationToken);
            var result = await command.ExecuteScalarAsync(cancellationToken);

            _rowCount = Convert.ToInt64(result);
            return _rowCount.Value;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error calculating row count for {DatabaseType}", Name);
            ErrorMessage.Add($"Error calculating row count: {ex.Message}");
            throw;
        }
    }
    
    // Handle duplicate headers similar to CSV/Excel readers
    protected virtual string[] HandleDuplicateHeaders(string[] originalHeaders)
    {
        if (!_config.HandleDuplicateHeaders)
            return originalHeaders;

        var processedHeaders = new List<string>();
        var headerCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var header in originalHeaders)
        {
            var cleanHeader = string.IsNullOrWhiteSpace(header) ? "Column" : header.Trim();

            if (!headerCounts.ContainsKey(cleanHeader))
            {
                headerCounts[cleanHeader] = 0;
                processedHeaders.Add(cleanHeader);
            }
            else
            {
                headerCounts[cleanHeader]++;
                processedHeaders.Add($"{cleanHeader}_{headerCounts[cleanHeader]}");
                //ColumnMapperHelper.HandleDuplicateHeaders(header, ref headerCounts);
            }
        }

        // Calculate duplicate count
        _duplicateHeaderCount = headerCounts.Values.Sum(count => count);

        return processedHeaders.ToArray();
    }

    // Test database connection
    public virtual async Task<bool> TestConnectionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            return connection.State == ConnectionState.Open;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Connection test failed for {DatabaseType}", Name);
            ErrorMessage.Add($"Connection test failed: {ex.Message}");
            return false;
        }
    }

    // Get list of tables from database
    public virtual async Task<List<string>> GetTablesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var connection = CreateConnection();
            using var command = CreateCommand(connection, BuildQuery());
            command.CommandTimeout = (int)_config.CommandTimeout.TotalSeconds;

            connection.Open();
            using var reader = command.ExecuteReader(CommandBehavior.SingleResult);

            var tables = new List<string>();
            while (await reader.ReadAsync(cancellationToken))
            {
                var tableName = reader.GetString(0);
                if (!string.IsNullOrEmpty(tableName))
                {
                    tables.Add(tableName);
                }
            }

            _logger.LogInformation("Retrieved {TableCount} tables from {DatabaseType}", tables.Count, Name);
            return tables;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving tables from {DatabaseType}", Name);
            ErrorMessage.Add($"Error retrieving tables: {ex.Message}");
            throw;
        }
    }

    // Get headers from the database query    
    public virtual async Task<IEnumerable<string>> GetHeadersAsync(CancellationToken cancellationToken = default)
    {
        if (_headers != null)
            return _headers;

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            if (_headers != null)
                return _headers;

            var query = BuildQuery();
            using var connection = CreateConnection();
            using var command = CreateParameterizedCommand(connection, AdaptQuery(query), _config.Parameters);
            command.CommandTimeout = (int)_config.CommandTimeout.TotalSeconds;

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly, cancellationToken);

            var schemaTable = reader.GetSchemaTable();
            _originalHeaders = schemaTable.Rows.Cast<DataRow>()
                .Select(row => row["ColumnName"].ToString())
                .Where(name => !string.IsNullOrEmpty(name))
                .ToArray();

            _headers = HandleDuplicateHeaders(_originalHeaders);

            _logger.LogInformation("Retrieved {HeaderCount} headers from {DatabaseType} query (duplicates: {DuplicateCount})",
                _headers.Length, Name, _duplicateHeaderCount ?? 0);
            return _headers;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving headers from {DatabaseType} query", Name);
            ErrorMessage.Add($"Error retrieving headers: {ex.Message}");
            throw;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    protected virtual async Task<IEnumerable<IDictionary<string, object>>> ReadBatchAsync(
        DbDataReader reader,
        string[] headers,
        CancellationToken cancellationToken)
    {
        var batch = new List<IDictionary<string, object>>(_config.BatchSize);

        try
        {
            while (await reader.ReadAsync(cancellationToken) && batch.Count < _config.BatchSize)
            {
                var row = new Dictionary<string, object>(headers.Length);
                for (int i = 0; i < headers.Length; i++)
                {
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    row[headers[i]] = ConvertValue(value, reader.GetFieldType(i));
                }
                batch.Add(row);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error reading batch from {DatabaseType}", Name);
            ErrorMessage.Add($"Error reading batch: {ex.Message}");
            throw;
        }

        return batch;
    }

    public virtual async Task<IAsyncEnumerable<IDictionary<string, object>>> ReadRowsAsync(
        int maxDegreeOfParallelism = 4,
        CancellationToken cancellationToken = default)
    {
        var headers = (await GetHeadersAsync(cancellationToken)).ToArray();
        var blockingCollection = new BlockingCollection<IEnumerable<IDictionary<string, object>>>(maxDegreeOfParallelism * 2);

        var producerTask = Task.Run(async () =>
        {
            DbConnection connection = null;
            DbCommand command = null;
            DbDataReader reader = null;

            try
            {
                connection = CreateConnection();
                var query = BuildQuery();
                command = CreateParameterizedCommand(connection, AdaptQuery(query), _config.Parameters);
                command.CommandTimeout = (int)_config.CommandTimeout.TotalSeconds;

                await connection.OpenAsync(cancellationToken);
                reader = await command.ExecuteReaderAsync(cancellationToken);

                while (!cancellationToken.IsCancellationRequested)
                {
                    var batch = await ReadBatchAsync(reader, headers, cancellationToken);
                    if (batch?.Any() == true)
                    {
                        blockingCollection.Add(batch, cancellationToken);
                    }
                    else
                    {
                        break; // No more records
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error in producer task for {DatabaseType}", Name);
                ErrorMessage.Add($"Error in producer task: {ex.Message}");
            }
            finally
            {
                reader?.Dispose();
                command?.Dispose();
                connection?.Dispose();
                blockingCollection.CompleteAdding();
            }
        }, cancellationToken);

        async IAsyncEnumerable<IDictionary<string, object>> ConsumeRows()
        {
            try
            {
                foreach (var batch in blockingCollection.GetConsumingEnumerable(cancellationToken))
                {
                    foreach (var row in batch)
                    {
                        yield return row;
                    }
                }
            }
            finally
            {
                await producerTask;
            }
        }

        return ConsumeRows();
    }

    public virtual async Task<IEnumerable<IDictionary<string, object>>> ReadPreviewBatchAsync(
        DataImportOptions options,
        IColumnFilter columnFilter,
        CancellationToken cancellationToken)
    {
        var batch = new List<IDictionary<string, object>>(options.PreviewLimit);
        try
        {
            var headers = (await GetHeadersAsync(cancellationToken)).ToArray();

            var previewQuery = BuildLimitQuery(options.PreviewLimit);

            using var connection = CreateConnection();
            using var command = CreateParameterizedCommand(connection, AdaptQuery(previewQuery), _config.Parameters);
            command.CommandTimeout = (int)_config.CommandTimeout.TotalSeconds;

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            
            while (await reader.ReadAsync(cancellationToken) && batch.Count < options?.PreviewLimit)
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < headers.Length; i++)
                {
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    row[headers[i]] = ConvertValue(value, reader.GetFieldType(i));
                }
                if (options != null && columnFilter != null)
                {
                    var filteredRow = columnFilter.FilterColumns(row, options.ColumnMappings);
                    batch.Add(filteredRow);
                }
                else
                {
                    batch.Add(row);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving preview batch from {DatabaseType} query", Name);
            ErrorMessage.Add($"Error retrieving preview : {ex.Message}");
            throw;
        }

        return batch;
    }

    protected string GetTableNameWithSchema()
    {
        EnsureTableOrQuerySpecified();
        
        // If schema is not provided, use only table name
        if (string.IsNullOrWhiteSpace(_config.SchemaName))
        {
            SplitSchemaTableName(_config.TableName);
            return EscapeIdentifier(_config.TableName);
        }
        // If schema is provided, format as "Schema.Table"
        return $"{EscapeIdentifier(_config.SchemaName)}.{EscapeIdentifier(_config.TableName)}";
    }
    
    public List<string> GetTables() => GetTablesAsync().GetAwaiter().GetResult();
    public IEnumerable<string> GetHeaders() => GetHeadersAsync().GetAwaiter().GetResult();

    
    public virtual async Task<List<TableInfo>> GetAvailableTables()
    {
        await _semaphore.WaitAsync();
        try
        {
            
            using var connection = CreateConnection();
            await connection.OpenAsync();

            var tables = new List<TableInfo>();

            var schemaTable = await connection.GetSchemaAsync("Tables");
            foreach (DataRow row in schemaTable.Rows)
            {
                tables.Add(new TableInfo
                {
                    Schema = Convert.ToString(row["TABLE_SCHEMA"]),
                    Name = Convert.ToString(row["TABLE_NAME"]),
                    Type = "TABLE"
                });
            }

            _logger.LogInformation("Retrieved {TableCount} tables from {DatabaseType} query", tables.Count, Name);
            return tables;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving AvailableTables from {DatabaseType} query", Name);
            ErrorMessage.Add($"Error retrieving AvailableTables: {ex.Message}");
            throw;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task<TableSchema> GetTableSchema(string tableName)
    {
        await _semaphore.WaitAsync();
        try
        {
            
            // check if config has table name, if not use the provided tableName parameter
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                SplitSchemaTableName(tableName);
            }

            if (string.IsNullOrEmpty(_config.TableName))
                throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
            // Fetch only one row to get schema
            var query = BuildLimitQuery(1); 
            using var connection = CreateConnection();
            using var command = CreateParameterizedCommand(connection, AdaptQuery(query), _config.Parameters);
            command.CommandTimeout = (int)_config.CommandTimeout.TotalSeconds;

            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly);

            var columns = new List<ColumnInfo>();
            var schemaTable = reader.GetSchemaTable();


            foreach (DataRow row in schemaTable.Rows)
            {
                int? Length = Convert.ToInt32(row["ColumnSize"]);

                columns.Add(new ColumnInfo
                {
                    Ordinal = Convert.ToInt32(row["ColumnOrdinal"]),
                    Name = Convert.ToString(row["ColumnName"]),
                    DataType = Convert.ToString(row["DataType"]),
                    Length = Length.HasValue && Length > 0 ? Length : null,//Postgres might length might not be defined
                    IsNullable = row["AllowDBNull"] != DBNull.Value && (bool)row["AllowDBNull"],
                });
            }

            _logger.LogInformation("Retrieved {TableCount} tables from {DatabaseType} query", columns.Count, Name);
            return new TableSchema() { Columns = columns };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving TableSchema from {DatabaseType} query", Name);
            ErrorMessage.Add($"Error retrieving TableSchema: {ex.Message}");
            //throw;
            return new TableSchema() { Columns = new List<ColumnInfo>() };
        }
        finally
        {
            _semaphore.Release();
        }
    }


    
    /// <summary>
    /// Enumerates databases on the server. Must be implemented by each database-specific reader.
    /// </summary>
    public virtual async Task<List<string>> GetAvailableDatabasesAsync(CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            _logger.LogInformation("Retrieving available databases from {DatabaseType}", Name);
            var databases = new List<string>();            
            using var connection = CreateConnection();
            using var command = CreateCommand(connection, BuildDatabaseNamesQuery());
            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                databases.Add(reader.GetString(0));
            }
            return databases;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving databases from {DatabaseType}", Name);
            ErrorMessage.Add($"Error retrieving databases: {ex.Message}");
            throw;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    #region Private Helpers
    private void EnsureTableOrQuerySpecified()
    {
        if (string.IsNullOrEmpty(_config.TableName) && string.IsNullOrEmpty(_config.Query))
            throw new ArgumentException("Either TableName or Query parameter must be provided.");
    }
    private void SplitSchemaTableName(string tableName)
    {
        if (!string.IsNullOrWhiteSpace(tableName))
        {
            // If tableName contains schema (e.g. "dbo.MyTable"), split it
            var schema = tableName.Contains('.')
            ? tableName.Split('.')[0]
            : string.Empty;
            var table = tableName.Contains('.')
                ? tableName.Split('.')[1]
                : tableName;
            // Update config with table and schema in config to be used in query building
            _config.TableName = table;
            _config.SchemaName = schema;
        }
    }
    #endregion

    public virtual void Dispose()
    {
        if (!_disposed)
        {
            _semaphore?.Dispose();
            _disposed = true;
            _logger.LogInformation("Disposed {DatabaseType} reader", Name);
        }
    }

}