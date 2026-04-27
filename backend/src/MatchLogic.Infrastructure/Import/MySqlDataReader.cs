using MatchLogic.Application.Features.Import;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import;

[HandlesConnectionConfig(typeof(MySQLConnectionConfig))]
public class MySqlDataReader : BaseDatabaseReader
{
    public override DataSourceType SourceType => DataSourceType.MySQL;
    public override string Name => "MySQL Reader";

    protected override string OpenIdentifierDelimiter => "`";

    protected override string CloseIdentifierDelimiter => "`";

    public MySqlDataReader(ConnectionConfig connectionConfig, ILogger logger) : base(
        connectionConfig,
        connectionConfig is MySQLConnectionConfig mysqlConfig
            ? new DatabaseConfig
            {
                ConnectionString = mysqlConfig.ConnectionString,
                Query = mysqlConfig.Query,
                TableName = mysqlConfig.TableName,
                SchemaName = mysqlConfig.SchemaName,
                CommandTimeout = mysqlConfig.ConnectionTimeout
            }
            : throw new ArgumentException($"Expected {nameof(MySQLConnectionConfig)} but received {connectionConfig.GetType().Name}", nameof(connectionConfig)),
        logger)
    {
        // No additional initialization needed here as everything is handled in the base call
    }

    protected override DbConnection CreateConnection()
    {
        return new MySqlConnection(_config.ConnectionString);
    }

    protected override DbCommand CreateCommand(DbConnection connection, string query)
    {
        return new MySqlCommand(query, (MySqlConnection)connection);
    }

    protected override string BuildDatabaseNamesQuery()
    {
        return "SHOW DATABASES";
    }

    protected override string BuildCustomQueryLimit(int rowCount, string customQuery)
    {
        return $"SELECT * FROM ({customQuery}) AS preview_query LIMIT {rowCount}";
    }

    protected override string BuildTableQueryLimit(int rowCount, string TableNameWithSchema)
    {
        return $"SELECT * FROM {TableNameWithSchema} LIMIT {rowCount}";
    }

    // Override GetAvailableTables to use MySQL-specific implementation
    public override async Task<List<TableInfo>> GetAvailableTables()
    {
        await _semaphore.WaitAsync();
        try
        {
            using var connection = CreateConnection();
            await connection.OpenAsync();

            var tables = new List<TableInfo>();

            // Use MySQL-specific query to get tables from the current database only
            var query = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()";

            using var command = CreateCommand(connection, query);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                tables.Add(new TableInfo
                {
                    Schema = Convert.ToString(reader["TABLE_SCHEMA"]),
                    Name = Convert.ToString(reader["TABLE_NAME"]),
                    Type = "TABLE"//Convert.ToString(reader["TABLE_TYPE"])
                });
            }

            _logger.LogInformation("Retrieved {TableCount} tables from {DatabaseType} current database", tables.Count, Name);
            return tables;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving AvailableTables from {DatabaseType} current database", Name);
            ErrorMessage.Add($"Error retrieving AvailableTables: {ex.Message}");
            throw;
        }
        finally
        {
            _semaphore.Release();
        }
    }
}