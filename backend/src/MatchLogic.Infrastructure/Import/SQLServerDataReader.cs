using MatchLogic.Application.Features.Import;
using MatchLogic.Domain.Import;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Data.Common;

namespace MatchLogic.Infrastructure.Import;
[HandlesConnectionConfig(typeof(SQLServerConnectionConfig))]
public class SQLServerReader : BaseDatabaseReader
{
    public override DataSourceType SourceType => DataSourceType.SQLServer;
    public override string Name => "SQL Server Reader";

    protected override string OpenIdentifierDelimiter => "[";// SQL Server uses square brackets for escaping names

    protected override string CloseIdentifierDelimiter => "]";// SQL Server uses square brackets for escaping names

    public SQLServerReader(ConnectionConfig connectionConfig, ILogger logger) : base(
    connectionConfig,
    connectionConfig is SQLServerConnectionConfig sqlConfig
        ? new DatabaseConfig
        {
            ConnectionString = sqlConfig.ConnectionString,
            Query = sqlConfig.Query,
            TableName = sqlConfig.TableName,
            SchemaName = sqlConfig.SchemaName,
            CommandTimeout = sqlConfig.ConnectionTimeout
        }
        : throw new ArgumentException($"Expected {nameof(SQLServerConnectionConfig)} but received {connectionConfig.GetType().Name}", nameof(connectionConfig)),
    logger)
    {
        // No additional initialization needed here as everything is handled in the base call
    }
    
    protected override DbConnection CreateConnection()
    {
        return new SqlConnection(_config.ConnectionString);
    }
    protected override DbCommand CreateCommand(DbConnection connection, string query)
    {
        return new SqlCommand(query, (SqlConnection)connection);
    }


    protected override string BuildDatabaseNamesQuery()
    {
        return "SELECT name FROM sys.databases";
    }

    protected override string BuildCustomQueryLimit(int rowCount, string customQuery)
    {
        return $"SELECT TOP {rowCount} * FROM ({customQuery}) AS preview_query";
    }

    protected override string BuildTableQueryLimit(int rowCount, string TableNameWithSchema)
    {
        return $"SELECT TOP {rowCount} * FROM {TableNameWithSchema}";
    }

   
}
