using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Data.Common;
using Npgsql;
using MatchLogic.Application.Features.Import;

namespace MatchLogic.Infrastructure.Import;

[HandlesConnectionConfig(typeof(PostgresConnectionConfig))]
public class PostgresSQLReader : BaseDatabaseReader
{
    public override DataSourceType SourceType => DataSourceType.PostgreSQL;
    public override string Name => "PostgreSQL Reader";

    protected override string OpenIdentifierDelimiter => "\"";

    protected override string CloseIdentifierDelimiter => "\"";

    public PostgresSQLReader(ConnectionConfig connectionConfig, ILogger logger) : base(
        connectionConfig,
        connectionConfig is PostgresConnectionConfig pgConfig
            ? new DatabaseConfig
            {
                ConnectionString = pgConfig.ConnectionString,
                Query = pgConfig.Query,
                TableName = pgConfig.TableName,
                SchemaName = pgConfig.SchemaName,
                CommandTimeout = pgConfig.ConnectionTimeout
            }
            : throw new ArgumentException($"Expected {nameof(PostgresConnectionConfig)} but received {connectionConfig.GetType().Name}", nameof(connectionConfig)),
        logger)
    {
        // No additional initialization needed here as everything is handled in the base call
    }

    protected override DbConnection CreateConnection()
    {
        return new NpgsqlConnection(_config.ConnectionString);
    }

    protected override DbCommand CreateCommand(DbConnection connection, string query)
    {
        return new NpgsqlCommand(query, (NpgsqlConnection)connection);
    }


    protected override string BuildDatabaseNamesQuery()
    {
        return "SELECT datname FROM pg_database WHERE datistemplate = false";
    }

    protected override string BuildCustomQueryLimit(int rowCount, string customQuery)
    {
        return $"SELECT * FROM ({customQuery}) AS preview_query LIMIT {rowCount}";
    }

    protected override string BuildTableQueryLimit(int rowCount, string TableNameWithSchema)
    {
        return $"SELECT * FROM {TableNameWithSchema} LIMIT {rowCount}";
    }
}