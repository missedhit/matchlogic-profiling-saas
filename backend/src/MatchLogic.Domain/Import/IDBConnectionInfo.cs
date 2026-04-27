using System;

namespace MatchLogic.Domain.Import;

public interface IDBConnectionInfo
{
    string ConnectionString { get; }
    string Query { get; }
    string TableName { get; }
    string SchemaName { get; }
    TimeSpan ConnectionTimeout { get; }
}



