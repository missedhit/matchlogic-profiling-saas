using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Import;
public interface IConnectionReaderStrategy : IDisposable
{
    ConnectionConfig Config { get; }
    IEnumerable<string> GetHeaders();
    Task<IAsyncEnumerable<IDictionary<string, object>>> ReadRowsAsync(int maxDegreeOfParallelism = 4, CancellationToken cancellationToken = default);
    Task<IEnumerable<IDictionary<string, object>>> ReadPreviewBatchAsync(DataImportOptions options, IColumnFilter columnFilter, CancellationToken cancellationToken);

    string Name { get; }

    long RowCount { get; }

    long DuplicateHeaderCount { get; }
    List<string> ErrorMessage { get; }

    Task<bool> TestConnectionAsync(CancellationToken cancellationToken = default);
    Task<List<TableInfo>> GetAvailableTables();
    Task<TableSchema> GetTableSchema(string tableName);
}
public interface IFileConnectionReaderStrategy : IConnectionReaderStrategy
{
    // Inherits all members from IConnectionReaderStrategy
}
public interface IDataBaseConnectionReaderStrategy : IConnectionReaderStrategy
{
    Task<List<string>> GetAvailableDatabasesAsync(CancellationToken cancellationToken = default);
}
