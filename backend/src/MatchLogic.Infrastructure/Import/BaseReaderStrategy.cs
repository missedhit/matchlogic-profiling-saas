using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import
{
    public abstract class BaseReaderStrategy : IConnectionReaderStrategy
    {
        public ConnectionConfig Config { get; }

        protected readonly ILogger _logger;
        protected string[] _headers;
        protected long? _rowCount;
        protected bool _isInitialized = false;
        public List<string> ErrorMessage { get; } = new List<string>();

        public abstract string Name { get; }

        public abstract long RowCount { get; }

        public abstract long DuplicateHeaderCount { get; }

        protected BaseReaderStrategy(ConnectionConfig config, ILogger logger)
        {
            Config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public abstract IEnumerable<string> GetHeaders();

        protected abstract Task<IEnumerable<IDictionary<string, object>>> ReadBatchAsync(CancellationToken cancellationToken);

        public abstract void Dispose();

        public virtual async Task<IAsyncEnumerable<IDictionary<string, object>>> ReadRowsAsync(int maxDegreeOfParallelism = 4, CancellationToken cancellationToken = default)
        {
            var blockingCollection = new BlockingCollection<IEnumerable<IDictionary<string, object>>>(maxDegreeOfParallelism * 2);

            var producerTask = Task.Run(async () =>
            {
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var batch = await ReadBatchAsync(cancellationToken);
                        if (batch == null || !batch.Any())
                        {
                            break;
                        }
                        blockingCollection.Add(batch, cancellationToken);
                    }
                }
                finally
                {
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

        public abstract Task<IEnumerable<IDictionary<string, object>>> ReadPreviewBatchAsync(DataImportOptions options, IColumnFilter columnFilter, CancellationToken cancellationToken);
        public abstract Task<bool> TestConnectionAsync(CancellationToken cancellationToken = default);
        public abstract Task<List<TableInfo>> GetAvailableTables();
        public abstract Task<TableSchema> GetTableSchema(string tableName);
    }
}
