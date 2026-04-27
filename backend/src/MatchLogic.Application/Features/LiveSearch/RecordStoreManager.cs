using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch
{
    public class RecordStoreManager : IRecordStoreManager
    {
        private readonly IDataStore _dataStore;
        private readonly ILogger<RecordStoreManager> _logger;
        private readonly ConcurrentDictionary<Guid, IRecordStore> _stores = new();
        private readonly HashSet<Guid> _builtProjects = new();
        private readonly bool _useMemoryMapped;

        public RecordStoreManager(
            IDataStore dataStore,
            ILogger<RecordStoreManager> logger,
            bool useMemoryMapped = true)
        {
            _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _useMemoryMapped = useMemoryMapped;
        }

        public async Task BuildStoresAsync(
            Guid projectId,
            IEnumerable<Guid> dataSourceIds,
            IProgress<RecordStoreBuildProgress> progress = null,
            CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Building record stores for project {ProjectId}", projectId);

            foreach (var dsId in dataSourceIds)
            {
                var normalizedDataSourceId = GuidCollectionNameConverter.ToValidCollectionName(dsId);
                var collectionName = $"cleanse_{normalizedDataSourceId}";
                
                if(!_dataStore.CollectionExistsAsync(collectionName).Result)
                {
                    var dataSource = await _dataStore.QueryAsync<DataSource>(e => e.Id == dsId, Constants.Collections.DataSources);
                    collectionName = $"snap_{GuidCollectionNameConverter.ToValidCollectionName(dataSource.First().ActiveSnapshotId.Value)}_rows"; 
                }
                
                var store = _useMemoryMapped
                    ? (IRecordStore)new MemoryMappedRecordStore(dsId, new MemoryMappedStoreOptions(), _logger)
                    : new InMemoryRecordStore();

                _logger.LogInformation("Building store for data source {DataSourceId} from {Collection}",
                    dsId, collectionName);

                int recordCount = 0;
                await foreach (var record in _dataStore.StreamDataAsync(collectionName, cancellationToken))
                {
                    await store.AddRecordAsync(record);
                    recordCount++;

                    if (recordCount % 10000 == 0)
                    {
                        progress?.Report(new RecordStoreBuildProgress
                        {
                            DataSourceId = dsId,
                            ProcessedRecords = recordCount,
                            Message = $"Processed {recordCount} records"
                        });
                    }
                }

                await store.SwitchToReadOnlyModeAsync();
                _stores[dsId] = store;

                _logger.LogInformation(
                    "Built store for data source {DataSourceId}: {Records} records",
                    dsId, recordCount);
            }

            _builtProjects.Add(projectId);
        }

        public void RegisterRecordStore(Guid datasourceId, IRecordStore store)
        {
            _stores[datasourceId] = store;
        }

        public async Task<IDictionary<string, object>> GetRecordAsync(
            Guid dataSourceId,
            int rowNumber,
            CancellationToken cancellationToken = default)
        {
            if (!_stores.TryGetValue(dataSourceId, out var store))
                throw new InvalidOperationException($"Store not found for data source {dataSourceId}");

            return await store.GetRecordAsync(rowNumber);
        }

        public async Task<List<IDictionary<string, object>>> GetRecordsAsync(
            Guid dataSourceId,
            IEnumerable<int> rowNumbers,
            CancellationToken cancellationToken = default)
        {
            if (!_stores.TryGetValue(dataSourceId, out var store))
                throw new InvalidOperationException($"Store not found for data source {dataSourceId}");

            var results = await store.GetRecordsAsync(rowNumbers);
            return results.ToList();
        }

        public bool AreStoresBuilt(Guid projectId)
        {
            return _builtProjects.Contains(projectId);
        }

        public RecordStoreStatistics GetStatistics()
        {
            var stats = new RecordStoreStatistics
            {
                TotalDataSources = _stores.Count,
                TotalMemoryMB = (long)(GC.GetTotalMemory(false) / (1024.0 * 1024.0)),
                DataSourceStats = new List<DataSourceStoreStats>()
            };

            foreach (var kvp in _stores)
            {
                var storeStats = kvp.Value.GetStatistics();
                stats.TotalRecords += storeStats.RecordCount;

                stats.DataSourceStats.Add(new DataSourceStoreStats
                {
                    DataSourceId = kvp.Key,
                    RecordCount = storeStats.RecordCount,
                    MemoryMB = (long)(GC.GetTotalMemory(false) / (1024.0 * 1024.0)),
                    StoreType = storeStats.StorageType
                });
            }

            return stats;
        }

        public IRecordStore GetRecordStoreAsync(Guid datasourceId)
        {
            return _stores[datasourceId];
        }
    }
}
