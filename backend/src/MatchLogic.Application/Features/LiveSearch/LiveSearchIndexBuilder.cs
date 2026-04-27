using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch
{
    public class LiveSearchIndexBuilder
    {
        private readonly ProductionQGramIndexerDME _indexer;
        private readonly IDataStore _dataStore;
        private readonly LiveSearchConfiguration _config;
        private readonly ILogger<LiveSearchIndexBuilder> _logger;

        public LiveSearchIndexBuilder(
            ProductionQGramIndexerDME indexer,
            IDataStore dataStore,
            IOptions<LiveSearchConfiguration> config,
            ILogger<LiveSearchIndexBuilder> logger)
        {
            _indexer = indexer ?? throw new ArgumentNullException(nameof(indexer));
            _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            _config = config?.Value ?? throw new ArgumentNullException(nameof(config));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Build Q-gram index by delegating to ProductionQGramIndexerDME
        /// </summary>
        public async Task<QGramIndexData> BuildIndexAsync(
            List<DataSource> dataSources,
            MatchDefinitionCollection matchDefinitionCollection,
            CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Starting index build");
            _logger.LogInformation("Starting index build for {Count} data sources", dataSources.Count);

            _indexer.InitializeBlockingConfiguration(matchDefinitionCollection);

            // Index each data source using existing IndexDataSourceAsync
            foreach (var dataSource in dataSources)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var collectionName = $"cleanse_{GuidCollectionNameConverter.ToValidCollectionName(dataSource.Id)}";
                if(!_dataStore.CollectionExistsAsync(collectionName).Result)
                {
                    collectionName = $"snap_{GuidCollectionNameConverter.ToValidCollectionName(dataSource.ActiveSnapshotId.Value)}_rows";
                }
                var records = _dataStore.StreamDataAsync(collectionName, cancellationToken);

                // Get fields to index from match definitions
                var fieldsToIndex = GetFieldsToIndex(matchDefinitionCollection.Definitions, dataSource.Id);

                var estimatedRecordCount = dataSource.RecordCount > 0
                    ? dataSource.RecordCount
                    : 100000; // Fallback default

                var config = new DataSourceIndexingConfig
                {
                    DataSourceId = dataSource.Id,
                    DataSourceName = dataSource.Name,
                    FieldsToIndex = fieldsToIndex,
                    UseInMemoryStore = true,
                    InMemoryThreshold = (int)estimatedRecordCount
                };

                var progressTracker = new SimpleProgressTracker(_logger);

                // Delegate to existing ProductionQGramIndexerDME.IndexDataSourceAsync
                var result = await _indexer.IndexDataSourceAsync(
                    records,
                    config,
                    progressTracker,
                    cancellationToken);

                _logger.LogInformation(
                    "Indexed data source {Name}: {Records} records in {Duration:F2}s",
                    dataSource.Name,
                    result.ProcessedRecords,
                    result.IndexingDuration.TotalSeconds);
            }

            // Seal the global index (required before candidate generation)
            _indexer.SealGlobalIndex();

            // Extract index data for persistence using NEW method
            var indexData = _indexer.BuildIndexDataForPersistence(_config.ProjectId);

            _logger.LogInformation(
                "Index build completed: {Records} total records, {Fields} fields",
                indexData.TotalRecords,
                indexData.GlobalFieldIndex.Count);

            return indexData;
        }

        /// <summary>
        /// Get record store for a data source (created during indexing)
        /// </summary>
        public IRecordStore GetRecordStore(Guid dataSourceId)
        {
            // Delegate to indexer
            return _indexer.GetRecordStore(dataSourceId);
        }

        /// <summary>
        /// Get fields to index for a specific data source from match definitions
        /// </summary>
        private List<string> GetFieldsToIndex(List<MatchLogic.Domain.Entities.MatchDefinition> matchDefinitions, Guid dataSourceId)
        {
            var fields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var matchDef in matchDefinitions)
            {
                foreach (var criteria in matchDef.Criteria)
                {
                    foreach (var fieldMapping in criteria.FieldMappings)
                    {
                        if (fieldMapping.DataSourceId == dataSourceId)
                        {
                            fields.Add(fieldMapping.FieldName);
                        }
                    }
                }
            }

            return fields.ToList();
        }
    }

    /// <summary>
    /// Simple progress tracker for indexing
    /// </summary>
    internal class SimpleProgressTracker : IStepProgressTracker
    {
        private readonly ILogger _logger;

        public SimpleProgressTracker(ILogger logger)
        {
            _logger = logger;
        }

        public Task CompleteStepAsync(string message = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task FailStepAsync(string error, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task StartStepAsync(int totalItems, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task UpdateProgressAsync(int processedCount, string message)
        {
            if (processedCount % 100000 == 0)
            {
                _logger.LogInformation("{Message}", message);
            }
            return Task.CompletedTask;
        }

        public Task UpdateProgressAsync(int currentItem, string message = null, CancellationToken cancellationToken = default)
        {
            //throw new NotImplementedException();
            return Task.CompletedTask;
        }
    }
}
