using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
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
    public class IndexNodeInitializer
    {
        private readonly LiveSearchConfiguration _config;
        private readonly ProductionQGramIndexerDME _indexer;
        private readonly IDataStore _dataStore;
        private readonly IIndexPersistenceService _persistenceService;
        private readonly IQGramIndexManager _indexManager;
        private readonly IRecordStoreManager _recordStoreManager;
        private readonly IGenericRepository<DataSource, Guid> _dataSourceRepo;
        private readonly IGenericRepository<MatchLogic.Domain.Entities.MatchDefinitionCollection, Guid> _matchDefRepo;
        private readonly LiveSearchIndexBuilder _indexBuilder;
        private readonly ILogger<IndexNodeInitializer> _logger;

        public IndexNodeInitializer(
            IOptions<LiveSearchConfiguration> config,
            ProductionQGramIndexerDME indexer,
            IDataStore dataStore,
            IIndexPersistenceService persistenceService,
            IQGramIndexManager indexManager,
            IRecordStoreManager recordStoreManager,
            IGenericRepository<DataSource, Guid> dataSourceRepo,
            IGenericRepository<MatchLogic.Domain.Entities.MatchDefinitionCollection, Guid> matchDefRepo,
            LiveSearchIndexBuilder indexBuilder,
            ILogger<IndexNodeInitializer> logger)
        {
            _config = config?.Value ?? throw new ArgumentNullException(nameof(config));
            _indexer = indexer ?? throw new ArgumentNullException(nameof(indexer));
            _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            _persistenceService = persistenceService ?? throw new ArgumentNullException(nameof(persistenceService));
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
            _recordStoreManager = recordStoreManager ?? throw new ArgumentNullException(nameof(recordStoreManager));
            _dataSourceRepo = dataSourceRepo ?? throw new ArgumentNullException(nameof(dataSourceRepo));
            _matchDefRepo = matchDefRepo ?? throw new ArgumentNullException(nameof(matchDefRepo));
            _indexBuilder = indexBuilder ?? throw new ArgumentNullException(nameof(indexBuilder));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task InitializeAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation(
                "Initializing Live Search node: IsIndexingNode={IsIndexing}, ProjectId={ProjectId}",
                _config.IsIndexingNode,
                _config.ProjectId);

            var startTime = DateTime.UtcNow;

            try
            {
                if (_config.IsIndexingNode)
                {
                    await InitializeIndexingNodeAsync(cancellationToken);
                }
                else
                {
                    await InitializeQueryNodeAsync(cancellationToken);
                }

                var duration = DateTime.UtcNow - startTime;
                _logger.LogInformation(
                    "Node initialization completed in {Duration:F2} seconds",
                    duration.TotalSeconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Live Search node");
                throw;
            }
        }

        private IRecordStore GetRecordStoreFromIndexer(Guid dataSourceId)
        {
            // ProductionQGramIndexerDME stores record stores internally
            // We need to expose them via a new method
            return _indexBuilder.GetRecordStore(dataSourceId);
        }

        #region Indexing Node Initialization

        private async Task InitializeIndexingNodeAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Starting INDEXING NODE initialization");

            // Step 1: Load data sources and match definitions
            var dataSources = await LoadDataSourcesAsync(cancellationToken);
            var matchDefinitionCollection = await LoadMatchDefinitionsAsync(cancellationToken);

            _logger.LogInformation(
                "Loaded {DataSourceCount} data sources and {MatchDefCount} match definitions",
                dataSources.Count,
                matchDefinitionCollection.Definitions.Count);

            // Step 2: Build Q-gram index using wrapper (which delegates to ProductionQGramIndexerDME)
            _logger.LogInformation(
                $"IndexBuilder is available {_indexBuilder != null}");

            var indexData = await _indexBuilder.BuildIndexAsync(
                dataSources,
                matchDefinitionCollection,
                cancellationToken);

            _logger.LogInformation(
                "Index built: {TotalRecords} records, {FieldCount} fields",
                indexData.TotalRecords,
                indexData.GlobalFieldIndex.Count);

            // Step 3: Persist index
            var collectionName = $"qgram_index_{GuidCollectionNameConverter.ToValidCollectionName(_config.ProjectId)}";
            await _dataStore.DeleteCollection(collectionName);
            await _persistenceService.SaveIndexAsync(
                _dataStore,
                collectionName,
                indexData,
                cancellationToken);

            _logger.LogInformation("Index persisted to collection {Collection}", collectionName);

            // Step 4: Store in memory (IQGramIndexManager already has it loaded via ProductionQGramIndexerDME)
            await _indexManager.StoreIndexAsync(_config.ProjectId, indexData, cancellationToken);

            // Step 5: Register record stores from ProductionQGramIndexerDME to IRecordStoreManager
            foreach (var dataSource in dataSources)
            {
                var store = _indexer.GetRecordStore(dataSource.Id);
                if (store != null)
                {
                    _logger.LogInformation("Registering Store for Datasource {Name}", dataSource.Name);
                    _indexer.RegisterRecordStore(dataSource.Id, store);
                    _recordStoreManager.RegisterRecordStore(dataSource.Id, store);
                }
            }

            _logger.LogInformation("INDEXING NODE initialization completed");
        }

        #endregion

        #region Query Node Initialization

        private async Task InitializeQueryNodeAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Starting QUERY NODE initialization");

            // Step 1: Load data sources
            var dataSources = await LoadDataSourcesAsync(cancellationToken);

            // Step 2: Wait for and load index
            var timeout = TimeSpan.FromMinutes(_config.IndexPollTimeoutMinutes);
            var pollInterval = TimeSpan.FromSeconds(_config.IndexPollIntervalSeconds);

            var indexLoaded = await _indexManager.WaitForIndexAvailabilityAsync(
                _config.ProjectId,
                timeout,
                pollInterval,
                cancellationToken);

            if (!indexLoaded)
            {
                throw new TimeoutException(
                    $"Index not available after {timeout.TotalMinutes} minutes. " +
                    "Ensure indexing node is running and has completed indexing.");
            }

            var indexData = await _indexManager.GetIndexAsync(_config.ProjectId, cancellationToken);

            _logger.LogInformation(
                "Index loaded: {TotalRecords} records, {FieldCount} fields",
                indexData.TotalRecords,
                indexData.GlobalFieldIndex.Count);

            // Step 3: Load index into ProductionQGramIndexerDME using NEW method
            _indexer.LoadIndexDataFromPersistence(indexData);

            // Step 4: Build record stores
            var dataSourceIds = dataSources.Select(ds => ds.Id).ToList();
            await _recordStoreManager.BuildStoresAsync(
                _config.ProjectId,
                dataSourceIds,
                progress: null,
                cancellationToken);

            // Step 5: Register record stores with ProductionQGramIndexerDME
            foreach (var dataSource in dataSources)
            {
                var store = _recordStoreManager.GetRecordStoreAsync(
                    dataSource.Id);

                if (store != null)
                {
                    _indexer.RegisterRecordStore(dataSource.Id, store);
                }
            }

            _logger.LogInformation("QUERY NODE initialization completed");
        }

        #endregion

        #region Helper Methods

        private async Task<List<DataSource>> LoadDataSourcesAsync(CancellationToken cancellationToken)
        {
            var dataSources = await _dataSourceRepo.QueryAsync(
                ds => ds.ProjectId == _config.ProjectId,
                Constants.Collections.DataSources);

            return dataSources.ToList();
        }

        private async Task<MatchLogic.Domain.Entities.MatchDefinitionCollection> LoadMatchDefinitionsAsync(CancellationToken cancellationToken)
        {
            var matchDefs = await _matchDefRepo.QueryAsync(
                md => md.ProjectId == _config.ProjectId,
                Constants.Collections.MatchDefinitionCollection);

            return matchDefs.First();
        }

        #endregion
    }
}
