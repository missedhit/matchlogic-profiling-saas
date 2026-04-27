using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Application.Common;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    /// <summary>
    /// Maps data sources and match definitions to integer indices using repository pattern
    /// </summary>
    public sealed class DataSourceIndexMapper : IDataSourceIndexMapper
    {
        private readonly ILogger<DataSourceIndexMapper> _logger;
        private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
        private readonly IGenericRepository<MatchDefinitionCollection, Guid> _matchDefinitionRepository;

        // Bidirectional mappings for data sources
        private readonly Dictionary<Guid, int> _dataSourceToIndex;
        private readonly Dictionary<int, Guid> _indexToDataSource;

        // Bidirectional mappings for match definitions  
        private readonly Dictionary<Guid, int> _definitionToIndex;
        private readonly Dictionary<int, Guid> _indexToDefinition;

        private bool _isInitialized;

        public bool IsInitialized => _isInitialized;
        public int DataSourceCount => _dataSourceToIndex.Count;
        public int DefinitionCount => _definitionToIndex.Count;

        public DataSourceIndexMapper(
            ILogger<DataSourceIndexMapper> logger,
            IGenericRepository<DataSource, Guid> dataSourceRepository,
            IGenericRepository<MatchDefinitionCollection, Guid> matchDefinitionRepository)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _dataSourceRepository = dataSourceRepository ?? throw new ArgumentNullException(nameof(dataSourceRepository));
            _matchDefinitionRepository = matchDefinitionRepository ?? throw new ArgumentNullException(nameof(matchDefinitionRepository));

            _dataSourceToIndex = new Dictionary<Guid, int>();
            _indexToDataSource = new Dictionary<int, Guid>();
            _definitionToIndex = new Dictionary<Guid, int>();
            _indexToDefinition = new Dictionary<int, Guid>();

            _isInitialized = false;
        }

        public async Task InitializeAsync(Guid projectId)
        {
            if (projectId == Guid.Empty)
                throw new ArgumentException("Invalid project ID", nameof(projectId));

            Clear();

            // Fetch data sources from repository in their stored order
            var dataSources = await _dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == projectId,
                Common.Constants.Collections.DataSources);

            // Index data sources based on retrieval order
            int dsIndex = 0;
            foreach (var dataSource in dataSources)
            {
                _dataSourceToIndex[dataSource.Id] = dsIndex;
                _indexToDataSource[dsIndex] = dataSource.Id;
                dsIndex++;
            }

            // Fetch match definition collections
            var matchCollections = await _matchDefinitionRepository.QueryAsync(
                mc => mc.ProjectId == projectId,
                Common.Constants.Collections.MatchDefinitionCollection);

            // Index all match definitions from all collections
            int defIndex = 0;
            foreach (var collection in matchCollections)
            {
                foreach (var definition in collection.Definitions)
                {
                    _definitionToIndex[definition.Id] = definition.UIDefinitionIndex;
                    _indexToDefinition[defIndex] = definition.Id;
                    defIndex++;
                }
            }

            _isInitialized = true;

            _logger.LogInformation(
                "Initialized mapper for project {ProjectId}: {DataSourceCount} data sources, {DefinitionCount} definitions",
                projectId,
                _dataSourceToIndex.Count,
                _definitionToIndex.Count);
        }

        public int GetDataSourceIndex(Guid dataSourceId)
        {
            EnsureInitialized();

            if (_dataSourceToIndex.TryGetValue(dataSourceId, out int index))
                return index;

            throw new KeyNotFoundException($"Data source ID {dataSourceId} not found");
        }

        public int GetDefinitionIndex(Guid definitionId)
        {
            EnsureInitialized();

            if (_definitionToIndex.TryGetValue(definitionId, out int index))
                return index;

            throw new KeyNotFoundException($"Definition ID {definitionId} not found");
        }

        public Guid GetDataSourceId(int index)
        {
            EnsureInitialized();

            if (_indexToDataSource.TryGetValue(index, out Guid id))
                return id;

            throw new ArgumentOutOfRangeException(nameof(index), $"Invalid data source index: {index}");
        }

        public Guid GetDefinitionId(int index)
        {
            EnsureInitialized();

            if (_indexToDefinition.TryGetValue(index, out Guid id))
                return id;

            throw new ArgumentOutOfRangeException(nameof(index), $"Invalid definition index: {index}");
        }

        public bool TryGetDataSourceIndex(Guid dataSourceId, out int index)
        {
            if (!_isInitialized)
            {
                index = -1;
                return false;
            }

            return _dataSourceToIndex.TryGetValue(dataSourceId, out index);
        }

        public bool TryGetDefinitionIndex(Guid definitionId, out int index)
        {
            if (!_isInitialized)
            {
                index = -1;
                return false;
            }

            return _definitionToIndex.TryGetValue(definitionId, out index);
        }

        public IReadOnlyCollection<Guid> GetAllDataSourceIds()
        {
            EnsureInitialized();
            return _dataSourceToIndex.Keys.ToList().AsReadOnly();
        }

        public IReadOnlyCollection<Guid> GetAllDefinitionIds()
        {
            EnsureInitialized();
            return _definitionToIndex.Keys.ToList().AsReadOnly();
        }

        public IReadOnlyDictionary<Guid, int> GetDataSourceIndexMap()
        {
            EnsureInitialized();
            return new Dictionary<Guid, int>(_dataSourceToIndex);
        }

        public IReadOnlyDictionary<Guid, int> GetDefinitionIndexMap()
        {
            EnsureInitialized();
            return new Dictionary<Guid, int>(_definitionToIndex);
        }

        public bool TryGetDataSourceName(Guid dataSourceId, out string dsName) => throw new NotImplementedException();

        private void EnsureInitialized()
        {
            if (!_isInitialized)
                throw new InvalidOperationException("DataSourceIndexMapper is not initialized. Call InitializeAsync or InitializeForJobAsync first.");
        }

        private void Clear()
        {
            _dataSourceToIndex.Clear();
            _indexToDataSource.Clear();
            _definitionToIndex.Clear();
            _indexToDefinition.Clear();
            _isInitialized = false;
        }
    }
}
