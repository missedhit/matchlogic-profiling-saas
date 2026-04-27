using MatchLogic.Application.Common;
using MatchLogic.Application.Features.MatchDefinition.Adapters;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchDefinition
{
    /// <summary>
    /// Service for match configuration operations
    /// </summary>
    public class MatchConfigurationService : IMatchConfigurationService
    {
        private readonly IGenericRepository<MatchDefinitionCollection, Guid> _collectionRepository;
        private readonly IGenericRepository<MatchingDataSourcePairs, Guid> _pairsRepository;
        private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
        private readonly IGenericRepository<MatchSettings, Guid> _settingsRepository;
        private readonly MatchDefinitionAdapter _adapter;

        private const string MATCH_COLLECTION_NAME = Constants.Collections.MatchDefinitionCollection;
        private const string PAIRS_COLLECTION_NAME = Constants.Collections.MatchDataSourcePairs;
        private const string MATCH_SETTINGS_COLLECTION_NAME = Constants.Collections.MatchSettings;

        public MatchConfigurationService(
            IGenericRepository<MatchDefinitionCollection, Guid> collectionRepository,
            IGenericRepository<MatchingDataSourcePairs, Guid> pairsRepository,
            IGenericRepository<DataSource, Guid> dataSourceRepository,
            IGenericRepository<MatchSettings, Guid> settingsRepository)
        {
            _collectionRepository = collectionRepository;
            _pairsRepository = pairsRepository;
            _dataSourceRepository = dataSourceRepository;
            _adapter = new MatchDefinitionAdapter();
            _settingsRepository = settingsRepository;
        }

        #region MatchDefinitionCollection Operations

        /// <summary>
        /// Get a match definition collection by ID
        /// </summary>
        public async Task<MatchDefinitionCollection> GetCollectionByIdAsync(Guid id)
        {
            return await _collectionRepository.GetByIdAsync(id, MATCH_COLLECTION_NAME);
        }

        /// <summary>
        /// Get all match definition collections for a project
        /// </summary>
        public async Task<MatchDefinitionCollection> GetCollectionsByProjectIdAsync(Guid projectId)
        {
            var collection = await _collectionRepository.QueryAsync(
                m => m.ProjectId == projectId,
                MATCH_COLLECTION_NAME);
            return collection?.FirstOrDefault();
        }

        /// <summary>
        /// Create a new match definition collection
        /// </summary>
        public async Task<Guid> CreateCollectionAsync(MatchDefinitionCollection collection)
        {
            if (collection.Id == Guid.Empty)
            {
                collection.Id = Guid.NewGuid();
            }

            await _collectionRepository.InsertAsync(collection, MATCH_COLLECTION_NAME);
            return collection.Id;
        }

        /// <summary>
        /// Update an existing match definition collection
        /// </summary>
        public async Task UpdateCollectionAsync(MatchDefinitionCollection collection)
        {
            await _collectionRepository.UpdateAsync(collection, MATCH_COLLECTION_NAME);
        }

        /// <summary>
        /// Delete a match definition collection
        /// </summary>
        public async Task DeleteCollectionAsync(Guid id)
        {
            await _collectionRepository.DeleteAsync(id, MATCH_COLLECTION_NAME);
        }

        #endregion

        #region DataSourcePair Operations

        /// <summary>
        /// Get a data source pairs collection by ID
        /// </summary>
        public async Task<MatchingDataSourcePairs> GetDataSourcePairsByIdAsync(Guid id)
        {
            return await _pairsRepository.GetByIdAsync(id, PAIRS_COLLECTION_NAME);
        }

        /// <summary>
        /// Get data source pairs collection by projectId
        /// </summary>
        /// <param name="projectId">project id</param>
        /// <returns></returns>
        public async Task<MatchingDataSourcePairs> GetDataSourcePairsByProjectIdAsync(Guid projectId)
        {
            var pairsCollections = await _pairsRepository.QueryAsync(
                p => p.ProjectId == projectId,
                PAIRS_COLLECTION_NAME);

            return pairsCollections.FirstOrDefault();
        }

        /// <summary>
        /// Find a data source pair by data source IDs
        /// </summary>
        public async Task<MatchingDataSourcePair> FindDataSourcePairAsync(Guid projectId, Guid dataSourceAId, Guid dataSourceBId)
        {
            var pairsCollection = await GetDataSourcePairsByProjectIdAsync(projectId);

            if (pairsCollection != null && pairsCollection.Contains(dataSourceAId, dataSourceBId))
            {
                return new MatchingDataSourcePair(dataSourceAId, dataSourceBId);
            }

            return null;
        }

        /// <summary>
        /// Find or create a data source pair
        /// </summary>
        public async Task<MatchingDataSourcePair> CreateDataSourcePairAsync(
            Guid projectId,
            Guid dataSourceAId,
            Guid dataSourceBId)
        {
            var pair = await FindDataSourcePairAsync(projectId, dataSourceAId, dataSourceBId);
            if (pair != null)
            {
                return pair;
            }

            // Get or create a pairs collection for this project
            var pairsCollection = await GetDataSourcePairsByProjectIdAsync(projectId);

            if (pairsCollection == null)
            {
                pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
                {
                    ProjectId = projectId
                };

                await _pairsRepository.InsertAsync(pairsCollection, PAIRS_COLLECTION_NAME);
            }

            // Add the new pair
            pairsCollection.Add(dataSourceAId, dataSourceBId);
            await _pairsRepository.UpdateAsync(pairsCollection, PAIRS_COLLECTION_NAME);

            return new MatchingDataSourcePair(dataSourceAId, dataSourceBId);
        }

        /// <summary>
        /// Get data source pairs for a list of data sources
        /// </summary>
        private async Task<List<MatchingDataSourcePair>> GetExistingDataSourcePairsAsync(Guid projectId, List<DataSource> dataSources)
        {
            if (dataSources == null || dataSources.Count == 0)
                return new List<MatchingDataSourcePair>();

            var dataSourceIds = dataSources.Select(ds => ds.Id).ToList();
            var result = new List<MatchingDataSourcePair>();

            // Get pairs collection for this project
            var pairsCollection = await GetDataSourcePairsByProjectIdAsync(projectId);

            if (pairsCollection != null)
            {
                for (int i = 0; i < pairsCollection.Count; i++)
                {
                    var pair = pairsCollection[i];
                    if (dataSourceIds.Contains(pair.DataSourceA) && dataSourceIds.Contains(pair.DataSourceB))
                    {
                        result.Add(pair);
                    }
                }
            }

            return result;
        }

        public async Task<MatchingDataSourcePairs> CreateDataSourcePairsAsync(Guid projectId, MatchingDataSourcePairs matchingDataSourcePairs)
        {
            await _pairsRepository.InsertAsync(matchingDataSourcePairs, PAIRS_COLLECTION_NAME);
            return await GetDataSourcePairsByProjectIdAsync(projectId);
        }

        public async Task<MatchingDataSourcePairs> UpdateDataSourcePairsAsync(Guid projectId, MatchingDataSourcePairs matchingDataSourcePairs)
        {
            var dataSourcePairToDelete = await _pairsRepository.QueryAsync(x => x.ProjectId == projectId, PAIRS_COLLECTION_NAME);
            if (dataSourcePairToDelete != null && dataSourcePairToDelete.Count > 0)
            {
                await _pairsRepository.DeleteAsync(dataSourcePairToDelete.First().Id, PAIRS_COLLECTION_NAME);
            }
            return await CreateDataSourcePairsAsync(projectId, matchingDataSourcePairs);
        }

        public async Task DeleteDataSourcePairAsync(Guid Id)
        {
            await _pairsRepository.DeleteAsync(Id, PAIRS_COLLECTION_NAME);
        }
        #endregion

        #region Field List Operations

        /// <summary>
        /// Get match definitions organized by data source pair
        /// </summary>
        public async Task<MatchDefinitionCollectionFieldListDto> GetFieldListConfigurationAsync(Guid collectionId)
        {
            var collection = await GetCollectionByIdAsync(collectionId);
            if (collection == null)
            {
                throw new InvalidOperationException($"Match definition collection with ID {collectionId} not found");
            }

            return _adapter.ToFieldListDto(collection);
        }

        /// <summary>
        /// Save match definitions organized by data source pair
        /// </summary>
        public async Task<Guid> SaveFieldListConfigurationAsync(MatchDefinitionCollectionFieldListDto dto)
        {
            var collection = _adapter.FromFieldListDto(dto);

            if (dto.Id == Guid.Empty)
            {
                // Create new
                return await CreateCollectionAsync(collection);
            }
            else
            {
                // Update existing
                await UpdateCollectionAsync(collection);
                return dto.Id;
            }
        }

        #endregion

        #region Mapped Row Operations
        /// <summary>
        /// Get match definition from projectId
        /// </summary>
        /// <param name="projectId"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public async Task<MatchDefinitionCollectionMappedRowDto> GetMappedRowConfigurationByProjectIdAsync(Guid projectId)
        {
            var collection = await GetCollectionsByProjectIdAsync(projectId);
            //if (collection == null)
            //{
            //    throw new InvalidOperationException($"Match definition collection with project ID {projectId} not found");
            //}
            return _adapter.ToMappedRowDto(collection);
        }
        /// <summary>
        /// Get match definitions with mapped rows
        /// </summary>
        public async Task<MatchDefinitionCollectionMappedRowDto> GetMappedRowConfigurationAsync(Guid collectionId)
        {
            var collection = await GetCollectionByIdAsync(collectionId);
            if (collection == null)
            {
                throw new InvalidOperationException($"Match definition collection with ID {collectionId} not found");
            }

            return _adapter.ToMappedRowDto(collection);
        }

        /// <summary>
        /// Save match definitions with mapped rows
        /// </summary>
        public async Task<Guid> SaveMappedRowConfigurationAsync(MatchDefinitionCollectionMappedRowDto dto)
        {
            // Extract all data source IDs using LINQ
            var dataSourceIds = dto.Definitions
                .SelectMany(def => def.Criteria)
                .SelectMany(crit => crit.MappedRow.FieldsByDataSource.Values)
                .Select(field => field.DataSourceId)
                .ToHashSet();

            var dataSources = await GetDataSourcesAsync(dataSourceIds);
            var dataSourcePairs = await GetExistingDataSourcePairsAsync(dto.ProjectId, dataSources);

            // Convert to domain model
            var collection = _adapter.FromMappedRowDto(dto, dataSourcePairs);

            if (dto.Id == Guid.Empty)
            {
                // Create new
                return await CreateCollectionAsync(collection);
            }
            else
            {
                // Update existing
                await UpdateCollectionAsync(collection);
                return dto.Id;
            }
        }

        #endregion        

        #region MatchSettings Operations

        /// <summary>
        /// Get match settings for a project
        /// </summary>
        public async Task<MatchSettings> GetSettingsByProjectIdAsync(Guid projectId)
        {
            var settings = await _settingsRepository.QueryAsync(
                s => s.ProjectId == projectId,
                MATCH_SETTINGS_COLLECTION_NAME);

            return settings.FirstOrDefault();
        }

        /// <summary>
        /// Save match settings for a project
        /// </summary>
        public async Task<Guid> SaveSettingsAsync(MatchSettings settings)
        {
            var existingSettings = await GetSettingsByProjectIdAsync(settings.ProjectId);

            if (existingSettings != null)
            {
                settings.Id = existingSettings.Id;
                await _settingsRepository.UpdateAsync(settings, MATCH_SETTINGS_COLLECTION_NAME);
                return settings.Id;
            }
            else
            {
                if (settings.Id == Guid.Empty)
                {
                    settings.Id = Guid.NewGuid();
                }

                await _settingsRepository.InsertAsync(settings, MATCH_SETTINGS_COLLECTION_NAME);
                return settings.Id;
            }
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Get data sources by IDs
        /// </summary>
        private async Task<List<DataSource>> GetDataSourcesAsync(HashSet<Guid> dataSourceIds)
        {
            var result = new List<DataSource>();

            foreach (var id in dataSourceIds)
            {
                var dataSource = await _dataSourceRepository.GetByIdAsync(id, Constants.Collections.DataSources);
                if (dataSource != null)
                {
                    result.Add(new DataSource
                    {
                        Id = dataSource.Id,
                        Name = dataSource.Name
                    });
                }
            }

            return result;
        }

        #endregion
    }
}
