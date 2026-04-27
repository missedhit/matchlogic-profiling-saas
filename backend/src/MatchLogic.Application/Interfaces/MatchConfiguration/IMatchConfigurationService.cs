using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Project;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.MatchConfiguration
{
    /// <summary>
    /// Interface for the match configuration service
    /// </summary>
    public interface IMatchConfigurationService
    {
        #region MatchDefinition Operations
        // MatchDefinitionCollection operations
        Task<MatchDefinitionCollection> GetCollectionByIdAsync(Guid id);
        Task<MatchDefinitionCollection> GetCollectionsByProjectIdAsync(Guid projectId);
        Task<Guid> CreateCollectionAsync(MatchDefinitionCollection collection);
        Task UpdateCollectionAsync(MatchDefinitionCollection collection);
        Task DeleteCollectionAsync(Guid id);
        #endregion

        #region DataSourcePair Operations
        // DataSourcePairs operations
        Task<MatchingDataSourcePairs> GetDataSourcePairsByIdAsync(Guid id);
        Task<MatchingDataSourcePairs> GetDataSourcePairsByProjectIdAsync(Guid projectId);
        Task<MatchingDataSourcePair> FindDataSourcePairAsync(Guid projectId, Guid dataSourceAId, Guid dataSourceBId);
        Task<MatchingDataSourcePair> CreateDataSourcePairAsync(
            Guid projectId,
            Guid dataSourceAId,
            Guid dataSourceBId);

        Task<MatchingDataSourcePairs> CreateDataSourcePairsAsync(
            Guid projectId,
            MatchingDataSourcePairs matchingDataSourcePairs);

        Task<MatchingDataSourcePairs> UpdateDataSourcePairsAsync(
           Guid projectId,
           MatchingDataSourcePairs matchingDataSourcePairs);

        Task DeleteDataSourcePairAsync(Guid Id);

        #endregion

        #region Field List Operations
        // Field List operations
        Task<MatchDefinitionCollectionFieldListDto> GetFieldListConfigurationAsync(Guid collectionId);
        Task<Guid> SaveFieldListConfigurationAsync(MatchDefinitionCollectionFieldListDto dto);
        #endregion        

        #region Mapped Row Operations
        // Mapped Row operations
        Task<MatchDefinitionCollectionMappedRowDto> GetMappedRowConfigurationAsync(Guid collectionId);
        Task<MatchDefinitionCollectionMappedRowDto> GetMappedRowConfigurationByProjectIdAsync(Guid projectId);
        Task<Guid> SaveMappedRowConfigurationAsync(MatchDefinitionCollectionMappedRowDto dto);
        #endregion

        #region MatchSettings Operations

        /// <summary>   
        /// Get match settings for a project
        /// </summary>
        Task<MatchSettings> GetSettingsByProjectIdAsync(Guid projectId);

        /// <summary>
        /// Save match settings for a project
        /// </summary>
        Task<Guid> SaveSettingsAsync(MatchSettings settings);

        #endregion
    }
}
