using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public interface IMatchDefinitionFilter
    {
        /// <summary>
        /// Get match definitions that are relevant to a specific data source pair
        /// </summary>
        List<MatchLogic.Domain.Entities.MatchDefinition> GetRelevantDefinitions(
            MatchDefinitionCollection matchDefinitions,
            MatchingDataSourcePair dataSourcePair);

        /// <summary>
        /// Get match definitions that are relevant to a specific data source pair
        /// </summary>
        List<MatchLogic.Domain.Entities.MatchDefinition> GetRelevantDefinitions(
            IEnumerable<MatchLogic.Domain.Entities.MatchDefinition> matchDefinitions,
            MatchingDataSourcePair dataSourcePair);

        /// <summary>
        /// Extract all unique data source IDs from a match definition's criteria
        /// </summary>
        HashSet<Guid> ExtractDataSourceIds(MatchLogic.Domain.Entities.MatchDefinition matchDefinition);

        /// <summary>
        /// Check if a match definition is applicable to a data source pair
        /// </summary>
        bool IsDefinitionRelevant(MatchLogic.Domain.Entities.MatchDefinition matchDefinition, MatchingDataSourcePair dataSourcePair);

        /// <summary>
        /// Get field mappings for specific data sources from a match definition
        /// </summary>
        Dictionary<Guid, List<FieldMapping>> GetFieldMappingsByDataSource(MatchLogic.Domain.Entities.MatchDefinition matchDefinition);

        /// <summary>
        /// Validate if required fields exist in the data sources
        /// </summary>
        Task<bool> ValidateFieldAvailabilityAsync(
            MatchLogic.Domain.Entities.MatchDefinition matchDefinition,
            Guid projectId);
    }
}
