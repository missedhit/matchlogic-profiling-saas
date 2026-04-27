using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class MatchDefinitionFilter : IMatchDefinitionFilter
    {
        private readonly ILogger<MatchDefinitionFilter> _logger;
        private readonly IAutoMappingService _autoMappingService;
        private readonly IGenericRepository<MatchingDataSourcePairs, Guid> _dataSourcePairsRepository;

        public MatchDefinitionFilter(
            ILogger<MatchDefinitionFilter> logger,
            IAutoMappingService autoMappingService,
            IGenericRepository<MatchingDataSourcePairs, Guid> dataSourcePairsRepository)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _autoMappingService = autoMappingService ?? throw new ArgumentNullException(nameof(autoMappingService));
            _dataSourcePairsRepository = dataSourcePairsRepository ?? throw new ArgumentNullException(nameof(dataSourcePairsRepository));
        }

        public List<MatchLogic.Domain.Entities.MatchDefinition> GetRelevantDefinitions(
            MatchDefinitionCollection matchDefinitions,
            MatchingDataSourcePair dataSourcePair)
        {
            if (matchDefinitions == null)
                throw new ArgumentNullException(nameof(matchDefinitions));
            if (dataSourcePair == null)
                throw new ArgumentNullException(nameof(dataSourcePair));

            return GetRelevantDefinitions(matchDefinitions.Definitions, dataSourcePair);
        }

        public List<MatchLogic.Domain.Entities.MatchDefinition> GetRelevantDefinitions(
            IEnumerable<MatchLogic.Domain.Entities.MatchDefinition> matchDefinitions,
            MatchingDataSourcePair dataSourcePair)
        {
            if (matchDefinitions == null)
                throw new ArgumentNullException(nameof(matchDefinitions));
            if (dataSourcePair == null)
                throw new ArgumentNullException(nameof(dataSourcePair));

            var relevantDefinitions = new List<MatchLogic.Domain.Entities.MatchDefinition>();

            foreach (var definition in matchDefinitions)
            {
                if (IsDefinitionRelevant(definition, dataSourcePair))
                {
                    relevantDefinitions.Add(definition);
                    _logger.LogDebug(
                        "Definition {DefinitionId} (UIIndex: {UIIndex}) is relevant for pair [{SourceA}, {SourceB}]",
                        definition.Id, definition.UIDefinitionIndex, dataSourcePair.DataSourceA, dataSourcePair.DataSourceB);
                }
            }

            _logger.LogInformation(
                "Found {Count} relevant definitions for data source pair [{SourceA}, {SourceB}]",
                relevantDefinitions.Count, dataSourcePair.DataSourceA, dataSourcePair.DataSourceB);

            return relevantDefinitions;
        }

        public bool IsDefinitionRelevant(MatchLogic.Domain.Entities.MatchDefinition matchDefinition, MatchingDataSourcePair dataSourcePair)
        {
            if (matchDefinition == null || dataSourcePair == null)
                return false;

            // First check: Does the DataSourcePairId match?
            // The DataSourcePairId in the definition should correspond to this pair
            // Note: We might need to fetch the actual pair from repository to compare

            // Extract data sources from the field mappings in criteria
            var definitionDataSources = ExtractDataSourceIds(matchDefinition);

            if (!definitionDataSources.Any())
            {
                _logger.LogWarning("Match definition {DefinitionId} has no data source references in field mappings",
                    matchDefinition.Id);
                return false;
            }

            var pairDataSources = new HashSet<Guid> { dataSourcePair.DataSourceA, dataSourcePair.DataSourceB };

            // For deduplication (same source in pair)
            if (dataSourcePair.DataSourceA == dataSourcePair.DataSourceB)
            {
                // All field mappings should reference only this single data source
                return definitionDataSources.Count == 1 &&
                       definitionDataSources.Contains(dataSourcePair.DataSourceA);
            }

            // For cross-source matching
            // The definition's field mappings should use exactly the data sources in the pair
            bool exactMatch = definitionDataSources.SetEquals(pairDataSources);

            if (!exactMatch)
            {
                // Check if it's a subset match (definition uses only some sources from the pair)
                bool isSubset = definitionDataSources.IsSubsetOf(pairDataSources);

                if (isSubset && definitionDataSources.Count == 1)
                {
                    // Definition only uses one source from a two-source pair - not valid for cross-matching
                    _logger.LogDebug(
                        "Definition {DefinitionId} only uses one source {Source} from cross-source pair",
                        matchDefinition.Id, definitionDataSources.First());
                    return false;
                }
            }

            return exactMatch;
        }

        public HashSet<Guid> ExtractDataSourceIds(MatchLogic.Domain.Entities.MatchDefinition matchDefinition)
        {
            var dataSourceIds = new HashSet<Guid>();

            if (matchDefinition?.Criteria == null)
                return dataSourceIds;

            foreach (var criterion in matchDefinition.Criteria)
            {
                if (criterion.FieldMappings != null)
                {
                    foreach (var fieldMapping in criterion.FieldMappings)
                    {
                        if (fieldMapping.DataSourceId != Guid.Empty)
                        {
                            dataSourceIds.Add(fieldMapping.DataSourceId);
                        }
                    }
                }
            }

            return dataSourceIds;
        }

        public Dictionary<Guid, List<FieldMapping>> GetFieldMappingsByDataSource(MatchLogic.Domain.Entities.MatchDefinition matchDefinition)
        {
            var mappingsBySource = new Dictionary<Guid, List<FieldMapping>>();

            if (matchDefinition?.Criteria == null)
                return mappingsBySource;

            foreach (var criterion in matchDefinition.Criteria)
            {
                if (criterion.FieldMappings == null)
                    continue;

                foreach (var fieldMapping in criterion.FieldMappings)
                {
                    if (fieldMapping.DataSourceId == Guid.Empty)
                        continue;

                    if (!mappingsBySource.ContainsKey(fieldMapping.DataSourceId))
                    {
                        mappingsBySource[fieldMapping.DataSourceId] = new List<FieldMapping>();
                    }

                    // Avoid duplicate field mappings
                    if (!mappingsBySource[fieldMapping.DataSourceId].Any(fm =>
                        fm.FieldName == fieldMapping.FieldName))
                    {
                        mappingsBySource[fieldMapping.DataSourceId].Add(fieldMapping);
                    }
                }
            }

            return mappingsBySource;
        }

        public async Task<bool> ValidateFieldAvailabilityAsync(
            MatchLogic.Domain.Entities.MatchDefinition matchDefinition,
            Guid projectId)
        {
            if (matchDefinition == null)
                return false;

            if (projectId == Guid.Empty)
            {
                _logger.LogError("Invalid project ID for field validation");
                return false;
            }

            try
            {
                // Get all fields for all data sources in the project
                var fieldsPerDataSource = await _autoMappingService.GetExtendedFieldsAsync(projectId);

                if (!fieldsPerDataSource.Any())
                {
                    _logger.LogWarning("No field mappings found for project {ProjectId}", projectId);
                    return false;
                }

                // Get field mappings from the match definition grouped by data source
                var requiredFieldsBySource = GetFieldMappingsByDataSource(matchDefinition);

                // Validate each data source has the required fields
                foreach (var (dataSourceId, requiredFieldMappings) in requiredFieldsBySource)
                {
                    // Find the data source name that corresponds to this ID
                    // We need to match by DataSourceId since fieldsPerDataSource is keyed by name
                    bool dataSourceFound = false;

                    foreach (var (dataSourceName, availableFields) in fieldsPerDataSource)
                    {
                        // Check if any field in this data source matches our data source ID
                        if (availableFields.Any(f => f.DataSourceId == dataSourceId))
                        {
                            dataSourceFound = true;

                            // Now validate all required fields exist
                            foreach (var requiredFieldMapping in requiredFieldMappings)
                            {
                                bool fieldExists = availableFields.Any(f =>
                                    string.Equals(f.FieldName, requiredFieldMapping.FieldName,
                                        StringComparison.OrdinalIgnoreCase));

                                if (!fieldExists)
                                {
                                    _logger.LogError(
                                        "Field '{FieldName}' not found in data source {DataSourceId} ({DataSourceName})",
                                        requiredFieldMapping.FieldName, dataSourceId, dataSourceName);
                                    return false;
                                }
                            }
                            break;
                        }
                    }

                    if (!dataSourceFound)
                    {
                        _logger.LogError(
                            "Data source {DataSourceId} not found in project {ProjectId}",
                            dataSourceId, projectId);
                        return false;
                    }
                }

                _logger.LogDebug(
                    "All fields validated successfully for definition {DefinitionId}",
                    matchDefinition.Id);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error validating fields for definition {DefinitionId} in project {ProjectId}",
                    matchDefinition.Id, projectId);
                return false;
            }
        }
    }
}
