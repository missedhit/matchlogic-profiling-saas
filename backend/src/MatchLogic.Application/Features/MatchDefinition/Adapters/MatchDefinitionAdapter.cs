using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchDefinition.Adapters
{
    public class MatchDefinitionAdapter
    {
        #region Domain to DTO Conversions

        /// <summary>
        /// Convert domain model to field list DTO
        /// </summary>
        public MatchDefinitionCollectionFieldListDto ToFieldListDto(MatchDefinitionCollection collection)
        {
            if (collection == null)
                return null;

            var dto = new MatchDefinitionCollectionFieldListDto
            {
                Id = collection.Id,
                ProjectId = collection.ProjectId,
                JobId = collection.JobId,
                Name = collection.Name,
                Definitions = new List<MatchDefinitionFieldListDto>()
            };

            // Create a lookup of data source pairs by ID for better performance
            var pairLookup = BuildDataSourcePairLookup(collection);

            // For each definition, create a field list DTO
            foreach (var definition in collection.Definitions)
            {
                // Skip if no pair found
                if (!pairLookup.TryGetValue(definition.DataSourcePairId, out var pair))
                    continue;

                var defDto = CreateFieldListDefinition(definition, pair);
                dto.Definitions.Add(defDto);
            }

            return dto;
        }

        private Dictionary<Guid, MatchingDataSourcePair> BuildDataSourcePairLookup(MatchDefinitionCollection collection)
        {
            var result = new Dictionary<Guid, MatchingDataSourcePair>();

            foreach (var definition in collection.Definitions)
            {
                if (result.ContainsKey(definition.DataSourcePairId))
                    continue;

                var pair = GetDataSourcePairById(definition.DataSourcePairId, collection);
                if (pair != null)
                {
                    result[definition.DataSourcePairId] = pair;
                }
            }

            return result;
        }

        private MatchDefinitionFieldListDto CreateFieldListDefinition(
            Domain.Entities.MatchDefinition definition,
            MatchingDataSourcePair pair)
        {
            var defDto = new MatchDefinitionFieldListDto
            {
                Id = definition.Id,
                DataSourcePairId = definition.DataSourcePairId,
                ProjectRunId = definition.ProjectRunId,
                DataSourcePairName = $"{pair.DataSourceA} - {pair.DataSourceB}",
                Criteria = new List<MatchCriterionFieldListDto>()
            };

            // Add criteria
            foreach (var criterion in definition.Criteria)
            {
                var critDto = CreateFieldListCriterion(criterion);
                defDto.Criteria.Add(critDto);
            }

            return defDto;
        }

        private MatchCriterionFieldListDto CreateFieldListCriterion(MatchCriteria criterion)
        {
            var critDto = new MatchCriterionFieldListDto
            {
                Id = criterion.Id,
                MatchingType = criterion.MatchingType,
                DataType = criterion.DataType,
                Weight = criterion.Weight,
                Arguments = new Dictionary<ArgsValue, string>(),
                Fields = criterion.FieldMappings
                    .Select(fm => new FieldDto
                    {
                        Id = fm.Id,
                        Name = fm.FieldName,
                        DataSourceId = fm.DataSourceId,
                        DataSourceName = fm.DataSourceName
                    })
                    .ToList()
            };

            // Copy arguments
            foreach (var arg in criterion.Arguments)
            {
                critDto.Arguments[arg.Key] = arg.Value;
            }

            return critDto;
        }


        #endregion

        #region DTO to Domain Conversions

        /// <summary>
        /// Convert field list DTO to domain model
        /// </summary>
        public MatchDefinitionCollection FromFieldListDto(MatchDefinitionCollectionFieldListDto dto)
        {
            if (dto == null)
                return null;

            var collection = new MatchDefinitionCollection
            {
                Id = dto.Id == Guid.Empty ? Guid.NewGuid() : dto.Id,
                ProjectId = dto.ProjectId,
                JobId = dto.JobId,
                Name = dto.Name,
                Definitions = new List<Domain.Entities.MatchDefinition>()
            };

            // Create a definition for each field list DTO
            foreach (var defDto in dto.Definitions)
            {
                var definition = CreateDefinitionFromFieldListDto(defDto);
                collection.Definitions.Add(definition);
            }

            return collection;
        }

        private Domain.Entities.MatchDefinition CreateDefinitionFromFieldListDto(MatchDefinitionFieldListDto defDto)
        {
            var definition = new Domain.Entities.MatchDefinition
            {
                Id = defDto.Id == Guid.Empty ? Guid.NewGuid() : defDto.Id,
                DataSourcePairId = defDto.DataSourcePairId,
                ProjectRunId = defDto.ProjectRunId,
                Criteria = new List<MatchCriteria>()
            };

            // Add criteria
            foreach (var critDto in defDto.Criteria)
            {
                var criterion = CreateCriterionFromFieldListDto(critDto);
                definition.Criteria.Add(criterion);
            }

            return definition;
        }

        private MatchCriteria CreateCriterionFromFieldListDto(MatchCriterionFieldListDto critDto)
        {
            var criterion = new MatchCriteria
            {
                Id = critDto.Id == Guid.Empty ? Guid.NewGuid() : critDto.Id,
                MatchingType = critDto.MatchingType,
                DataType = critDto.DataType,
                Weight = critDto.Weight,
                Arguments = new Dictionary<ArgsValue, string>(),
                FieldMappings = critDto.Fields
                    .Select(f => new FieldMapping
                    {
                        Id = f.Id == Guid.Empty ? Guid.NewGuid() : f.Id,
                        DataSourceId = f.DataSourceId,
                        DataSourceName = f.DataSourceName,
                        FieldName = f.Name
                    })
                    .ToList()
            };

            // Copy arguments
            foreach (var arg in critDto.Arguments)
            {
                criterion.Arguments[arg.Key] = arg.Value;
            }

            return criterion;
        }

        /// <summary>
        /// Convert mapped row DTO to domain model to track UI definition index
        /// </summary>                
        public MatchDefinitionCollection FromMappedRowDto(
            MatchDefinitionCollectionMappedRowDto dto,
            List<MatchingDataSourcePair> dataSourcePairs)
        {
            if (dto == null || dataSourcePairs == null)
                return null;

            var collection = new MatchDefinitionCollection
            {
                Id = dto.Id == Guid.Empty ? Guid.NewGuid() : dto.Id,
                ProjectId = dto.ProjectId,
                JobId = dto.JobId,
                Name = dto.Name,
                Definitions = new List<Domain.Entities.MatchDefinition>()
            };

            // Create a lookup for faster pair retrieval
            var pairLookup = CreatePairLookup(dataSourcePairs);

            // Create a lookup for definitions by pair ID and UI index
            var definitionLookup = new Dictionary<string, Domain.Entities.MatchDefinition>();

            // Process each mapped row definition with its index
            for (int defIndex = 0; defIndex < dto.Definitions.Count; defIndex++)
            {
                var defDto = dto.Definitions[defIndex];
                ProcessMappedRowDefinition(collection, defDto, pairLookup, definitionLookup, defIndex);
            }

            return collection;
        }

        private Dictionary<string, MatchingDataSourcePair> CreatePairLookup(List<MatchingDataSourcePair> pairs)
        {
            var lookup = new Dictionary<string, MatchingDataSourcePair>();

            foreach (var pair in pairs)
            {
                // Create keys for both source orderings, including self-reference case
                var key1 = $"{pair.DataSourceA}:{pair.DataSourceB}";
                var key2 = $"{pair.DataSourceB}:{pair.DataSourceA}";

                lookup[key1] = pair;

                // Only add the reverse key if it's different (avoid duplicate keys)
                if (key1 != key2)
                {
                    lookup[key2] = pair;
                }
            }

            return lookup;
        }

        private void ProcessMappedRowDefinition(
            MatchDefinitionCollection collection,
            MatchDefinitionMappedRowDto defDto,
            Dictionary<string, MatchingDataSourcePair> pairLookup,
            Dictionary<string, Domain.Entities.MatchDefinition> definitionLookup,
            int defIndex)
        {
            foreach (var critDto in defDto.Criteria)
            {
                ProcessMappedRowCriterion(collection, defDto, critDto, pairLookup, definitionLookup, defIndex);
            }
        }

        private void ProcessMappedRowCriterion(
            MatchDefinitionCollection collection,
            MatchDefinitionMappedRowDto defDto,
            MatchCriterionMappedRowDto critDto,
            Dictionary<string, MatchingDataSourcePair> pairLookup,
            Dictionary<string, Domain.Entities.MatchDefinition> definitionLookup,
            int defIndex)
        {
            // Get all data sources used in this criterion
            var dataSourceIds = critDto.MappedRow.FieldsByDataSource.Values
                .Select(f => f.DataSourceId)
                .Distinct()
                .ToList();

            // Process each possible pair, including self-reference pairs
            for (int i = 0; i < dataSourceIds.Count; i++)
            {
                for (int j = i; j < dataSourceIds.Count; j++) // Changed from i+1 to i to include self-reference
                {
                    var dsIdA = dataSourceIds[i];
                    var dsIdB = dataSourceIds[j];

                    // Skip if we can't form a valid pair
                    if (!IsValidPair(dsIdA, dsIdB, pairLookup))
                        continue;

                    // Find or create a definition for this pair and UI index
                    var definition = GetOrCreateDefinition(collection, defDto, dsIdA, dsIdB, pairLookup, definitionLookup, defIndex);
                    if (definition == null)
                        continue;

                    // Create a criterion for this pair
                    var criterion = CreateCriterionForPair(critDto, dsIdA, dsIdB);

                    // Only add if there are field mappings
                    if (criterion.FieldMappings.Any())
                    {
                        definition.Criteria.Add(criterion);
                    }
                }
            }
        }

        private bool IsValidPair(Guid dsIdA, Guid dsIdB, Dictionary<string, MatchingDataSourcePair> pairLookup)
        {
            // Self-reference is valid if it exists in the pair lookup
            if (dsIdA.Equals(dsIdB))
            {
                var selfKey = $"{dsIdA}:{dsIdA}";
                return pairLookup.ContainsKey(selfKey);
            }

            // For different sources, check if pair exists
            var pairKey = $"{dsIdA}:{dsIdB}";
            return pairLookup.ContainsKey(pairKey);
        }

        private Domain.Entities.MatchDefinition GetOrCreateDefinition(
            MatchDefinitionCollection collection,
            MatchDefinitionMappedRowDto defDto,
            Guid dsIdA,
            Guid dsIdB,
            Dictionary<string, MatchingDataSourcePair> pairLookup,
            Dictionary<string, Domain.Entities.MatchDefinition> definitionLookup,
            int defIndex)
        {
            // Get the pair
            var pairKey = $"{dsIdA}:{dsIdB}";
            if (!pairLookup.TryGetValue(pairKey, out var pair))
                return null;

            // Find or create a definition for this pair and UI index
            var definitionKey = $"{pair.Id}:{defIndex}";
            if (!definitionLookup.TryGetValue(definitionKey, out var definition))
            {
                definition = new Domain.Entities.MatchDefinition
                {
                    Id = Guid.NewGuid(),
                    DataSourcePairId = pair.Id,
                    ProjectRunId = defDto.ProjectRunId,
                    UIDefinitionIndex = defIndex,
                    Criteria = new List<MatchCriteria>()
                };

                collection.Definitions.Add(definition);
                definitionLookup[definitionKey] = definition;
            }

            return definition;
        }

        private MatchCriteria CreateCriterionForPair(
            MatchCriterionMappedRowDto critDto,
            Guid dsIdA,
            Guid dsIdB)
        {
            var criterion = new MatchCriteria
            {
                Id = Guid.NewGuid(),
                MatchingType = critDto.MatchingType,
                DataType = critDto.DataType,
                Weight = critDto.Weight,
                Arguments = new Dictionary<ArgsValue, string>(),
                FieldMappings = new List<FieldMapping>()
            };

            // Copy arguments
            foreach (var arg in critDto.Arguments)
            {
                criterion.Arguments[arg.Key] = arg.Value;
            }

            // Add field mappings for the data sources in this pair
            foreach (var fieldPair in critDto.MappedRow.FieldsByDataSource)
            {
                var field = fieldPair.Value;

                // For self-reference pairs, include only that data source
                // For normal pairs, include both data sources
                if (dsIdA.Equals(dsIdB))
                {
                    if (field.DataSourceId == dsIdA)
                    {
                        criterion.FieldMappings.Add(CreateFieldMapping(field));
                    }
                }
                else if (field.DataSourceId == dsIdA || field.DataSourceId == dsIdB)
                {
                    criterion.FieldMappings.Add(CreateFieldMapping(field));
                }
            }

            return criterion;
        }

        private FieldMapping CreateFieldMapping(FieldDto field)
        {
            return new FieldMapping
            {
                Id = Guid.NewGuid(),
                DataSourceId = field.DataSourceId,
                DataSourceName = field.DataSourceName,
                FieldName = field.Name
            };
        }


        /// <summary>
        /// to reconstruct the original UI definitions
        /// </summary>
        /// <param name="collection">MatchDefinitionCollection domain model</param>
        /// <returns>MappedRowDto</returns>        
        public MatchDefinitionCollectionMappedRowDto ToMappedRowDto(MatchDefinitionCollection collection)
        {
            if (collection == null)
                return null;

            var dto = new MatchDefinitionCollectionMappedRowDto
            {
                Id = collection.Id,
                ProjectId = collection.ProjectId,
                JobId = collection.JobId,
                Name = collection.Name,
                Definitions = new List<MatchDefinitionMappedRowDto>()
            };

            // Group definitions by UIDefinitionIndex
            var uiDefinitionGroups = collection.Definitions
                .GroupBy(d => d.UIDefinitionIndex)
                .ToDictionary(g => g.Key, g => g.ToList());

            // Process each UI definition group
            foreach (var uiIndex in uiDefinitionGroups.Keys.OrderBy(k => k))
            {
                var definitions = uiDefinitionGroups[uiIndex];
                if (definitions.Count == 0)
                    continue;

                var uiDefinition = CreateUIDefinition(definitions);
                dto.Definitions.Add(uiDefinition);
            }

            return dto;
        }

        private MatchDefinitionMappedRowDto CreateUIDefinition(List<Domain.Entities.MatchDefinition> definitions)
        {
            var uiDefinition = new MatchDefinitionMappedRowDto
            {
                ProjectRunId = definitions.First().ProjectRunId,
                Criteria = new List<MatchCriterionMappedRowDto>()
            };

            // Instead of grouping criteria, reconstruct them by their logical position
            // We need to identify unique criteria by their semantic meaning across all pairs
            var criteriaTemplates = ExtractCriteriaTemplates(definitions);

            // Create a mapped criterion for each template
            foreach (var template in criteriaTemplates)
            {
                var mappedCriterion = CreateMappedCriterionFromTemplate(template, definitions);
                uiDefinition.Criteria.Add(mappedCriterion);
            }

            return uiDefinition;
        }

        private class CriteriaTemplate
        {
            public MatchingType MatchingType { get; set; }
            public CriteriaDataType DataType { get; set; }
            public double Weight { get; set; }
            public Dictionary<ArgsValue, string> Arguments { get; set; }
            public int TemplateIndex { get; set; } // To maintain order
            public Dictionary<string, string> FieldNamesByDataSource { get; set; } = new();
        }

        private List<CriteriaTemplate> ExtractCriteriaTemplates(List<Domain.Entities.MatchDefinition> definitions)
        {
            var templates = new List<CriteriaTemplate>();

            // Use the first definition as the template source
            var firstDefinition = definitions.First();

            for (int i = 0; i < firstDefinition.Criteria.Count; i++)
            {
                var criterion = firstDefinition.Criteria[i];
                var template = new CriteriaTemplate
                {
                    MatchingType = criterion.MatchingType,
                    DataType = criterion.DataType,
                    Weight = criterion.Weight,
                    Arguments = new Dictionary<ArgsValue, string>(criterion.Arguments),
                    TemplateIndex = i
                };

                // Collect all field names for this criterion position across all definitions
                CollectFieldNamesForTemplate(template, definitions, i);

                templates.Add(template);
            }

            return templates;
        }

        private void CollectFieldNamesForTemplate(CriteriaTemplate template, List<Domain.Entities.MatchDefinition> definitions, int criterionIndex)
        {
            foreach (var definition in definitions)
            {
                if (criterionIndex >= definition.Criteria.Count)
                    continue;

                var criterion = definition.Criteria[criterionIndex];

                // Add field mappings from this criterion
                foreach (var mapping in criterion.FieldMappings)
                {
                    if (!template.FieldNamesByDataSource.ContainsKey(mapping.DataSourceName))
                    {
                        template.FieldNamesByDataSource[mapping.DataSourceName] = mapping.FieldName;
                    }
                }
            }
        }

        private MatchCriterionMappedRowDto CreateMappedCriterionFromTemplate(CriteriaTemplate template, List<Domain.Entities.MatchDefinition> definitions)
        {
            var mappedCriterion = new MatchCriterionMappedRowDto
            {
                MatchingType = template.MatchingType,
                DataType = template.DataType,
                Weight = template.Weight,
                Arguments = new Dictionary<ArgsValue, string>(template.Arguments),
                MappedRow = new MappedFieldRowDto
                {
                    FieldsByDataSource = new Dictionary<string, FieldDto>()
                }
            };

            // Create field DTOs for each data source
            foreach (var fieldPair in template.FieldNamesByDataSource)
            {
                // Find the data source ID for this data source name
                var dataSourceId = FindDataSourceId(fieldPair.Key, definitions);

                if (dataSourceId != Guid.Empty)
                {
                    mappedCriterion.MappedRow.FieldsByDataSource[fieldPair.Key] = new FieldDto
                    {
                        DataSourceId = dataSourceId,
                        DataSourceName = fieldPair.Key,
                        Name = fieldPair.Value
                    };
                }
            }

            return mappedCriterion;
        }

        private Guid FindDataSourceId(string dataSourceName, List<Domain.Entities.MatchDefinition> definitions)
        {
            foreach (var definition in definitions)
            {
                foreach (var criterion in definition.Criteria)
                {
                    var mapping = criterion.FieldMappings.FirstOrDefault(fm => fm.DataSourceName == dataSourceName);
                    if (mapping != null)
                    {
                        return mapping.DataSourceId;
                    }
                }
            }
            return Guid.Empty;
        }

        #endregion


        #region Helper Methods

        /// <summary>
        /// Find a data source pair by ID
        /// </summary>
        private MatchingDataSourcePair GetDataSourcePairById(Guid pairId, MatchDefinitionCollection collection)
        {
            // Find the definition with this pair ID
            var definition = collection.Definitions.FirstOrDefault(d => d.DataSourcePairId == pairId);
            if (definition == null || !definition.Criteria.Any())
                return null;

            // Extract data source IDs from field mappings
            var dataSourceIds = definition.Criteria
                .SelectMany(c => c.FieldMappings)
                .Select(fm => fm.DataSourceId)
                .Distinct()
                .ToList();

            // If there's only one distinct data source ID, it's a self-reference pair
            if (dataSourceIds.Count == 1)
            {
                return new MatchingDataSourcePair(dataSourceIds[0], dataSourceIds[0]);
            }
            // If there are at least 2 distinct data sources, use the first two
            else if (dataSourceIds.Count >= 2)
            {
                return new MatchingDataSourcePair(dataSourceIds[0], dataSourceIds[1]);
            }

            // Not enough data sources found
            return null;
        }

        #endregion

    }
}
