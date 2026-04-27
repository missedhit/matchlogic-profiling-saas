using MatchLogic.Application.Common;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Application.Features;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.Repository;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Project.DataProfiling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MatchLogic.Infrastructure;
using Microsoft.Extensions.Logging;
using MatchLogic.Application.Features.MatchDefinition;

namespace MatchLogic.Application.UnitTests;

public class MatchConfigurationServiceTests
{
    private readonly IGenericRepository<MatchDefinitionCollection, Guid> _mockCollectionRepo;
    private readonly IGenericRepository<MatchingDataSourcePairs, Guid> _mockPairRepo;
    private readonly IGenericRepository<DataSource, Guid> _mockDataSourceRepo;
    private readonly IGenericRepository<MatchSettings, Guid> _mockMatchSettingsRepo;
    private readonly IMatchConfigurationService _service;

    public MatchConfigurationServiceTests()
    {
        var _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();
        var _logger = new NullLogger<DataProfilingCommand>();
        var _completionTracker = new CompletionTracker();

        IServiceCollection services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, _dbJobPath);



        var _serviceProvider = services.BuildServiceProvider();
        _mockCollectionRepo = _serviceProvider.GetService<IGenericRepository<MatchDefinitionCollection, Guid>>();
        _mockPairRepo = _serviceProvider.GetService<IGenericRepository<MatchingDataSourcePairs, Guid>>();
        _mockDataSourceRepo = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        _mockMatchSettingsRepo = _serviceProvider.GetService<IGenericRepository<MatchSettings, Guid>>();

        _service = new MatchConfigurationService(
            _mockCollectionRepo,
            _mockPairRepo,
            _mockDataSourceRepo,
            _mockMatchSettingsRepo);
    }
    [Fact]
    public async Task GetMappedRowConfiguration_ShouldReturnDto()
    {
        // Arrange
        var collectionId = Guid.NewGuid();
        var dataSourceIds = new[] { Guid.NewGuid(), Guid.NewGuid() };

        var collection = new MatchDefinitionCollection
        {
            Id = collectionId,
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Test Collection",
            Definitions = new List<MatchDefinition>
    {
        new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    Id = Guid.NewGuid(),
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.7,
                    Arguments = new Dictionary<ArgsValue, string>
                    {
                        [ArgsValue.Level] = "80"
                    },
                    FieldMappings = new List<FieldMapping>
                    {
                        new FieldMapping(dataSourceIds[0], "DS1", "Field1"),
                        new FieldMapping(dataSourceIds[1], "DS2", "Field1")
                    }
                }
            }
        }
    }
        };

        // Setup necessary data in repository
        await _mockCollectionRepo.InsertAsync(collection, Constants.Collections.MatchDefinitionCollection);

        // Setup data sources
        var dataSources = new List<DataSource>
{
    new DataSource { Id = dataSourceIds[0], Name = "DS1" },
    new DataSource { Id = dataSourceIds[1], Name = "DS2" }
};

        foreach (var ds in dataSources)
        {
            await _mockDataSourceRepo.InsertAsync(ds, Constants.Collections.DataSources);
        }

        // Act
        var result = await _service.GetMappedRowConfigurationAsync(collectionId);

        // Assert
        Assert.Equal(collectionId, result.Id);
        Assert.Equal(collection.Name, result.Name);
        Assert.Equal(collection.ProjectId, result.ProjectId);
        Assert.Equal(collection.JobId, result.JobId);
        Assert.Single(result.Definitions);
        Assert.Single(result.Definitions[0].Criteria);
        Assert.Equal(2, result.Definitions[0].Criteria[0].MappedRow.FieldsByDataSource.Count);
        Assert.True(result.Definitions[0].Criteria[0].MappedRow.FieldsByDataSource.ContainsKey("DS1"));
        Assert.True(result.Definitions[0].Criteria[0].MappedRow.FieldsByDataSource.ContainsKey("DS2"));
    }

    [Fact]
    public async Task SaveFieldListConfiguration_ShouldCreateMultipleDefinitionsAndCriteria()
    {
        // Arrange
        var dataSourceIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var pairIds = new[] { Guid.NewGuid(), Guid.NewGuid() };

        var fieldListDto = new MatchDefinitionCollectionFieldListDto
        {
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Complex Collection",
            Definitions = new List<MatchDefinitionFieldListDto>
    {
        // First definition with two criteria
        new MatchDefinitionFieldListDto
        {
            DataSourcePairId = pairIds[0],
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionFieldListDto>
            {
                // First criterion - Fuzzy text matching
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.6,
                    Arguments = new Dictionary<ArgsValue, string>
                    {
                        [ArgsValue.Level] = "75"
                    },
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSourceIds[0], DataSourceName = "DS1", Name = "Name" },
                        new FieldDto { DataSourceId = dataSourceIds[1], DataSourceName = "DS2", Name = "FullName" }
                    }
                },
                // Second criterion - Exact number matching
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Number,
                    Weight = 0.4,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSourceIds[0], DataSourceName = "DS1", Name = "ID" },
                        new FieldDto { DataSourceId = dataSourceIds[1], DataSourceName = "DS2", Name = "CustomerID" }
                    }
                }
            }
        },
        // Second definition with one criterion
        new MatchDefinitionFieldListDto
        {
            DataSourcePairId = pairIds[1],
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionFieldListDto>
            {
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 1.0,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSourceIds[0], DataSourceName = "DS1", Name = "Email" },
                        new FieldDto { DataSourceId = dataSourceIds[2], DataSourceName = "DS3", Name = "EmailAddress" }
                    }
                }
            }
        }
    }
        };

        // Act
        var result = await _service.SaveFieldListConfigurationAsync(fieldListDto);

        // Retrieve the saved collection to verify its contents
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(result, Constants.Collections.MatchDefinitionCollection);

        // Assert
        Assert.NotEqual(Guid.Empty, result);
        Assert.Equal(2, savedCollection.Definitions.Count);
        Assert.Equal(2, savedCollection.Definitions[0].Criteria.Count);
        Assert.Single(savedCollection.Definitions[1].Criteria);

        // Verify first definition
        var def1 = savedCollection.Definitions.First(d => d.DataSourcePairId == pairIds[0]);
        Assert.Equal(pairIds[0], def1.DataSourcePairId);
        Assert.Equal(2, def1.Criteria.Count);

        // Verify criteria in first definition
        var fuzzyTextCriteria = def1.Criteria.First(c => c.MatchingType == MatchingType.Fuzzy);
        Assert.Equal(CriteriaDataType.Text, fuzzyTextCriteria.DataType);
        Assert.Equal(0.6, fuzzyTextCriteria.Weight);
        Assert.Equal("75", fuzzyTextCriteria.Arguments[ArgsValue.Level]);
        Assert.Equal(2, fuzzyTextCriteria.FieldMappings.Count);

        var exactNumCriteria = def1.Criteria.First(c => c.MatchingType == MatchingType.Exact);
        Assert.Equal(CriteriaDataType.Number, exactNumCriteria.DataType);
        Assert.Equal(0.4, exactNumCriteria.Weight);
        Assert.Equal(2, exactNumCriteria.FieldMappings.Count);

        // Verify second definition
        var def2 = savedCollection.Definitions.First(d => d.DataSourcePairId == pairIds[1]);
        Assert.Equal(pairIds[1], def2.DataSourcePairId);
        Assert.Single(def2.Criteria);

        // Verify criteria in second definition
        var soundexCriteria = def2.Criteria.First();
        Assert.Equal(MatchingType.Fuzzy, soundexCriteria.MatchingType);
        Assert.Equal(CriteriaDataType.Text, soundexCriteria.DataType);
        Assert.Equal(1.0, soundexCriteria.Weight);
        Assert.Equal(2, soundexCriteria.FieldMappings.Count);
    }

    [Fact]
    public async Task SaveFieldListConfiguration_WithSelfReferencePair_ShouldWorkCorrectly()
    {
        // Arrange
        // 1. Create the single data source that will reference itself
        var dataSourceId = Guid.NewGuid();
        var dataSourceName = "DS1";
        var projectId = Guid.NewGuid();

        // Add data source to repository
        await _mockDataSourceRepo.InsertAsync(
            new DataSource { Id = dataSourceId, Name = dataSourceName },
            Constants.Collections.DataSources);

        // Create data source pairs collection in repository with the project ID
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };
        // Add the self-reference pair
        pairsCollection.Add(dataSourceId, dataSourceId);
        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Get a pair ID for testing
        var selfReferencePairId = Guid.NewGuid();

        // 2. Create field list DTO with only self-reference (DS1-DS1)
        var fieldListDto = new MatchDefinitionCollectionFieldListDto
        {
            ProjectId = projectId,  // Use the same project ID
            JobId = Guid.NewGuid(),
            Name = "DS1-DS1 Self-Reference Field List Test",
            Definitions = new List<MatchDefinitionFieldListDto>
    {
        // Definition with only self-reference criteria
        new MatchDefinitionFieldListDto
        {
            DataSourcePairId = selfReferencePairId,
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionFieldListDto>
            {
                // First self-reference criterion for name matching
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.8,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "95" },
                    Fields = new List<FieldDto>
                    {
                        new FieldDto
                        {
                            DataSourceId = dataSourceId,
                            DataSourceName = dataSourceName,
                            Name = "Name"
                        }
                    }
                },
                // Second self-reference criterion for ID matching
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 1.0,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    Fields = new List<FieldDto>
                    {
                        new FieldDto
                        {
                            DataSourceId = dataSourceId,
                            DataSourceName = dataSourceName,
                            Name = "ID"
                        }
                    }
                }
            }
        }
    }
        };

        // Act - Save the field list configuration
        var savedId = await _service.SaveFieldListConfigurationAsync(fieldListDto);

        // Assert - Verify the saved collection
        Assert.NotEqual(Guid.Empty, savedId);

        // Retrieve the saved collection
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(savedId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(savedCollection);
        Assert.Equal("DS1-DS1 Self-Reference Field List Test", savedCollection.Name);
        Assert.Equal(projectId, savedCollection.ProjectId);
        Assert.Equal(fieldListDto.JobId, savedCollection.JobId);

        // Verify pairs collection exists for the project
        var pairsFromRepo = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pairsFromRepo);
        Assert.Equal(projectId, pairsFromRepo.ProjectId);
        Assert.True(pairsFromRepo.Contains(dataSourceId, dataSourceId));

        // Verify we have a single definition with the self-reference pair ID
        Assert.Single(savedCollection.Definitions);
        var definition = savedCollection.Definitions.Single();
        Assert.Equal(selfReferencePairId, definition.DataSourcePairId);

        // Verify criteria
        Assert.Equal(2, definition.Criteria.Count);

        // Find the fuzzy name criterion
        var nameCriterion = definition.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.NotNull(nameCriterion);
        Assert.Equal(0.8, nameCriterion.Weight);
        Assert.Equal("95", nameCriterion.Arguments[ArgsValue.Level]);

        // Verify it has exactly 1 field mapping (to itself)
        Assert.Single(nameCriterion.FieldMappings);
        Assert.Equal(dataSourceId, nameCriterion.FieldMappings[0].DataSourceId);
        Assert.Equal("Name", nameCriterion.FieldMappings[0].FieldName);

        // Find the exact ID criterion
        var idCriterion = definition.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.NotNull(idCriterion);
        Assert.Equal(1.0, idCriterion.Weight);

        // Verify it has exactly 1 field mapping
        Assert.Single(idCriterion.FieldMappings);
        Assert.Equal(dataSourceId, idCriterion.FieldMappings[0].DataSourceId);
        Assert.Equal("ID", idCriterion.FieldMappings[0].FieldName);

        // Act - Convert to mapped row format 
        var convertedDto = await _service.GetMappedRowConfigurationAsync(savedId);

        // Assert - Verify converted DTO
        Assert.Equal(savedId, convertedDto.Id);
        Assert.Equal("DS1-DS1 Self-Reference Field List Test", convertedDto.Name);
        Assert.Equal(projectId, convertedDto.ProjectId);
        Assert.Equal(fieldListDto.JobId, convertedDto.JobId);

        // Should have 1 definition in mapped row format
        Assert.Single(convertedDto.Definitions);

        var convertedDef = convertedDto.Definitions.First();

        // Should have 2 criteria
        Assert.Equal(2, convertedDef.Criteria.Count);

        // Verify the name criterion in mapped row format
        var convertedNameCriterion = convertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.8, convertedNameCriterion.Weight);
        Assert.Equal("95", convertedNameCriterion.Arguments[ArgsValue.Level]);

        // Verify it has the mapping to DS1
        Assert.Single(convertedNameCriterion.MappedRow.FieldsByDataSource);
        Assert.True(convertedNameCriterion.MappedRow.FieldsByDataSource.ContainsKey(dataSourceName));
        Assert.Equal("Name", convertedNameCriterion.MappedRow.FieldsByDataSource[dataSourceName].Name);

        // Verify the ID criterion in mapped row format
        var convertedIdCriterion = convertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.Equal(1.0, convertedIdCriterion.Weight);

        // Verify it has the mapping to DS1
        Assert.Single(convertedIdCriterion.MappedRow.FieldsByDataSource);
        Assert.True(convertedIdCriterion.MappedRow.FieldsByDataSource.ContainsKey(dataSourceName));
        Assert.Equal("ID", convertedIdCriterion.MappedRow.FieldsByDataSource[dataSourceName].Name);
    }

    [Fact]
    public async Task SaveFieldListConfiguration_WithSelfReferenceAndMultiplePairs_ShouldWorkCorrectly()
    {
        // Arrange
        // 1. Create data sources - including one that will reference itself
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid(),
            ["DS3"] = Guid.NewGuid()
        };
        var projectId = Guid.NewGuid();

        // Add data sources to repository
        foreach (var ds in dataSources)
        {
            await _mockDataSourceRepo.InsertAsync(
                new DataSource { Id = ds.Value, Name = ds.Key },
                Constants.Collections.DataSources);
        }

        // Create a data source pairs collection for the project
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add all pairs to the collection
        pairsCollection.Add(dataSources["DS1"], dataSources["DS1"]);
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Get pairs IDs for the test
        var ds1Ds1PairId = Guid.NewGuid();
        var ds1Ds2PairId = Guid.NewGuid();
        var ds2Ds3PairId = Guid.NewGuid();

        // 2. Create field list DTO with self-reference and other pairs
        var fieldListDto = new MatchDefinitionCollectionFieldListDto
        {
            ProjectId = projectId,
            JobId = Guid.NewGuid(),
            Name = "Multiple Pairs With Self-Reference Test",
            Definitions = new List<MatchDefinitionFieldListDto>
    {
        // First definition - DS1-DS1 (self-reference) for deduplication
        new MatchDefinitionFieldListDto
        {
            DataSourcePairId = ds1Ds1PairId,
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionFieldListDto>
            {
                // Name fuzzy matching criterion
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.9,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "90" },
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "FullName" }
                    }
                },
                // Email exact matching criterion
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 1.0,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Email" }
                    }
                }
            }
        },
        // Second definition - DS1-DS2 for matching across systems
        new MatchDefinitionFieldListDto
        {
            DataSourcePairId = ds1Ds2PairId,
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionFieldListDto>
            {
                // Name fuzzy matching criterion
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.7,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "85" },
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "FullName" },
                        new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "CustomerName" }
                    }
                },
                // ID exact matching criterion
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Number,
                    Weight = 0.6,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "ID" },
                        new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "CustomerID" }
                    }
                }
            }
        },
        // Third definition - DS2-DS3 for additional matching
        new MatchDefinitionFieldListDto
        {
            DataSourcePairId = ds2Ds3PairId,
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionFieldListDto>
            {
                // Email exact matching criterion
                new MatchCriterionFieldListDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.8,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    Fields = new List<FieldDto>
                    {
                        new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "Email" },
                        new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "EmailAddress" }
                    }
                }
            }
        }
    }
        };

        // Act - Save the field list configuration
        var savedId = await _service.SaveFieldListConfigurationAsync(fieldListDto);

        // Assert - Verify the saved collection
        Assert.NotEqual(Guid.Empty, savedId);

        // Retrieve the saved collection
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(savedId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(savedCollection);
        Assert.Equal("Multiple Pairs With Self-Reference Test", savedCollection.Name);
        Assert.Equal(projectId, savedCollection.ProjectId);
        Assert.Equal(fieldListDto.JobId, savedCollection.JobId);

        // We should have 3 definitions (one for each pair)
        Assert.Equal(3, savedCollection.Definitions.Count);

        // Get pairs collection from repository
        var pairsFromRepo = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pairsFromRepo);
        Assert.Equal(projectId, pairsFromRepo.ProjectId);
        Assert.Equal(3, pairsFromRepo.Count);

        // Verify the pairs collection contains all the expected pairs
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS1"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS2"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS2"], dataSources["DS3"]));

        // Verify the self-reference definition (DS1-DS1)
        var selfRefDef = savedCollection.Definitions.FirstOrDefault(d => d.DataSourcePairId == ds1Ds1PairId);
        Assert.NotNull(selfRefDef);
        Assert.Equal(2, selfRefDef.Criteria.Count);

        // Check the name criterion in self-reference definition
        var selfRefNameCriterion = selfRefDef.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);
        Assert.NotNull(selfRefNameCriterion);
        Assert.Equal(0.9, selfRefNameCriterion.Weight);
        Assert.Equal("90", selfRefNameCriterion.Arguments[ArgsValue.Level]);

        // Both field mappings should point to the same data source
        Assert.Single(selfRefNameCriterion.FieldMappings);
        Assert.Equal(dataSources["DS1"], selfRefNameCriterion.FieldMappings[0].DataSourceId);
        Assert.Equal("FullName", selfRefNameCriterion.FieldMappings[0].FieldName);

        // Verify the DS1-DS2 definition
        var ds1Ds2Def = savedCollection.Definitions.FirstOrDefault(d => d.DataSourcePairId == ds1Ds2PairId);
        Assert.NotNull(ds1Ds2Def);
        Assert.Equal(2, ds1Ds2Def.Criteria.Count);

        // Check the name criterion in DS1-DS2 definition
        var ds1Ds2NameCriterion = ds1Ds2Def.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);
        Assert.NotNull(ds1Ds2NameCriterion);
        Assert.Equal(0.7, ds1Ds2NameCriterion.Weight);
        Assert.Equal("85", ds1Ds2NameCriterion.Arguments[ArgsValue.Level]);

        // Field mappings should be to different data sources
        Assert.Equal(2, ds1Ds2NameCriterion.FieldMappings.Count);
        Assert.Contains(ds1Ds2NameCriterion.FieldMappings, fm =>
            fm.DataSourceId == dataSources["DS1"] && fm.FieldName == "FullName");
        Assert.Contains(ds1Ds2NameCriterion.FieldMappings, fm =>
            fm.DataSourceId == dataSources["DS2"] && fm.FieldName == "CustomerName");

        // Verify the DS2-DS3 definition
        var ds2Ds3Def = savedCollection.Definitions.FirstOrDefault(d => d.DataSourcePairId == ds2Ds3PairId);
        Assert.NotNull(ds2Ds3Def);
        Assert.Single(ds2Ds3Def.Criteria);

        // Check the email criterion
        var emailCriterion = ds2Ds3Def.Criteria.First();
        Assert.Equal(MatchingType.Exact, emailCriterion.MatchingType);
        Assert.Equal(CriteriaDataType.Text, emailCriterion.DataType);
        Assert.Equal(0.8, emailCriterion.Weight);

        // Field mappings should be to different data sources
        Assert.Equal(2, emailCriterion.FieldMappings.Count);
        Assert.Contains(emailCriterion.FieldMappings, fm =>
            fm.DataSourceId == dataSources["DS2"] && fm.FieldName == "Email");
        Assert.Contains(emailCriterion.FieldMappings, fm =>
            fm.DataSourceId == dataSources["DS3"] && fm.FieldName == "EmailAddress");

        // Act - Convert to field list format
        var convertedDto = await _service.GetFieldListConfigurationAsync(savedId);

        // Assert - Verify converted DTO
        Assert.Equal(savedId, convertedDto.Id);
        Assert.Equal("Multiple Pairs With Self-Reference Test", convertedDto.Name);
        Assert.Equal(projectId, convertedDto.ProjectId);
        Assert.Equal(fieldListDto.JobId, convertedDto.JobId);

        // Should have 3 definitions in field list format
        Assert.Equal(3, convertedDto.Definitions.Count);

        // Find the definitions by criteria
        var selfRefMappedDef = convertedDto.Definitions.FirstOrDefault(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Fuzzy &&
                              c.DataType == CriteriaDataType.Text &&
                              c.Weight == 0.9));

        var ds1Ds2MappedDef = convertedDto.Definitions.FirstOrDefault(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Fuzzy &&
                              c.DataType == CriteriaDataType.Text &&
                              c.Weight == 0.7));

        var ds2Ds3MappedDef = convertedDto.Definitions.FirstOrDefault(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Exact &&
                              c.DataType == CriteriaDataType.Text &&
                              c.Weight == 0.8));

        Assert.NotNull(selfRefMappedDef);
        Assert.NotNull(ds1Ds2MappedDef);
        Assert.NotNull(ds2Ds3MappedDef);

        // Check specific criteria in self-reference mapped definition
        var selfRefNameMappedCriterion = selfRefMappedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.9, selfRefNameMappedCriterion.Weight);
        Assert.Equal("90", selfRefNameMappedCriterion.Arguments[ArgsValue.Level]);

        // In mapped row format, self-reference should have single data source
        Assert.Single(selfRefNameMappedCriterion.Fields);
        Assert.True(selfRefNameMappedCriterion.Fields?.Any(x => x.DataSourceName == "DS1"));
        Assert.Equal("FullName", selfRefNameMappedCriterion.Fields?.SingleOrDefault(x => x.DataSourceName == "DS1")?.Name);
    }
    [Fact]
    public async Task DeleteConfiguration_ShouldRemoveCollectionAndRelatedEntities()
    {
        // Arrange
        var collectionId = Guid.NewGuid();
        var collection = new MatchDefinitionCollection
        {
            Id = collectionId,
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Collection To Delete",
            Definitions = new List<MatchDefinition>
    {
        new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    Id = Guid.NewGuid(),
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 1.0,
                    FieldMappings = new List<FieldMapping>
                    {
                        new FieldMapping(Guid.NewGuid(), "DS1", "Field1"),
                        new FieldMapping(Guid.NewGuid(), "DS2", "Field1")
                    }
                }
            }
        }
    }
        };

        // Add to repository
        await _mockCollectionRepo.InsertAsync(collection, Constants.Collections.MatchDefinitionCollection);

        // Verify it exists
        var beforeDelete = await _mockCollectionRepo.GetByIdAsync(collectionId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(beforeDelete);

        // Act
        await _service.DeleteCollectionAsync(collectionId);

        // Assert
        // Try to get the deleted collection
        var v = await _mockCollectionRepo.GetByIdAsync(collectionId, Constants.Collections.MatchDefinitionCollection);
        Assert.Null(v);
    }

    [Fact]
    public async Task CreateDataSourcePair_ShouldCreatePairIfNotExists()
    {
        // Arrange
        var leftDataSourceId = Guid.NewGuid();
        var rightDataSourceId = Guid.NewGuid();
        var projectId = Guid.NewGuid();

        // Act
        var pairId = await _service.CreateDataSourcePairAsync(projectId, leftDataSourceId, rightDataSourceId);

        // Assert
        Assert.NotNull(pairId);

        // Verify pair was created in repository
        var pair = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pair);
        Assert.Equal(leftDataSourceId, pair[0].DataSourceA);
        Assert.Equal(rightDataSourceId, pair[0].DataSourceB);
    }

    [Fact]
    public async Task SaveMappedRowConfigurationAsync_WithUIDefinitionIndex_ShouldPreserveStructure_Test()
    {
        // Arrange
        // 1. Create 3 data sources
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid(),
            ["DS3"] = Guid.NewGuid()
        };
        var projectId = Guid.NewGuid();

        // Add data sources to repository
        foreach (var ds in dataSources)
        {
            await _mockDataSourceRepo.InsertAsync(
                new DataSource { Id = ds.Value, Name = ds.Key },
                Constants.Collections.DataSources);
        }

        // 2. Create mapped row DTO with all 3 data sources in each criterion
        // This time we'll have two distinct UI definitions to test the UIDefinitionIndex
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = Guid.NewGuid(),
            Name = "Multi-Definition Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
    {
        // First UI definition (index 0) - Name and ID matching
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // Text criterion - Name matching
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    //DataType = CriteriaDataType.Text,
                    //Weight = 0.6,
                    //Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "80" },
                     DataType = CriteriaDataType.Text,
                     Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "3" },
                     Weight=0.5,
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "CustomerName" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "FullName" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "PersonName" }
                        }
                    }
                },
                // Number criterion - ID matching
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Number,
                    Weight = 0.4,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "ID" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "CustomerID" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "PersonID" }
                        }
                    }
                }
            }
        },
        // Second UI definition (index 1) - Address and Email matching
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // Address criterion
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.5,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "3" },
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Address" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "StreetAddress" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "Location" }
                        }
                    }
                },
                // Email criterion
                new MatchCriterionMappedRowDto
                {
                    //MatchingType = MatchingType.Exact,
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                     Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "3" },
                    Weight = 0.5,
                    //Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Email" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "EmailAddress" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "ContactEmail" }
                        }
                    }
                }
            }
        }
    }
        };

        // Create pairs collection with the project ID
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add all pairs to the collection
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        pairsCollection.Add(dataSources["DS1"], dataSources["DS3"]);
        pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Act - Save the configuration
        var savedId = await _service.SaveMappedRowConfigurationAsync(mappedRowDto);

        // Assert - Verify the saved collection
        Assert.NotEqual(Guid.Empty, savedId);

        // Retrieve the saved collection
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(savedId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(savedCollection);
        Assert.Equal("Multi-Definition Test", savedCollection.Name);
        Assert.Equal(projectId, savedCollection.ProjectId);
        Assert.Equal(mappedRowDto.JobId, savedCollection.JobId);

        // With 3 data sources, we should have 3 pairs
        // And with 2 UI definitions, we should have 6 db definitions (3 pairs × 2 UI definitions)
        Assert.Equal(6, savedCollection.Definitions.Count);

        // Get pairs collection from repository
        var pairsFromRepo = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pairsFromRepo);
        Assert.Equal(projectId, pairsFromRepo.ProjectId);
        Assert.Equal(3, pairsFromRepo.Count);

        // Verify the pairs collection contains all expected pairs
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS2"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS3"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS2"], dataSources["DS3"]));

        // We need to use the pair IDs for checking the definitions
        // For this test we'll simulate the pair IDs by getting them from the saved definitions
        var definitions = savedCollection.Definitions.ToList();
        var pairIds = definitions.Select(d => d.DataSourcePairId).Distinct().ToList();
        Assert.Equal(3, pairIds.Count); // Should have 3 unique pair IDs

        // Group definitions by UIDefinitionIndex
        var defsByUiIndex = savedCollection.Definitions.GroupBy(d => d.UIDefinitionIndex).ToList();
        Assert.Equal(2, defsByUiIndex.Count); // Should have 2 groups (one for each UI definition)

        // Verify first UI definition group (index 0)
        var firstUiGroup = defsByUiIndex.First(g => g.Key == 0).ToList();
        Assert.Equal(3, firstUiGroup.Count); // One per pair

        // Verify second UI definition group (index 1)
        var secondUiGroup = defsByUiIndex.First(g => g.Key == 1).ToList();
        Assert.Equal(3, secondUiGroup.Count); // One per pair

        // For each definition in the first group, get the pair and check field mappings
        foreach (var def in firstUiGroup)
        {
            Assert.Equal(2, def.Criteria.Count);

            // Get field mappings for name criterion
            var nameCriterion = def.Criteria.First(c => c.MatchingType == MatchingType.Fuzzy);
            Assert.Equal(2, nameCriterion.FieldMappings.Count);

            // Check the field mapping contains data sources from a valid pair
            var dataSourceIds = nameCriterion.FieldMappings.Select(fm => fm.DataSourceId).ToList();
            Assert.Equal(2, dataSourceIds.Count);

            // Check if this is a valid pair in our collection
            bool isPairValid =
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS2"])) ||
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS3"])) ||
                (dataSourceIds.Contains(dataSources["DS2"]) && dataSourceIds.Contains(dataSources["DS3"]));

            Assert.True(isPairValid, "The field mappings should correspond to a valid data source pair");
        }

        // For each definition in the second group, verify field mappings
        foreach (var def in secondUiGroup)
        {
            Assert.Equal(2, def.Criteria.Count);

            // Get field mappings for address criterion
            var addressCriterion = def.Criteria.First(c => c.MatchingType == MatchingType.Fuzzy);
            Assert.Equal(2, addressCriterion.FieldMappings.Count);

            // Check the field mapping contains data sources from a valid pair
            var dataSourceIds = addressCriterion.FieldMappings.Select(fm => fm.DataSourceId).ToList();
            Assert.Equal(2, dataSourceIds.Count);

            // Check if this is a valid pair in our collection
            bool isPairValid =
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS2"])) ||
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS3"])) ||
                (dataSourceIds.Contains(dataSources["DS2"]) && dataSourceIds.Contains(dataSources["DS3"]));

            Assert.True(isPairValid, "The field mappings should correspond to a valid data source pair");
        }

        // Act - Convert back to mapped row format
        var convertedDto = await _service.GetMappedRowConfigurationAsync(savedId);

        // Assert - Verify the converted DTO
        Assert.Equal(savedId, convertedDto.Id);
        Assert.Equal("Multi-Definition Test", convertedDto.Name);
        Assert.Equal(projectId, convertedDto.ProjectId);
        Assert.Equal(mappedRowDto.JobId, convertedDto.JobId);

        // Should have 2 UI definitions like the original input
        Assert.Equal(2, convertedDto.Definitions.Count);

        // Find the first converted UI definition (with Name and ID matching)
        var firstConvertedDef = convertedDto.Definitions.First(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text));

        // Should have 2 criteria (Name and ID)
        Assert.Equal(2, firstConvertedDef.Criteria.Count);

        // Each criterion should have all 3 data sources
        foreach (var criterion in firstConvertedDef.Criteria)
        {
            Assert.Equal(3, criterion.MappedRow.FieldsByDataSource.Count);
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS2"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS3"));
        }

        // Find the second converted UI definition (with Address and Email matching)
        var secondConvertedDef = convertedDto.Definitions.Last(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Fuzzy));

        // Should have 2 criteria (Address and Email)
        Assert.Equal(2, secondConvertedDef.Criteria.Count);

        // Each criterion should have all 3 data sources
        foreach (var criterion in secondConvertedDef.Criteria)
        {
            Assert.Equal(3, criterion.MappedRow.FieldsByDataSource.Count);
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS2"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS3"));
        }

        // Verify specific criteria in first UI definition

        // Name matching criterion
        var nameMatchingCriterion = firstConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.5, nameMatchingCriterion.Weight);
        Assert.Equal("3", nameMatchingCriterion.Arguments[ArgsValue.Level]);

        // Verify field names match the original input
        Assert.Equal("CustomerName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("FullName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("PersonName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);

        // ID matching criterion
        var idMatchingCriterion = firstConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Number);

        Assert.Equal(0.4, idMatchingCriterion.Weight);

        // Verify field names match the original input
        Assert.Equal("ID", idMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("CustomerID", idMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("PersonID", idMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);

        // Verify specific criteria in second UI definition

        // Address matching criterion
        var addressMatchingCriterion = secondConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.5, addressMatchingCriterion.Weight);
        Assert.Equal("3", addressMatchingCriterion.Arguments[ArgsValue.Level]);

        // Verify field names match the original input
        Assert.Equal("Address", addressMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("StreetAddress", addressMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("Location", addressMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);

        // Email matching criterion
        var emailMatchingCriterion = secondConvertedDef.Criteria.Last(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.5, emailMatchingCriterion.Weight);

        // Verify field names match the original input
        Assert.Equal("Email", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("EmailAddress", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("ContactEmail", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);
    }
    [Fact]
    public async Task SaveMappedRowConfigurationAsync_WithUIDefinitionIndex_ShouldPreserveStructure()
    {
        // Arrange
        // 1. Create 3 data sources
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid(),
            ["DS3"] = Guid.NewGuid()
        };
        var projectId = Guid.NewGuid();

        // Add data sources to repository
        foreach (var ds in dataSources)
        {
            await _mockDataSourceRepo.InsertAsync(
                new DataSource { Id = ds.Value, Name = ds.Key },
                Constants.Collections.DataSources);
        }

        // 2. Create mapped row DTO with all 3 data sources in each criterion
        // This time we'll have two distinct UI definitions to test the UIDefinitionIndex
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = Guid.NewGuid(),
            Name = "Multi-Definition Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
    {
        // First UI definition (index 0) - Name and ID matching
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // Text criterion - Name matching
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.6,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "80" },
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "CustomerName" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "FullName" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "PersonName" }
                        }
                    }
                },
                // Number criterion - ID matching
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Number,
                    Weight = 0.4,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "ID" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "CustomerID" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "PersonID" }
                        }
                    }
                }
            }
        },
        // Second UI definition (index 1) - Address and Email matching
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // Address criterion
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.5,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "3" },
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Address" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "StreetAddress" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "Location" }
                        }
                    }
                },
                // Email criterion
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Exact,
                    //MatchingType = MatchingType.Fuzzy,
                    //DataType = CriteriaDataType.Text,
                     //Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "4" },
                    Weight = 0.5,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Email" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "EmailAddress" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "ContactEmail" }
                        }
                    }
                }
            }
        }
    }
        };

        // Create pairs collection with the project ID
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add all pairs to the collection
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        pairsCollection.Add(dataSources["DS1"], dataSources["DS3"]);
        pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Act - Save the configuration
        var savedId = await _service.SaveMappedRowConfigurationAsync(mappedRowDto);

        // Assert - Verify the saved collection
        Assert.NotEqual(Guid.Empty, savedId);

        // Retrieve the saved collection
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(savedId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(savedCollection);
        Assert.Equal("Multi-Definition Test", savedCollection.Name);
        Assert.Equal(projectId, savedCollection.ProjectId);
        Assert.Equal(mappedRowDto.JobId, savedCollection.JobId);

        // With 3 data sources, we should have 3 pairs
        // And with 2 UI definitions, we should have 6 db definitions (3 pairs × 2 UI definitions)
        Assert.Equal(6, savedCollection.Definitions.Count);

        // Get pairs collection from repository
        var pairsFromRepo = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pairsFromRepo);
        Assert.Equal(projectId, pairsFromRepo.ProjectId);
        Assert.Equal(3, pairsFromRepo.Count);

        // Verify the pairs collection contains all expected pairs
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS2"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS3"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS2"], dataSources["DS3"]));

        // We need to use the pair IDs for checking the definitions
        // For this test we'll simulate the pair IDs by getting them from the saved definitions
        var definitions = savedCollection.Definitions.ToList();
        var pairIds = definitions.Select(d => d.DataSourcePairId).Distinct().ToList();
        Assert.Equal(3, pairIds.Count); // Should have 3 unique pair IDs

        // Group definitions by UIDefinitionIndex
        var defsByUiIndex = savedCollection.Definitions.GroupBy(d => d.UIDefinitionIndex).ToList();
        Assert.Equal(2, defsByUiIndex.Count); // Should have 2 groups (one for each UI definition)

        // Verify first UI definition group (index 0)
        var firstUiGroup = defsByUiIndex.First(g => g.Key == 0).ToList();
        Assert.Equal(3, firstUiGroup.Count); // One per pair

        // Verify second UI definition group (index 1)
        var secondUiGroup = defsByUiIndex.First(g => g.Key == 1).ToList();
        Assert.Equal(3, secondUiGroup.Count); // One per pair

        // For each definition in the first group, get the pair and check field mappings
        foreach (var def in firstUiGroup)
        {
            Assert.Equal(2, def.Criteria.Count);

            // Get field mappings for name criterion
            var nameCriterion = def.Criteria.First(c => c.MatchingType == MatchingType.Fuzzy);
            Assert.Equal(2, nameCriterion.FieldMappings.Count);

            // Check the field mapping contains data sources from a valid pair
            var dataSourceIds = nameCriterion.FieldMappings.Select(fm => fm.DataSourceId).ToList();
            Assert.Equal(2, dataSourceIds.Count);

            // Check if this is a valid pair in our collection
            bool isPairValid =
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS2"])) ||
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS3"])) ||
                (dataSourceIds.Contains(dataSources["DS2"]) && dataSourceIds.Contains(dataSources["DS3"]));

            Assert.True(isPairValid, "The field mappings should correspond to a valid data source pair");
        }

        // For each definition in the second group, verify field mappings
        foreach (var def in secondUiGroup)
        {
            Assert.Equal(2, def.Criteria.Count);

            // Get field mappings for address criterion
            var addressCriterion = def.Criteria.First(c => c.MatchingType == MatchingType.Fuzzy);
            Assert.Equal(2, addressCriterion.FieldMappings.Count);

            // Check the field mapping contains data sources from a valid pair
            var dataSourceIds = addressCriterion.FieldMappings.Select(fm => fm.DataSourceId).ToList();
            Assert.Equal(2, dataSourceIds.Count);

            // Check if this is a valid pair in our collection
            bool isPairValid =
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS2"])) ||
                (dataSourceIds.Contains(dataSources["DS1"]) && dataSourceIds.Contains(dataSources["DS3"])) ||
                (dataSourceIds.Contains(dataSources["DS2"]) && dataSourceIds.Contains(dataSources["DS3"]));

            Assert.True(isPairValid, "The field mappings should correspond to a valid data source pair");
        }

        // Act - Convert back to mapped row format
        var convertedDto = await _service.GetMappedRowConfigurationAsync(savedId);

        // Assert - Verify the converted DTO
        Assert.Equal(savedId, convertedDto.Id);
        Assert.Equal("Multi-Definition Test", convertedDto.Name);
        Assert.Equal(projectId, convertedDto.ProjectId);
        Assert.Equal(mappedRowDto.JobId, convertedDto.JobId);

        // Should have 2 UI definitions like the original input
        Assert.Equal(2, convertedDto.Definitions.Count);

        // Find the first converted UI definition (with Name and ID matching)
        var firstConvertedDef = convertedDto.Definitions.First(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text));

        // Should have 2 criteria (Name and ID)
        Assert.Equal(2, firstConvertedDef.Criteria.Count);

        // Each criterion should have all 3 data sources
        foreach (var criterion in firstConvertedDef.Criteria)
        {
            Assert.Equal(3, criterion.MappedRow.FieldsByDataSource.Count);
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS2"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS3"));
        }

        // Find the second converted UI definition (with Address and Email matching)
        var secondConvertedDef = convertedDto.Definitions.Last(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Fuzzy));

        // Should have 2 criteria (Address and Email)
        Assert.Equal(2, secondConvertedDef.Criteria.Count);

        // Each criterion should have all 3 data sources
        foreach (var criterion in secondConvertedDef.Criteria)
        {
            Assert.Equal(3, criterion.MappedRow.FieldsByDataSource.Count);
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS2"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS3"));
        }

        // Verify specific criteria in first UI definition

        // Name matching criterion
        var nameMatchingCriterion = firstConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.6, nameMatchingCriterion.Weight);
        Assert.Equal("80", nameMatchingCriterion.Arguments[ArgsValue.Level]);

        // Verify field names match the original input
        Assert.Equal("CustomerName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("FullName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("PersonName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);

        // ID matching criterion
        var idMatchingCriterion = firstConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Number);

        Assert.Equal(0.4, idMatchingCriterion.Weight);

        // Verify field names match the original input
        Assert.Equal("ID", idMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("CustomerID", idMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("PersonID", idMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);

        // Verify specific criteria in second UI definition

        // Address matching criterion
        var addressMatchingCriterion = secondConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.5, addressMatchingCriterion.Weight);
        Assert.Equal("3", addressMatchingCriterion.Arguments[ArgsValue.Level]);

        // Verify field names match the original input
        Assert.Equal("Address", addressMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("StreetAddress", addressMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("Location", addressMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);

        // Email matching criterion
        var emailMatchingCriterion = secondConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.5, emailMatchingCriterion.Weight);

        // Verify field names match the original input
        Assert.Equal("Email", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("EmailAddress", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("ContactEmail", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);
    }
    [Fact]
    public async Task SaveMappedRowWithThreeDataSources_ShouldSaveAsTwoPairs_ThenConvertBack()
    {
        // Arrange
        // 1. Create 3 data sources
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid(),
            ["DS3"] = Guid.NewGuid()
        };
        var projectId = Guid.NewGuid();

        // Add data sources to repository
        foreach (var ds in dataSources)
        {
            await _mockDataSourceRepo.InsertAsync(
                new DataSource { Id = ds.Value, Name = ds.Key },
                Constants.Collections.DataSources);
        }

        // 2. Create input DTO with multiple definitions, 
        // each containing all 3 data sources in their mapped rows
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = Guid.NewGuid(),
            Name = "Three DataSources Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
    {
        // First definition with 2 criteria, each with all 3 data sources
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // Name matching criterion with 3 data sources
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.5,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "80" },
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "CustomerName" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "FullName" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "PersonName" }
                        }
                    }
                },
                // ID matching criterion with 3 data sources
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Number,
                    Weight = 0.3,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "ID" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "CustomerID" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "PersonID" }
                        }
                    }
                }
            }
        },
        // Second definition with 2 criteria, each with all 3 data sources
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // Email matching criterion with 3 data sources
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.4,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Email" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "EmailAddress" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "ContactEmail" }
                        }
                    }
                },
                // Address matching criterion with 3 data sources
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.2,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "3" },
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Address" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "StreetAddress" },
                            ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "Location" }
                        }
                    }
                }
            }
        }
    }
        };

        // Create a pairs collection for the project with only two specific pairs (DS1-DS2 and DS2-DS3)
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Only add two of the three possible pairs
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Act - Save the mapped row configuration
        var savedCollectionId = await _service.SaveMappedRowConfigurationAsync(mappedRowDto);

        // Assert - Verify the saved configuration
        Assert.NotEqual(Guid.Empty, savedCollectionId);

        // Retrieve the saved collection from the database
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(savedCollectionId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(savedCollection);
        Assert.Equal("Three DataSources Test", savedCollection.Name);
        Assert.Equal(projectId, savedCollection.ProjectId);
        Assert.Equal(mappedRowDto.JobId, savedCollection.JobId);

        // Verify data source pairs in the database - should only be the 2 we created
        var pairsFromRepo = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pairsFromRepo);
        Assert.Equal(projectId, pairsFromRepo.ProjectId);
        Assert.Equal(2, pairsFromRepo.Count);

        // Verify the specific pairs exist
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS2"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS2"], dataSources["DS3"]));

        // Check that DS1-DS3 pair does NOT exist
        Assert.False(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS3"]));

        // The collection should have definitions for each pair (for both original definitions)
        // So 2 pairs * 2 definitions = 4 definitions
        Assert.Equal(4, savedCollection.Definitions.Count);

        // Group definitions by UI index
        var definitionsByIndex = savedCollection.Definitions.GroupBy(d => d.UIDefinitionIndex).ToList();
        Assert.Equal(2, definitionsByIndex.Count); // Should have 2 groups for the 2 original definitions

        // For each UI definition group, find the pairs used
        foreach (var defGroup in definitionsByIndex)
        {
            Assert.Equal(2, defGroup.Count()); // Should have 2 definitions per UI index (one for each pair)

            // Get all dataSourcePairIds in this group
            var pairIds = defGroup.Select(d => d.DataSourcePairId).Distinct().ToList();
            Assert.Equal(2, pairIds.Count); // Should have 2 unique pairs
        }

        // For the first UI definition group (index 0), verify criteria and field mappings
        var firstGroup = definitionsByIndex.First().ToList();

        // Check each definition in this group
        foreach (var def in firstGroup)
        {
            // Each definition should have the 2 criteria from first definition
            Assert.Equal(2, def.Criteria.Count);

            // Should have a fuzzy text criterion
            var fuzzyTextCriterion = def.Criteria.FirstOrDefault(c =>
                c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);
            Assert.NotNull(fuzzyTextCriterion);

            // Should have a exact number criterion
            var exactNumberCriterion = def.Criteria.FirstOrDefault(c =>
                c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Number);
            Assert.NotNull(exactNumberCriterion);

            // Each criterion should have 2 field mappings (one for each data source in the pair)
            Assert.Equal(2, fuzzyTextCriterion.FieldMappings.Count);
            Assert.Equal(2, exactNumberCriterion.FieldMappings.Count);

            // The data source IDs in each criterion should match to form a pair
            var dsIdsInFuzzyCriterion = fuzzyTextCriterion.FieldMappings.Select(fm => fm.DataSourceId).ToList();
            var dsIdsInExactCriterion = exactNumberCriterion.FieldMappings.Select(fm => fm.DataSourceId).ToList();

            // Both criteria should use the same pair of data sources
            Assert.Equal(dsIdsInFuzzyCriterion.OrderBy(id => id), dsIdsInExactCriterion.OrderBy(id => id));

            // Verify it's one of our valid pairs
            bool isValidPair =
                (dsIdsInFuzzyCriterion.Contains(dataSources["DS1"]) && dsIdsInFuzzyCriterion.Contains(dataSources["DS2"])) ||
                (dsIdsInFuzzyCriterion.Contains(dataSources["DS2"]) && dsIdsInFuzzyCriterion.Contains(dataSources["DS3"]));

            Assert.True(isValidPair, "The data sources should form one of our valid pairs");
        }

        // Act - Convert back to mapped row format
        var convertedMappedRowDto = await _service.GetMappedRowConfigurationAsync(savedCollectionId);

        // Assert - Verify the converted DTO
        Assert.Equal(savedCollectionId, convertedMappedRowDto.Id);
        Assert.Equal("Three DataSources Test", convertedMappedRowDto.Name);
        Assert.Equal(projectId, convertedMappedRowDto.ProjectId);
        Assert.Equal(mappedRowDto.JobId, convertedMappedRowDto.JobId);

        // Should be converted back to 2 definitions (like the original input)
        Assert.Equal(2, convertedMappedRowDto.Definitions.Count);

        // Find the first converted definition
        var firstConvertedDef = convertedMappedRowDto.Definitions.First(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text));

        // Should have 2 criteria (like the original)
        Assert.Equal(2, firstConvertedDef.Criteria.Count);

        // Each criterion should have all 3 data sources
        foreach (var criterion in firstConvertedDef.Criteria)
        {
            Assert.Equal(3, criterion.MappedRow.FieldsByDataSource.Count);
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS2"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS3"));
        }

        // Find the second converted definition
        var secondConvertedDef = convertedMappedRowDto.Definitions.First(d =>
            d.Criteria.Any(c => c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text));

        // Should have 2 criteria (like the original)
        Assert.Equal(2, secondConvertedDef.Criteria.Count);

        // Each criterion should have all 3 data sources
        foreach (var criterion in secondConvertedDef.Criteria)
        {
            Assert.Equal(3, criterion.MappedRow.FieldsByDataSource.Count);
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS2"));
            Assert.True(criterion.MappedRow.FieldsByDataSource.ContainsKey("DS3"));
        }

        // Verify the first criterion in first definition (Name matching)
        var nameMatchingCriterion = firstConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.5, nameMatchingCriterion.Weight);
        Assert.Equal("80", nameMatchingCriterion.Arguments[ArgsValue.Level]);

        // Verify field names match the original input
        Assert.Equal("CustomerName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("FullName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("PersonName", nameMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);

        // Verify the first criterion in second definition (Email matching)
        var emailMatchingCriterion = secondConvertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.4, emailMatchingCriterion.Weight);

        // Verify field names match the original input
        Assert.Equal("Email", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("EmailAddress", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
        Assert.Equal("ContactEmail", emailMatchingCriterion.MappedRow.FieldsByDataSource["DS3"].Name);
    }

    [Fact]
    public async Task SaveMappedRowConfiguration_WithSelfReferencePair_ShouldPreserveStructure()
    {
        // Arrange
        // 1. Create 2 data sources - including one that will reference itself
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid()
        };
        var projectId = Guid.NewGuid();

        // Add data sources to repository
        foreach (var ds in dataSources)
        {
            await _mockDataSourceRepo.InsertAsync(
                new DataSource { Id = ds.Value, Name = ds.Key },
                Constants.Collections.DataSources);
        }

        // 2. Create mapped row DTO with self-reference (DS1-DS1) and normal pair (DS1-DS2)
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = Guid.NewGuid(),
            Name = "Self-Reference Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
    {
        // Definition with self-reference criterion (DS1-DS1)
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // Self-reference criterion for deduplication
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.7,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "90" },
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "FullName" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "Name" }
                        }
                    }
                },
                // Normal criterion with both data sources
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.5,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Email" },
                            ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "EmailAddress" }
                        }
                    }
                }
            }
        }
    }
        };

        // Create pairs collection with project ID and add both pairs
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add both a self-reference pair and a normal pair
        pairsCollection.Add(dataSources["DS1"], dataSources["DS1"]);
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Act - Save the configuration
        var savedId = await _service.SaveMappedRowConfigurationAsync(mappedRowDto);

        // Assert - Verify the saved collection
        Assert.NotEqual(Guid.Empty, savedId);

        // Retrieve the saved collection
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(savedId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(savedCollection);
        Assert.Equal("Self-Reference Test", savedCollection.Name);
        Assert.Equal(projectId, savedCollection.ProjectId);
        Assert.Equal(mappedRowDto.JobId, savedCollection.JobId);

        // With 1 UI definition and 2 pairs (DS1-DS1 and DS1-DS2), we should have 2 definitions
        Assert.Equal(2, savedCollection.Definitions.Count);

        // Get pairs collection from repository
        var pairsFromRepo = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pairsFromRepo);
        Assert.Equal(projectId, pairsFromRepo.ProjectId);
        Assert.Equal(2, pairsFromRepo.Count);

        // Verify the pairs collection contains both pairs
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS1"]));
        Assert.True(pairsFromRepo.Contains(dataSources["DS1"], dataSources["DS2"]));

        // Group definitions by UIDefinitionIndex
        var defsByUiIndex = savedCollection.Definitions.GroupBy(d => d.UIDefinitionIndex).ToList();
        Assert.Single(defsByUiIndex); // Should have 1 group (one UI definition)

        // Verify the UI definition group
        var uiGroup = defsByUiIndex.First().ToList();
        Assert.Equal(2, uiGroup.Count); // One per pair

        // Find the self-reference definition
        var selfRefDef = uiGroup.FirstOrDefault(d => {
            if (d.Criteria.Count == 0) return false;
            var criterion = d.Criteria.FirstOrDefault(c => c.MatchingType == MatchingType.Fuzzy);
            if (criterion == null || criterion.FieldMappings.Count != 1) return false;

            // For self-reference, there should be exactly one field mapping with DataSourceId = DS1
            return criterion.FieldMappings.Count == 1 &&
                   criterion.FieldMappings[0].DataSourceId == dataSources["DS1"] &&
                   criterion.FieldMappings[0].FieldName == "FullName";
        });

        // Find the normal pair definition
        var normalPairDef = uiGroup.FirstOrDefault(d => {
            if (d.Criteria.Count == 0) return false;
            var criterion = d.Criteria.FirstOrDefault(c => c.MatchingType == MatchingType.Exact);
            if (criterion == null || criterion.FieldMappings.Count != 2) return false;

            // For normal pair, there should be exactly two field mappings for DS1 and DS2
            var ds1Mapping = criterion.FieldMappings.FirstOrDefault(fm =>
                fm.DataSourceId == dataSources["DS1"] && fm.FieldName == "Email");
            var ds2Mapping = criterion.FieldMappings.FirstOrDefault(fm =>
                fm.DataSourceId == dataSources["DS2"] && fm.FieldName == "EmailAddress");

            return ds1Mapping != null && ds2Mapping != null;
        });

        Assert.NotNull(selfRefDef);
        Assert.NotNull(normalPairDef);

        // Verify criteria for self-reference pair
        Assert.True(selfRefDef.Criteria.Count >= 1);

        // Find the self-reference criterion
        var selfRefCriterion = selfRefDef.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.NotNull(selfRefCriterion);

        // Verify it has exactly 1 field mapping (to itself)
        Assert.Single(selfRefCriterion.FieldMappings);
        Assert.Equal(dataSources["DS1"], selfRefCriterion.FieldMappings[0].DataSourceId);
        Assert.Equal("FullName", selfRefCriterion.FieldMappings[0].FieldName);

        // Verify criteria for normal pair
        Assert.True(normalPairDef.Criteria.Count >= 1);

        // Find the normal criterion
        var normalCriterion = normalPairDef.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.NotNull(normalCriterion);

        // Verify it has exactly 2 field mappings
        Assert.Equal(2, normalCriterion.FieldMappings.Count);
        Assert.Contains(normalCriterion.FieldMappings, fm =>
            fm.DataSourceId == dataSources["DS1"] && fm.FieldName == "Email");
        Assert.Contains(normalCriterion.FieldMappings, fm =>
            fm.DataSourceId == dataSources["DS2"] && fm.FieldName == "EmailAddress");

        // Act - Convert back to mapped row format
        var convertedDto = await _service.GetMappedRowConfigurationAsync(savedId);

        // Assert - Verify the converted DTO
        Assert.Equal(savedId, convertedDto.Id);
        Assert.Equal("Self-Reference Test", convertedDto.Name);
        Assert.Equal(projectId, convertedDto.ProjectId);
        Assert.Equal(mappedRowDto.JobId, convertedDto.JobId);

        // Should have 1 UI definition like the original input
        Assert.Single(convertedDto.Definitions);

        var convertedDef = convertedDto.Definitions.First();

        // Should have 2 criteria
        Assert.Equal(2, convertedDef.Criteria.Count);

        // Verify the self-reference criterion
        var convertedSelfRefCriterion = convertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.7, convertedSelfRefCriterion.Weight);
        Assert.Equal("90", convertedSelfRefCriterion.Arguments[ArgsValue.Level]);

        // Verify it has the DS1 field mapping
        Assert.True(convertedSelfRefCriterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
        Assert.Equal("FullName", convertedSelfRefCriterion.MappedRow.FieldsByDataSource["DS1"].Name);

        // Verify the normal criterion
        var convertedNormalCriterion = convertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.5, convertedNormalCriterion.Weight);

        // Verify it has both field mappings
        Assert.Equal(2, convertedNormalCriterion.MappedRow.FieldsByDataSource.Count);
        Assert.True(convertedNormalCriterion.MappedRow.FieldsByDataSource.ContainsKey("DS1"));
        Assert.True(convertedNormalCriterion.MappedRow.FieldsByDataSource.ContainsKey("DS2"));
        Assert.Equal("Email", convertedNormalCriterion.MappedRow.FieldsByDataSource["DS1"].Name);
        Assert.Equal("EmailAddress", convertedNormalCriterion.MappedRow.FieldsByDataSource["DS2"].Name);
    }
    [Fact]
    public async Task SaveMappedRowConfiguration_WithOnlySelfReferencePair_ShouldWorkCorrectly()
    {
        // Arrange
        // 1. Create the single data source that will reference itself
        var dataSourceId = Guid.NewGuid();
        var dataSourceName = "DS1";
        var projectId = Guid.NewGuid();

        // Add data source to repository
        await _mockDataSourceRepo.InsertAsync(
            new DataSource { Id = dataSourceId, Name = dataSourceName },
            Constants.Collections.DataSources);

        // 2. Create mapped row DTO with only self-reference (DS1-DS1)
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = Guid.NewGuid(),
            Name = "DS1-DS1 Self-Reference Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
    {
        // Definition with only self-reference criteria
        new MatchDefinitionMappedRowDto
        {
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                // First self-reference criterion for name matching
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.8,
                    Arguments = new Dictionary<ArgsValue, string> { [ArgsValue.Level] = "95" },
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            [dataSourceName] = new FieldDto
                            {
                                DataSourceId = dataSourceId,
                                DataSourceName = dataSourceName,
                                Name = "Name"
                            }
                        }
                    }
                },
                // Second self-reference criterion for ID matching
                new MatchCriterionMappedRowDto
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 1.0,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            [dataSourceName] = new FieldDto
                            {
                                DataSourceId = dataSourceId,
                                DataSourceName = dataSourceName,
                                Name = "ID"
                            }
                        }
                    }
                }
            }
        }
    }
        };

        // Create pairs collection with project ID
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add the self-reference pair to the collection
        pairsCollection.Add(dataSourceId, dataSourceId);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Act - Save the configuration
        var savedId = await _service.SaveMappedRowConfigurationAsync(mappedRowDto);

        // Assert - Verify the saved collection
        Assert.NotEqual(Guid.Empty, savedId);

        // Retrieve the saved collection
        var savedCollection = await _mockCollectionRepo.GetByIdAsync(savedId, Constants.Collections.MatchDefinitionCollection);
        Assert.NotNull(savedCollection);
        Assert.Equal("DS1-DS1 Self-Reference Test", savedCollection.Name);
        Assert.Equal(projectId, savedCollection.ProjectId);
        Assert.Equal(mappedRowDto.JobId, savedCollection.JobId);

        // With 1 UI definition and 1 pair (DS1-DS1), we should have 1 definition
        Assert.Single(savedCollection.Definitions);

        // Get pairs collection from repository
        var pairsFromRepo = await _service.GetDataSourcePairsByProjectIdAsync(projectId);
        Assert.NotNull(pairsFromRepo);
        Assert.Equal(projectId, pairsFromRepo.ProjectId);
        // Assert.Single(pairsFromRepo); // Should have only one pair

        // Verify the pairs collection contains the self-reference pair
        Assert.True(pairsFromRepo.Contains(dataSourceId, dataSourceId));

        // Verify the definition
        var definition = savedCollection.Definitions.Single();
        Assert.Equal(0, definition.UIDefinitionIndex); // First UI definition

        // Verify criteria
        Assert.Equal(2, definition.Criteria.Count);

        // Find the fuzzy name criterion
        var nameCriterion = definition.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.NotNull(nameCriterion);
        Assert.Equal(0.8, nameCriterion.Weight);
        Assert.Equal("95", nameCriterion.Arguments[ArgsValue.Level]);

        // Verify it has exactly 1 field mapping (to itself)
        Assert.Single(nameCriterion.FieldMappings);
        Assert.Equal(dataSourceId, nameCriterion.FieldMappings[0].DataSourceId);
        Assert.Equal("Name", nameCriterion.FieldMappings[0].FieldName);

        // Find the exact ID criterion
        var idCriterion = definition.Criteria.FirstOrDefault(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.NotNull(idCriterion);
        Assert.Equal(1.0, idCriterion.Weight);

        // Verify it has exactly 1 field mapping
        Assert.Single(idCriterion.FieldMappings);
        Assert.Equal(dataSourceId, idCriterion.FieldMappings[0].DataSourceId);
        Assert.Equal("ID", idCriterion.FieldMappings[0].FieldName);

        // Act - Convert back to mapped row format
        var convertedDto = await _service.GetMappedRowConfigurationAsync(savedId);

        // Assert - Verify the converted DTO
        Assert.Equal(savedId, convertedDto.Id);
        Assert.Equal("DS1-DS1 Self-Reference Test", convertedDto.Name);
        Assert.Equal(projectId, convertedDto.ProjectId);
        Assert.Equal(mappedRowDto.JobId, convertedDto.JobId);

        // Should have 1 UI definition like the original input
        Assert.Single(convertedDto.Definitions);

        var convertedDef = convertedDto.Definitions.First();

        // Should have 2 criteria
        Assert.Equal(2, convertedDef.Criteria.Count);

        // Verify the name criterion
        var convertedNameCriterion = convertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text);

        Assert.Equal(0.8, convertedNameCriterion.Weight);
        Assert.Equal("95", convertedNameCriterion.Arguments[ArgsValue.Level]);

        // Verify it has the DS1 field mapping
        Assert.True(convertedNameCriterion.MappedRow.FieldsByDataSource.ContainsKey(dataSourceName));
        Assert.Equal("Name", convertedNameCriterion.MappedRow.FieldsByDataSource[dataSourceName].Name);

        // Verify the ID criterion
        var convertedIdCriterion = convertedDef.Criteria.First(c =>
            c.MatchingType == MatchingType.Exact && c.DataType == CriteriaDataType.Text);

        Assert.Equal(1.0, convertedIdCriterion.Weight);

        // Verify it has the field mapping
        Assert.Single(convertedIdCriterion.MappedRow.FieldsByDataSource);
        Assert.True(convertedIdCriterion.MappedRow.FieldsByDataSource.ContainsKey(dataSourceName));
        Assert.Equal("ID", convertedIdCriterion.MappedRow.FieldsByDataSource[dataSourceName].Name);
    }
}
