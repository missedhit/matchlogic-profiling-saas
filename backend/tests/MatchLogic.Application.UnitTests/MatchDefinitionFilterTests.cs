using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace MatchLogic.Application.UnitTests;

public class MatchDefinitionFilterTests
{
    private readonly Mock<ILogger<MatchDefinitionFilter>> _loggerMock;
    private readonly Mock<IAutoMappingService> _autoMappingServiceMock;
    private readonly Mock<IGenericRepository<MatchingDataSourcePairs, Guid>> _dataSourcePairsRepoMock;
    private readonly MatchDefinitionFilter _filter;

    public MatchDefinitionFilterTests()
    {
        _loggerMock = new Mock<ILogger<MatchDefinitionFilter>>();
        _autoMappingServiceMock = new Mock<IAutoMappingService>();
        _dataSourcePairsRepoMock = new Mock<IGenericRepository<MatchingDataSourcePairs, Guid>>();

        _filter = new MatchDefinitionFilter(
            _loggerMock.Object,
            _autoMappingServiceMock.Object,
            _dataSourcePairsRepoMock.Object);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Assert
        Assert.Throws<ArgumentNullException>(() =>
            new MatchDefinitionFilter(null, _autoMappingServiceMock.Object, _dataSourcePairsRepoMock.Object));
    }

    [Fact]
    public void Constructor_WithNullAutoMappingService_ThrowsArgumentNullException()
    {
        // Assert
        Assert.Throws<ArgumentNullException>(() =>
            new MatchDefinitionFilter(_loggerMock.Object, null, _dataSourcePairsRepoMock.Object));
    }

    [Fact]
    public void Constructor_WithNullRepository_ThrowsArgumentNullException()
    {
        // Assert
        Assert.Throws<ArgumentNullException>(() =>
            new MatchDefinitionFilter(_loggerMock.Object, _autoMappingServiceMock.Object, null));
    }

    #endregion

    #region GetRelevantDefinitions Tests

    [Fact]
    public void GetRelevantDefinitions_WithNullCollection_ThrowsArgumentNullException()
    {
        // Arrange
        var dataSourcePair = CreateTestDataSourcePair();

        // Assert
        Assert.Throws<ArgumentNullException>(() =>
            _filter.GetRelevantDefinitions((MatchDefinitionCollection)null, dataSourcePair));
    }

    [Fact]
    public void GetRelevantDefinitions_WithNullPair_ThrowsArgumentNullException()
    {
        // Arrange
        var collection = CreateTestMatchDefinitionCollection();

        // Assert
        Assert.Throws<ArgumentNullException>(() =>
            _filter.GetRelevantDefinitions(collection, null));
    }

    [Fact]
    public void GetRelevantDefinitions_ForCrossSourcePair_ReturnsMatchingDefinitions()
    {
        // Arrange
        var sourceA = Guid.NewGuid();
        var sourceB = Guid.NewGuid();
        var dataSourcePair = new MatchingDataSourcePair(sourceA, sourceB);

        var definitions = new List<MatchDefinition>
        {
            CreateCrossSourceDefinition(sourceA, sourceB), // Should match
            CreateCrossSourceDefinition(sourceA, Guid.NewGuid()), // Should not match
            CreateSingleSourceDefinition(sourceA) // Should not match (only uses one source)
        };

        // Act
        var result = _filter.GetRelevantDefinitions(definitions, dataSourcePair);

        // Assert
        Assert.Single(result);
        Assert.Equal(definitions[0].Id, result[0].Id);
    }

    [Fact]
    public void GetRelevantDefinitions_ForDeduplicationPair_ReturnsMatchingDefinitions()
    {
        // Arrange
        var sourceA = Guid.NewGuid();
        var dataSourcePair = new MatchingDataSourcePair(sourceA, sourceA); // Same source (deduplication)

        var definitions = new List<MatchDefinition>
        {
            CreateSingleSourceDefinition(sourceA), // Should match
            CreateSingleSourceDefinition(Guid.NewGuid()), // Should not match
            CreateCrossSourceDefinition(sourceA, Guid.NewGuid()) // Should not match
        };

        // Act
        var result = _filter.GetRelevantDefinitions(definitions, dataSourcePair);

        // Assert
        Assert.Single(result);
        Assert.Equal(definitions[0].Id, result[0].Id);
    }

    [Fact]
    public void GetRelevantDefinitions_WithEmptyDefinitions_ReturnsEmptyList()
    {
        // Arrange
        var dataSourcePair = CreateTestDataSourcePair();
        var definitions = new List<MatchDefinition>();

        // Act
        var result = _filter.GetRelevantDefinitions(definitions, dataSourcePair);

        // Assert
        Assert.Empty(result);
    }

    #endregion

    #region IsDefinitionRelevant Tests

    [Fact]
    public void IsDefinitionRelevant_WithNullDefinition_ReturnsFalse()
    {
        // Arrange
        var dataSourcePair = CreateTestDataSourcePair();

        // Act
        var result = _filter.IsDefinitionRelevant(null, dataSourcePair);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void IsDefinitionRelevant_WithNullPair_ReturnsFalse()
    {
        // Arrange
        var definition = CreateSingleSourceDefinition(Guid.NewGuid());

        // Act
        var result = _filter.IsDefinitionRelevant(definition, null);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void IsDefinitionRelevant_DefinitionWithNoFieldMappings_ReturnsFalse()
    {
        // Arrange
        var dataSourcePair = CreateTestDataSourcePair();
        var definition = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    FieldMappings = new List<FieldMapping>() // Empty field mappings
                }
            }
        };

        // Act
        var result = _filter.IsDefinitionRelevant(definition, dataSourcePair);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void IsDefinitionRelevant_CrossSourceDefinitionWithOnlyOneSourceUsed_ReturnsFalse()
    {
        // Arrange
        var sourceA = Guid.NewGuid();
        var sourceB = Guid.NewGuid();
        var dataSourcePair = new MatchingDataSourcePair(sourceA, sourceB);
        var definition = CreateSingleSourceDefinition(sourceA); // Only uses sourceA

        // Act
        var result = _filter.IsDefinitionRelevant(definition, dataSourcePair);

        // Assert
        Assert.False(result);
    }

    #endregion

    #region ExtractDataSourceIds Tests

    [Fact]
    public void ExtractDataSourceIds_WithNullDefinition_ReturnsEmptySet()
    {
        // Act
        var result = _filter.ExtractDataSourceIds(null);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void ExtractDataSourceIds_WithNoCriteria_ReturnsEmptySet()
    {
        // Arrange
        var definition = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            Criteria = null
        };

        // Act
        var result = _filter.ExtractDataSourceIds(definition);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void ExtractDataSourceIds_WithMultipleDataSources_ReturnsAllUniqueIds()
    {
        // Arrange
        var sourceA = Guid.NewGuid();
        var sourceB = Guid.NewGuid();
        var definition = CreateCrossSourceDefinition(sourceA, sourceB);

        // Act
        var result = _filter.ExtractDataSourceIds(definition);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains(sourceA, result);
        Assert.Contains(sourceB, result);
    }

    [Fact]
    public void ExtractDataSourceIds_WithDuplicateDataSources_ReturnsUniqueIds()
    {
        // Arrange
        var sourceA = Guid.NewGuid();
        var definition = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    FieldMappings = new List<FieldMapping>
                    {
                        new FieldMapping(sourceA, "Source1", "Field1"),
                        new FieldMapping(sourceA, "Source1", "Field2"), // Same source
                    }
                }
            }
        };

        // Act
        var result = _filter.ExtractDataSourceIds(definition);

        // Assert
        Assert.Single(result);
        Assert.Contains(sourceA, result);
    }

    #endregion

    #region GetFieldMappingsByDataSource Tests

    [Fact]
    public void GetFieldMappingsByDataSource_WithNullDefinition_ReturnsEmptyDictionary()
    {
        // Act
        var result = _filter.GetFieldMappingsByDataSource(null);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void GetFieldMappingsByDataSource_GroupsFieldsByDataSource()
    {
        // Arrange
        var sourceA = Guid.NewGuid();
        var sourceB = Guid.NewGuid();
        var definition = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    FieldMappings = new List<FieldMapping>
                    {
                        new FieldMapping(sourceA, "SourceA", "Field1"),
                        new FieldMapping(sourceA, "SourceA", "Field2"),
                        new FieldMapping(sourceB, "SourceB", "Field1"),
                    }
                }
            }
        };

        // Act
        var result = _filter.GetFieldMappingsByDataSource(definition);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Equal(2, result[sourceA].Count);
        Assert.Single(result[sourceB]);
    }

    [Fact]
    public void GetFieldMappingsByDataSource_RemovesDuplicateFieldMappings()
    {
        // Arrange
        var sourceA = Guid.NewGuid();
        var definition = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    FieldMappings = new List<FieldMapping>
                    {
                        new FieldMapping(sourceA, "SourceA", "Field1"),
                        new FieldMapping(sourceA, "SourceA", "Field1"), // Duplicate
                    }
                }
            }
        };

        // Act
        var result = _filter.GetFieldMappingsByDataSource(definition);

        // Assert
        Assert.Single(result[sourceA]);
        Assert.Equal("Field1", result[sourceA][0].FieldName);
    }

    #endregion

    #region ValidateFieldAvailabilityAsync Tests

    [Fact]
    public async Task ValidateFieldAvailabilityAsync_WithNullDefinition_ReturnsFalse()
    {
        // Act
        var result = await _filter.ValidateFieldAvailabilityAsync(null, Guid.NewGuid());

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFieldAvailabilityAsync_WithEmptyProjectId_ReturnsFalse()
    {
        // Arrange
        var definition = CreateSingleSourceDefinition(Guid.NewGuid());

        // Act
        var result = await _filter.ValidateFieldAvailabilityAsync(definition, Guid.Empty);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFieldAvailabilityAsync_NoFieldsInProject_ReturnsFalse()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var definition = CreateSingleSourceDefinition(Guid.NewGuid());

        _autoMappingServiceMock
            .Setup(x => x.GetExtendedFieldsAsync(projectId,true))
            .ReturnsAsync(new Dictionary<string, List<FieldMappingEx>>());

        // Act
        var result = await _filter.ValidateFieldAvailabilityAsync(definition, projectId);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFieldAvailabilityAsync_AllFieldsExist_ReturnsTrue()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();
        var definition = CreateSingleSourceDefinition(dataSourceId, "Field1", "Field2");

        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DataSource1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx
                {
                    DataSourceId = dataSourceId,
                    FieldName = "Field1",
                    DataSourceName = "DataSource1"
                },
                new FieldMappingEx
                {
                    DataSourceId = dataSourceId,
                    FieldName = "Field2",
                    DataSourceName = "DataSource1"
                },
                new FieldMappingEx
                {
                    DataSourceId = dataSourceId,
                    FieldName = "Field3",
                    DataSourceName = "DataSource1"
                }
            }
        };

        _autoMappingServiceMock
            .Setup(x => x.GetExtendedFieldsAsync(projectId, true))
            .ReturnsAsync(fieldsPerDataSource);

        // Act
        var result = await _filter.ValidateFieldAvailabilityAsync(definition, projectId);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task ValidateFieldAvailabilityAsync_MissingField_ReturnsFalse()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();
        var definition = CreateSingleSourceDefinition(dataSourceId, "Field1", "MissingField");

        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DataSource1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx
                {
                    DataSourceId = dataSourceId,
                    FieldName = "Field1",
                    DataSourceName = "DataSource1"
                }
            }
        };

        _autoMappingServiceMock
            .Setup(x => x.GetExtendedFieldsAsync(projectId, true))
            .ReturnsAsync(fieldsPerDataSource);

        // Act
        var result = await _filter.ValidateFieldAvailabilityAsync(definition, projectId);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFieldAvailabilityAsync_DataSourceNotInProject_ReturnsFalse()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();
        var differentDataSourceId = Guid.NewGuid();
        var definition = CreateSingleSourceDefinition(dataSourceId, "Field1");

        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DataSource1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx
                {
                    DataSourceId = differentDataSourceId, // Different data source
                    FieldName = "Field1",
                    DataSourceName = "DataSource1"
                }
            }
        };

        _autoMappingServiceMock
            .Setup(x => x.GetExtendedFieldsAsync(projectId, true))
            .ReturnsAsync(fieldsPerDataSource);

        // Act
        var result = await _filter.ValidateFieldAvailabilityAsync(definition, projectId);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFieldAvailabilityAsync_WithException_ReturnsFalse()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var definition = CreateSingleSourceDefinition(Guid.NewGuid());

        _autoMappingServiceMock
            .Setup(x => x.GetExtendedFieldsAsync(projectId, true))
            .ThrowsAsync(new Exception("Test exception"));

        // Act
        var result = await _filter.ValidateFieldAvailabilityAsync(definition, projectId);

        // Assert
        Assert.False(result);
    }

    #endregion

    #region Helper Methods

    private MatchingDataSourcePair CreateTestDataSourcePair(Guid? sourceA = null, Guid? sourceB = null)
    {
        return new MatchingDataSourcePair(
            sourceA ?? Guid.NewGuid(),
            sourceB ?? Guid.NewGuid());
    }

    private MatchDefinitionCollection CreateTestMatchDefinitionCollection()
    {
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Test Collection",
            Definitions = new List<MatchDefinition>()
        };
    }

    private MatchDefinition CreateSingleSourceDefinition(Guid dataSourceId, params string[] fieldNames)
    {
        var fields = fieldNames.Any() ? fieldNames : new[] { "Field1", "Field2" };

        return new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            UIDefinitionIndex = 1,
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 1.0,
                    FieldMappings = fields.Select(f =>
                        new FieldMapping(dataSourceId, "DataSource", f)).ToList()
                }
            }
        };
    }

    private MatchDefinition CreateCrossSourceDefinition(Guid sourceA, Guid sourceB)
    {
        return new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            UIDefinitionIndex = 1,
            Criteria = new List<MatchCriteria>
            {
                new MatchCriteria
                {
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.5,
                    FieldMappings = new List<FieldMapping>
                    {
                        new FieldMapping(sourceA, "SourceA", "Field1"),
                        new FieldMapping(sourceB, "SourceB", "Field1")
                    }
                },
                new MatchCriteria
                {
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.5,
                    FieldMappings = new List<FieldMapping>
                    {
                        new FieldMapping(sourceA, "SourceA", "Field2"),
                        new FieldMapping(sourceB, "SourceB", "Field2")
                    }
                }
            }
        };
    }

    #endregion
}
