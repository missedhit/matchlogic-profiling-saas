using MatchLogic.Application.EventHandlers;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching.Grouping;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Project.Commands;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace MatchLogic.Application.UnitTests;

public class EnhancedGroupingServiceTests : IDisposable
{
    private readonly ProductionQGramIndexer _indexer;
    private readonly EnhancedRecordComparisonService _comparisonService;
    private readonly EnhancedGroupingService _groupingService;

    private readonly Mock<ILogger<ProductionQGramIndexer>> _indexerLoggerMock;
    private readonly Mock<ILogger<EnhancedRecordComparisonService>> _comparisonLoggerMock;
    private readonly Mock<ILogger<EnhancedGroupingService>> _groupingLoggerMock;
    private readonly Mock<ITelemetry> _telemetryMock;
    private readonly Mock<IDataSourceIndexMapper> _indexMapperMock;
    private readonly Mock<IStepProgressTracker> _progressTrackerMock;

    private readonly Guid _dataSourceId = Guid.NewGuid();
    private readonly Guid _dataSource2Id = Guid.NewGuid();

    public EnhancedGroupingServiceTests()
    {
        _indexerLoggerMock = new Mock<ILogger<ProductionQGramIndexer>>();
        _comparisonLoggerMock = new Mock<ILogger<EnhancedRecordComparisonService>>();
        _groupingLoggerMock = new Mock<ILogger<EnhancedGroupingService>>();
        _telemetryMock = new Mock<ITelemetry>();
        _indexMapperMock = new Mock<IDataSourceIndexMapper>();
        _progressTrackerMock = new Mock<IStepProgressTracker>();

        // Setup indexer with q-gram size 3 for better similarity detection
        var indexerOptions = Options.Create(new QGramIndexerOptions
        {
            QGramSize = 3,  // Changed to 3 for better similarity detection
            MaxParallelism = 2,
            CandidateChannelCapacity = 1000
        });
        _indexer = new ProductionQGramIndexer(indexerOptions, _indexerLoggerMock.Object);

        // Setup comparison service with REAL comparator that returns actual scores
        var comparatorBuilder = CreateRealComparatorBuilder();
        var linkageOptions = Options.Create(new RecordLinkageOptions
        {
            BatchSize = 10,
            MaxDegreeOfParallelism = 2,
            MinimumMatchScore = 0.0 // Set to 0 to allow all matches through for testing
        });

        _comparisonService = new EnhancedRecordComparisonService(
            _comparisonLoggerMock.Object,
            _telemetryMock.Object,
            comparatorBuilder,
            linkageOptions);

        // Setup grouping service
        _groupingService = new EnhancedGroupingService(_groupingLoggerMock.Object);

        _telemetryMock.Setup(x => x.MeasureOperation(It.IsAny<string>()))
            .Returns(Mock.Of<IDisposable>());

        _progressTrackerMock.Setup(x => x.UpdateProgressAsync(It.IsAny<int>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }

    #region Test Data Creation

    private async Task<List<IDictionary<string, object>>> CreateTestDataWithKnownMatches()
    {
        var records = new List<IDictionary<string, object>>
        {
            // Group 1: Records that all match each other (clique)
            CreateRecord(0, "John Smith", "john@email.com", "123 Main St"),
            CreateRecord(1, "John Smith", "john@email.com", "123 Main Street"),
            CreateRecord(2, "Jon Smith", "john@email.com", "123 Main St"),
            
            // Group 2: Chain match (A-B-C but not A-C)
            CreateRecord(3, "Jane Doe", "jane@email.com", "456 Oak Ave"),
            CreateRecord(4, "Jane Doe", "jane.doe@email.com", "456 Oak Ave"),
            CreateRecord(5, "J. Doe", "jane.doe@email.com", "456 Oak Avenue"),
            
            // Group 3: Isolated pair
            CreateRecord(6, "Bob Wilson", "bob@email.com", "789 Pine Rd"),
            CreateRecord(7, "Robert Wilson", "bob@email.com", "789 Pine Rd"),
            
            // Singleton (no matches)
            CreateRecord(8, "Alice Johnson", "alice@email.com", "321 Elm St")
        };

        return records;
    }

    private async Task<List<IDictionary<string, object>>> CreateCrossSourceTestData()
    {
        var records = new List<IDictionary<string, object>>();

        // Source 1 records
        records.Add(CreateRecord(0, "John Smith", "john@email.com", "123 Main St", _dataSourceId));
        records.Add(CreateRecord(1, "Jane Doe", "jane@email.com", "456 Oak Ave", _dataSourceId));
        records.Add(CreateRecord(2, "Bob Wilson", "bob@email.com", "789 Pine Rd", _dataSourceId));

        // Source 2 records that match Source 1
        records.Add(CreateRecord(0, "John Smith", "john.smith@email.com", "123 Main Street", _dataSource2Id));
        records.Add(CreateRecord(1, "Jane M. Doe", "jane@email.com", "456 Oak Ave", _dataSource2Id));
        records.Add(CreateRecord(2, "Robert Wilson", "bob.wilson@email.com", "789 Pine Rd", _dataSource2Id));

        return records;
    }

    private IDictionary<string, object> CreateRecord(
        int rowNumber,
        string name,
        string email,
        string address,
        Guid? dataSourceId = null)
    {
        return new Dictionary<string, object>
        {
            ["Name"] = name,
            ["Email"] = email,
            ["Address"] = address,
            ["_metadata"] = new Dictionary<string, object>
            {
                ["RowNumber"] = rowNumber,
                ["DataSourceId"] = dataSourceId ?? _dataSourceId
            }
        };
    }

    #endregion

    #region Non-Transitive Grouping Tests

    [Fact]
    public async Task CreateGroups_NonTransitive_CreatesConnectedComponents()
    {
        // Arrange
        var testData = await CreateTestDataWithKnownMatches();
        var matchDefinition = CreateFuzzyMatchDefinitionForSingleSource();

        // Run through the pipeline
        var (matchResults, matchGraph) = await RunFullPipeline(testData, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: false))
        {
            groups.Add(group);
        }

        // Assert
        // Non-transitive should create connected components
        Assert.True(groups.Count >= 3, $"Expected at least 3 groups, got {groups.Count}");

        // Group 1: Should have all 3 John Smith records
        var group1 = groups.FirstOrDefault(g => g.Records.Any(r =>
            r["Name"]?.ToString()?.Contains("John Smith") == true));
        Assert.NotNull(group1);
        Assert.True(group1.Records.Count >= 2, $"John Smith group should have at least 2 records, got {group1.Records.Count}");

        // Group 2: Should have Jane Doe records
        var group2 = groups.FirstOrDefault(g => g.Records.Any(r =>
            r["Name"]?.ToString()?.Contains("Jane") == true));
        Assert.NotNull(group2);
        Assert.True(group2.Records.Count >= 2, $"Jane group should have at least 2 records, got {group2.Records.Count}");

        // Group 3: Bob Wilson pair
        var group3 = groups.FirstOrDefault(g => g.Records.Any(r =>
            r["Name"]?.ToString()?.Contains("Bob") == true ||
            r["Name"]?.ToString()?.Contains("Wilson") == true));
        Assert.NotNull(group3);
        Assert.Equal(2, group3.Records.Count);
    }

    [Fact]
    public async Task CreateGroups_NonTransitive_HandlesDisconnectedComponents()
    {
        // Arrange - Create data with multiple disconnected groups
        var records = new List<IDictionary<string, object>>
        {
            // Component 1
            CreateRecord(0, "Group1_A", "a@email.com", "123 St"),
            CreateRecord(1, "Group1_B", "a@email.com", "123 St"),
            
            // Component 2 (disconnected)
            CreateRecord(2, "Group2_A", "b@email.com", "456 Ave"),
            CreateRecord(3, "Group2_B", "b@email.com", "456 Ave"),
            
            // Component 3 (disconnected)
            CreateRecord(4, "Group3_A", "c@email.com", "789 Rd"),
            CreateRecord(5, "Group3_B", "c@email.com", "789 Rd")
        };

        var matchDefinition = CreateExactMatchDefinitionForSingleSource("Email");
        var (_, matchGraph) = await RunFullPipeline(records, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: false))
        {
            groups.Add(group);
        }

        // Assert
        Assert.Equal(3, groups.Count);
        Assert.All(groups, g => Assert.Equal(2, g.Records.Count));
    }

    #endregion

    #region Transitive Grouping Tests

    [Fact]
    public async Task CreateGroups_Transitive_OnlyCreatesCliques()
    {
        // Arrange
        var testData = await CreateTestDataWithKnownMatches();
        var matchDefinition = CreateFuzzyMatchDefinitionForSingleSource();
        var (_, matchGraph) = await RunFullPipeline(testData, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: true))
        {
            groups.Add(group);
        }

        // Assert
        Assert.True(groups.Count >= 3, $"Expected at least 3 groups, got {groups.Count}");

        // John Smith group should still exist (if they all match each other)
        var johnGroups = groups.Where(g => g.Records.Any(r =>
            r["Name"]?.ToString()?.Contains("John") == true ||
            r["Name"]?.ToString()?.Contains("Smith") == true)).ToList();
        Assert.NotEmpty(johnGroups);

        // Bob Wilson pair should remain
        var bobGroups = groups.Where(g => g.Records.Any(r =>
            r["Name"]?.ToString()?.Contains("Bob") == true ||
            r["Name"]?.ToString()?.Contains("Wilson") == true)).ToList();
        Assert.NotEmpty(bobGroups);
    }

    [Fact]
    public async Task CreateGroups_Transitive_ValidatesCliqueProperty()
    {
        // Arrange - Create specific pattern: A-B, B-C, A-C (complete triangle)
        var records = new List<IDictionary<string, object>>
        {
            CreateRecord(0, "Record A", "same@email.com", "123 St"),
            CreateRecord(1, "Record B", "same@email.com", "123 St"),
            CreateRecord(2, "Record C", "same@email.com", "123 St")
        };

        // Use exact match to ensure all records match each other
        var matchDefinition = CreateExactMatchDefinitionForSingleSource("Email");
        var (_, matchGraph) = await RunFullPipeline(records, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: true))
        {
            groups.Add(group);
        }

        // Assert
        Assert.Single(groups);
        Assert.Equal(3, groups[0].Records.Count); // All three form a clique

        // Verify metadata indicates it's a clique
        Assert.True((bool)groups[0].Metadata["is_clique"]);
    }

    [Fact]
    public async Task CreateGroups_Transitive_SplitsNonCliques()
    {
        // Arrange - Create chain: A-B-C where A doesn't match C
        var records = new List<IDictionary<string, object>>
        {
            CreateRecord(0, "Start", "a@email.com", "123 St"),
            CreateRecord(1, "Middle", "a@email.com", "456 Ave"), // Matches Start by email
            CreateRecord(2, "End", "b@email.com", "456 Ave")     // Matches Middle by address, not Start
        };

        var matchDefinition = CreateMultiFieldMatchDefinition();
        var (_, matchGraph) = await RunFullPipeline(records, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: true))
        {
            groups.Add(group);
        }

        // Assert
        // Should create separate groups since it's not a complete clique
        Assert.True(groups.Count >= 2 || groups.Any(g => g.Records.Count < 3));

        groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: false))
        {
            groups.Add(group);
        }

        // Assert
        // Should create one groups
        Assert.Single(groups);
    }

    #endregion

    #region Cross-Source Tests

    [Fact]
    public async Task CreateGroups_CrossSource_NonTransitive_MergesAcrossSources()
    {
        // Arrange
        var testData = await CreateCrossSourceTestData();
        var matchDefinition = CreateCrossSourceMatchDefinition();
        var (_, matchGraph) = await RunFullPipeline(testData, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: false))
        {
            groups.Add(group);
        }

        // Assert
        Assert.True(groups.Count >= 1 && groups.Count <= 3, $"Expected 1-3 groups, got {groups.Count}");

        // At least one group should have records from both sources
        Assert.True(groups.Any(g =>
        {
            var dataSources = g.Records
                .Select(r => ((Dictionary<string, object>)r["_metadata"])["DataSourceId"])
                .Distinct()
                .Count();
            return dataSources > 1;
        }), "Should have at least one cross-source group");
    }

    #endregion

    #region Edge Cases

    [Fact]
    public async Task CreateGroups_EmptyGraph_ReturnsNoGroups()
    {
        // Arrange
        var emptyGraph = new MatchGraph();

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(emptyGraph, requireTransitive: false))
        {
            groups.Add(group);
        }

        // Assert
        Assert.Empty(groups);
    }

    [Fact]
    public async Task CreateGroups_WithMinGroupSize_FiltersSmallGroups()
    {
        // Arrange
        var config = new GroupingConfiguration { MinGroupSize = 2 };
        var groupingService = new EnhancedGroupingService(
            _groupingLoggerMock.Object,
            Options.Create(config));

        var records = new List<IDictionary<string, object>>
        {
            CreateRecord(0, "Single1", "single1@email.com", "123 St"),
            CreateRecord(1, "Pair1", "pair@email.com", "456 Ave"),
            CreateRecord(2, "Pair2", "pair@email.com", "456 Ave")
        };

        var matchDefinition = CreateExactMatchDefinitionForSingleSource("Email");
        var (_, matchGraph) = await RunFullPipeline(records, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: false))
        {
            groups.Add(group);
        }

        // Assert
        Assert.Single(groups); // Only the pair, singleton filtered out
        Assert.Equal(2, groups[0].Records.Count);
    }

    #endregion

    #region Group Metadata Tests

    [Fact]
    public async Task CreateGroups_AddsCorrectMetadata()
    {

        var _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();
        var _logger = new NullLogger<DataCleansingCommand>();

        IServiceCollection services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, _dbJobPath);



        services.AddMediatR(cfg => {
            cfg.RegisterServicesFromAssembly(typeof(DatabaseUpdateEventHandler).Assembly);
        });
        var _serviceProvider = services.BuildServiceProvider();

        var _dataStore = _serviceProvider.GetService<IDataStore>();

        // Arrange
        var records = new List<IDictionary<string, object>>
        {
            CreateRecord(0, "Test1", "test@email.com", "123 St"),
            CreateRecord(1, "Test2", "test@email.com", "123 St")
        };

        var matchDefinition = CreateExactMatchDefinitionForSingleSource("Email");
        var (_, matchGraph) = await RunFullPipeline(records, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var generatedGroup in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: false))
        {
            groups.Add(generatedGroup);
        }

        // Assert
        Assert.Single(groups);
        var group = groups[0];

        var tempCollection = "tempCollection";
        var jobId = await _dataStore.InitializeJobAsync(tempCollection);

        var dictionary = new Dictionary<string, object>
        {
            { "temp", group }
        };
        var value = new List<Dictionary<string, object>>()
        {
            dictionary

        };
        await _dataStore.InsertBatchAsync(jobId, value, tempCollection);

        var result = await _dataStore.GetPagedDataAsync(tempCollection, 1, 100);

        Assert.NotNull(group.Metadata);
        Assert.Contains("avg_match_score", group.Metadata.Keys);
        Assert.Contains("min_match_score", group.Metadata.Keys);
        Assert.Contains("max_match_score", group.Metadata.Keys);
        Assert.Contains("is_clique", group.Metadata.Keys);
        Assert.Contains("size", group.Metadata.Keys);

        Assert.Equal(2, group.Metadata["size"]);
    }

    #endregion

    #region Additional Tests for Better Coverage

    [Fact]
    public async Task CreateGroups_LargeClique_HandlesCorrectly()
    {
        // Arrange - Create a large clique where all records match
        var records = new List<IDictionary<string, object>>();
        for (int i = 0; i < 10; i++)
        {
            records.Add(CreateRecord(i, $"Person {i}", "shared@email.com", "Same Address"));
        }

        var matchDefinition = CreateExactMatchDefinitionForSingleSource("Email");
        var (_, matchGraph) = await RunFullPipeline(records, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: true))
        {
            groups.Add(group);
        }

        // Assert
        Assert.Single(groups);
        Assert.Equal(10, groups[0].Records.Count);
        Assert.True((bool)groups[0].Metadata["is_clique"]);
    }

    [Fact]
    public async Task CreateGroups_MixedMatchTypes_HandlesCorrectly()
    {
        // Arrange - Mix of exact and fuzzy matches
        var records = new List<IDictionary<string, object>>
        {
            CreateRecord(0, "John Smith", "john@email.com", "123 Main St"),
            CreateRecord(1, "John Smith", "john@email.com", "123 Main Street"), // Exact name & email, fuzzy address
            CreateRecord(2, "J. Smith", "john@email.com", "123 Main St"),       // Fuzzy name, exact email & address
        };

        var matchDefinition = CreateMixedMatchDefinition();
        var (_, matchGraph) = await RunFullPipeline(records, matchDefinition);

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(matchGraph, requireTransitive: false))
        {
            groups.Add(group);
        }

        // Assert
        Assert.NotEmpty(groups);
        var mainGroup = groups.FirstOrDefault(g => g.Records.Count > 1);
        Assert.NotNull(mainGroup);
    }

    #endregion

    #region Helper Methods

    private async Task<(List<ScoredMatchPair>, MatchGraph)> RunFullPipeline(
        List<IDictionary<string, object>> records,
        MatchDefinitionCollection matchDefinitions)
    {
        // Step 1: Index all data sources
        var distinctSources = records
            .Select(r => (Guid)((Dictionary<string, object>)r["_metadata"])["DataSourceId"])
            .Distinct()
            .ToList();

        foreach (var sourceId in distinctSources)
        {
            var sourceRecords = records.Where(r =>
                (Guid)((Dictionary<string, object>)r["_metadata"])["DataSourceId"] == sourceId).ToList();

            var indexingConfig = new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = sourceId == _dataSourceId ? "TestSource" : "TestSource2",
                FieldsToIndex = new List<string> { "Name", "Email", "Address" },
                UseInMemoryStore = true,
                InMemoryThreshold = 10000
            };

            await _indexer.IndexDataSourceAsync(
                sourceRecords.ToAsyncEnumerable(),
                indexingConfig,
                _progressTrackerMock.Object);
        }

        // Step 2: Generate candidates
        var candidates = new List<CandidatePair>();
        await foreach (var candidatePair in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinitions)) { candidates.Add(candidatePair); }

        // Step 3: Setup index mapper BEFORE comparison
        SetupIndexMapper(matchDefinitions);

        // Step 4: Compare and build graph
        var (resultsEnumerable, matchGraph) = await _comparisonService.CompareWithGraphAsync(
            candidates.ToAsyncEnumerable(),
            matchDefinitions,
            _indexMapperMock.Object);

        var results = new List<ScoredMatchPair>();
        await foreach (var result in resultsEnumerable)
        {
            results.Add(result);
        }

        return (results, matchGraph);
    }

    private IComparatorBuilder CreateRealComparatorBuilder()
    {
        var builder = new Mock<IComparatorBuilder>();
        var comparator = new Mock<IComparator>();

        // Return actual similarity scores based on string comparison
        comparator.Setup(x => x.Compare(It.IsAny<string>(), It.IsAny<string>()))
            .Returns((string s1, string s2) =>
            {
                if (string.IsNullOrEmpty(s1) || string.IsNullOrEmpty(s2))
                    return 0.0;

                // Exact match
                if (s1.Equals(s2, StringComparison.OrdinalIgnoreCase))
                    return 1.0;

                // Normalize strings
                var norm1 = s1.ToLower().Trim();
                var norm2 = s2.ToLower().Trim();

                // Check for substring containment
                if (norm1.Contains(norm2) || norm2.Contains(norm1))
                    return 0.85;

                // Simple character-based similarity
                var maxLen = Math.Max(s1.Length, s2.Length);
                var minLen = Math.Min(s1.Length, s2.Length);

                if (maxLen == 0) return 1.0;

                // Count matching characters
                int matches = 0;
                for (int i = 0; i < minLen; i++)
                {
                    if (char.ToLower(s1[i]) == char.ToLower(s2[i]))
                        matches++;
                }

                var similarity = (double)matches / maxLen;

                // Boost if strings start the same
                if (minLen > 3 && norm1.Substring(0, 3) == norm2.Substring(0, 3))
                    similarity = Math.Min(1.0, similarity + 0.2);

                return similarity;
            });

        builder.Setup(x => x.WithArgs(It.IsAny<Dictionary<ArgsValue, string>>()))
            .Returns(builder.Object);
        builder.Setup(x => x.Build()).Returns(comparator.Object);

        return builder.Object;
    }

    private MatchDefinitionCollection CreateFuzzyMatchDefinition()
    {
        var definitionId = Guid.NewGuid();
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        // Use LOW FastLevel for q-gram pre-filtering, higher Level for actual comparison
                        CreateFuzzyCriteria("Name", 0.5, "0.2", "0.7"),    // FastLevel 0.2, Level 0.7
                        CreateFuzzyCriteria("Email", 0.3, "0.2", "0.8"),   // FastLevel 0.2, Level 0.8
                        CreateFuzzyCriteria("Address", 0.2, "0.2", "0.6")  // FastLevel 0.2, Level 0.6
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateFuzzyMatchDefinitionForSingleSource()
    {
        var definitionId = Guid.NewGuid();
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        // Use LOW FastLevel for q-gram pre-filtering, higher Level for actual comparison
                        CreateFuzzyCriteriaForSingleSource("Name", 0.5, "0.2", "0.7"),    // FastLevel 0.2, Level 0.7
                        CreateFuzzyCriteriaForSingleSource("Email", 0.3, "0.2", "0.8"),   // FastLevel 0.2, Level 0.8
                        CreateFuzzyCriteriaForSingleSource("Address", 0.2, "0.2", "0.6")  // FastLevel 0.2, Level 0.6
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateExactMatchDefinition(string fieldName)
    {
        var definitionId = Guid.NewGuid();
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        new MatchCriteria
                        {
                            Id = Guid.NewGuid(),
                            FieldName = fieldName,
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 1.0,
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(_dataSourceId, "TestSource", fieldName),
                                new FieldMapping(_dataSource2Id, "TestSource2", fieldName)
                            }
                        }
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateExactMatchDefinitionForSingleSource(string fieldName)
    {
        var definitionId = Guid.NewGuid();
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        new MatchCriteria
                        {
                            Id = Guid.NewGuid(),
                            FieldName = fieldName,
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 1.0,
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(_dataSourceId, "TestSource", fieldName),
                            }
                        }
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateMultiFieldMatchDefinition()
    {
        var definitionId = Guid.NewGuid();
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        CreateExactCriteriaForSingleSource("Email", 1.0),
                    }
                },
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        CreateExactCriteriaForSingleSource("Address", 1.0)
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateCrossSourceMatchDefinition()
    {
        var definitionId = Guid.NewGuid();
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        new MatchCriteria
                        {
                            Id = Guid.NewGuid(),
                            FieldName = "Name",
                            MatchingType = MatchingType.Fuzzy,
                            DataType = CriteriaDataType.Text,
                            Weight = 0.7,
                            Arguments = new Dictionary<ArgsValue, string>
                            {
                                [ArgsValue.FastLevel] = "0.2",  // Low for q-gram
                                [ArgsValue.Level] = "0.7"        // Higher for actual comparison
                            },
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(_dataSourceId, "TestSource", "Name"),
                                new FieldMapping(_dataSource2Id, "TestSource2", "Name")
                            }
                        },
                        new MatchCriteria
                        {
                            Id = Guid.NewGuid(),
                            FieldName = "Email",
                            MatchingType = MatchingType.Fuzzy,
                            DataType = CriteriaDataType.Text,
                            Weight = 0.3,
                            Arguments = new Dictionary<ArgsValue, string>
                            {
                                [ArgsValue.FastLevel] = "0.2",  // Low for q-gram
                                [ArgsValue.Level] = "0.6"        // Higher for actual comparison
                            },
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(_dataSourceId, "TestSource", "Email"),
                                new FieldMapping(_dataSource2Id, "TestSource2", "Email")
                            }
                        }
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateMixedMatchDefinition()
    {
        var definitionId = Guid.NewGuid();
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = definitionId,
                    DataSourcePairId = Guid.NewGuid(),
                    Criteria = new List<MatchCriteria>
                    {
                        CreateExactCriteriaForSingleSource("Email", 0.4),
                        CreateFuzzyCriteriaForSingleSource("Name", 0.3, "0.2", "0.3"),    // FastLevel 0.2, Level 0.7
                        CreateFuzzyCriteriaForSingleSource("Address", 0.3, "0.2", "0.6")  // FastLevel 0.2, Level 0.6
                    }
                }
            }
        };
    }

    // Updated to support separate FastLevel and Level thresholds
    private MatchCriteria CreateFuzzyCriteria(string fieldName, double weight, string fastLevelThreshold, string levelThreshold)
    {
        return new MatchCriteria
        {
            Id = Guid.NewGuid(),
            FieldName = fieldName,
            MatchingType = MatchingType.Fuzzy,
            DataType = CriteriaDataType.Text,
            Weight = weight,
            Arguments = new Dictionary<ArgsValue, string>
            {
                [ArgsValue.FastLevel] = fastLevelThreshold,  // For q-gram pre-filtering
                [ArgsValue.Level] = levelThreshold            // For actual comparison scoring
            },
            FieldMappings = new List<FieldMapping>
            {
                new FieldMapping(_dataSourceId, "TestSource", fieldName),
                new FieldMapping(_dataSource2Id, "TestSource2", fieldName)
            }
        };
    }

    private MatchCriteria CreateFuzzyCriteriaForSingleSource(string fieldName, double weight, string fastLevelThreshold, string levelThreshold)
    {
        return new MatchCriteria
        {
            Id = Guid.NewGuid(),
            FieldName = fieldName,
            MatchingType = MatchingType.Fuzzy,
            DataType = CriteriaDataType.Text,
            Weight = weight,
            Arguments = new Dictionary<ArgsValue, string>
            {
                [ArgsValue.FastLevel] = fastLevelThreshold,  // For q-gram pre-filtering
                [ArgsValue.Level] = levelThreshold            // For actual comparison scoring
            },
            FieldMappings = new List<FieldMapping>
            {
                new FieldMapping(_dataSourceId, "TestSource", fieldName),
            }
        };
    }

    private MatchCriteria CreateExactCriteria(string fieldName, double weight)
    {
        return new MatchCriteria
        {
            Id = Guid.NewGuid(),
            FieldName = fieldName,
            MatchingType = MatchingType.Exact,
            DataType = CriteriaDataType.Text,
            Weight = weight,
            FieldMappings = new List<FieldMapping>
            {
                new FieldMapping(_dataSourceId, "TestSource", fieldName),
                new FieldMapping(_dataSource2Id, "TestSource2", fieldName)
            }
        };
    }

    private MatchCriteria CreateExactCriteriaForSingleSource(string fieldName, double weight)
    {
        return new MatchCriteria
        {
            Id = Guid.NewGuid(),
            FieldName = fieldName,
            MatchingType = MatchingType.Exact,
            DataType = CriteriaDataType.Text,
            Weight = weight,
            FieldMappings = new List<FieldMapping>
            {
                new FieldMapping(_dataSourceId, "TestSource", fieldName),
            }
        };
    }

    private void SetupIndexMapper(MatchDefinitionCollection definitions)
    {
        // Setup data source index mappings
        _indexMapperMock.Setup(x => x.TryGetDataSourceIndex(It.IsAny<Guid>(), out It.Ref<int>.IsAny))
            .Returns((Guid id, out int index) =>
            {
                if (id == _dataSourceId)
                {
                    index = 0;
                    return true;
                }
                else if (id == _dataSource2Id)
                {
                    index = 1;
                    return true;
                }
                index = -1;
                return false;
            });

        // Setup data source name mappings
        _indexMapperMock.Setup(x => x.TryGetDataSourceName(It.IsAny<Guid>(), out It.Ref<string>.IsAny))
            .Returns((Guid id, out string name) =>
            {
                if (id == _dataSourceId)
                {
                    name = "TestSource";
                    return true;
                }
                else if (id == _dataSource2Id)
                {
                    name = "TestSource2";
                    return true;
                }
                name = null;
                return false;
            });

        // Setup definition index mappings
        for (int i = 0; i < definitions.Definitions.Count; i++)
        {
            var localIndex = i;
            var defId = definitions.Definitions[localIndex].Id;
            _indexMapperMock.Setup(x => x.TryGetDefinitionIndex(defId, out It.Ref<int>.IsAny))
                .Returns((Guid id, out int index) =>
                {
                    index = localIndex;
                    return true;
                });
        }
    }

    #endregion

    public void Dispose()
    {
        _indexer?.Dispose();
        _comparisonService?.DisposeAsync().AsTask().Wait();
    }
}