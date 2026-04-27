using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Xunit;

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Comprehensive unit tests for ProductionQGramIndexer
/// </summary>
public class ProductionQGramIndexerTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<ILogger<ProductionQGramIndexer>> _mockLogger;
    private readonly QGramIndexerOptions _options;
    private readonly ProductionQGramIndexer _indexer;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker;

    public ProductionQGramIndexerTests(ITestOutputHelper output)
    {
        _output = output;
        _mockLogger = new Mock<ILogger<ProductionQGramIndexer>>();
        _options = new QGramIndexerOptions
        {
            QGramSize = 3,
            DefaultInMemoryThreshold = 1000,
            EnableCompression = false, // Disable for testing
            MaxParallelism = 2,
            CandidateChannelCapacity = 100
        };

        var optionsMock = new Mock<IOptions<QGramIndexerOptions>>();
        optionsMock.Setup(x => x.Value).Returns(_options);

        _indexer = new ProductionQGramIndexer(optionsMock.Object, _mockLogger.Object);
        _mockProgressTracker = new Mock<IStepProgressTracker>();
    }

    #region Test Data Generation

    private async IAsyncEnumerable<IDictionary<string, object>> GenerateCustomerDataAsync(int count, int startId = 1)
    {
        for (int i = 0; i < count; i++)
        {
            yield return new Dictionary<string, object>
            {
                ["Id"] = $"CUST{startId + i:D3}",
                ["Name"] = $"Customer {startId + i}",
                ["Email"] = $"customer{startId + i}@example.com",
                ["Phone"] = $"555-{(startId + i):D3}-{(startId + i * 2):D4}",
                ["Address"] = $"{startId + i} Main St, City"
            };
        }
    }

    private async IAsyncEnumerable<IDictionary<string, object>> GenerateOrderDataAsync(int count, int startId = 1)
    {
        for (int i = 0; i < count; i++)
        {
            yield return new Dictionary<string, object>
            {
                ["Id"] = $"ORD{startId + i:D3}",
                ["CustomerName"] = $"Customer {startId + i}",
                ["CustomerEmail"] = $"customer{startId + i}@example.com",
                ["CustomerPhone"] = $"555-{(startId + i):D3}-{(startId + i * 2):D4}",
                ["OrderAmount"] = (startId + i) * 10.5m,
                ["OrderDate"] = DateTime.Now.AddDays(-i).ToString("yyyy-MM-dd")
            };
        }
    }

    private async IAsyncEnumerable<IDictionary<string, object>> GenerateCustomerDataWithDuplicatesAsync(int count)
    {
        // Generate base records
        for (int i = 1; i <= count / 2; i++)
        {
            yield return new Dictionary<string, object>
            {
                ["Id"] = $"CUST{i:D3}",
                ["Name"] = $"John Smith {i}",
                ["Email"] = $"john.smith{i}@example.com",
                ["Phone"] = $"555-123-{i:D4}"
            };
        }

        // Generate near-duplicates
        for (int i = 1; i <= count / 2; i++)
        {
            yield return new Dictionary<string, object>
            {
                ["Id"] = $"CUST{(count / 2) + i:D3}",
                ["Name"] = $"Jon Smith {i}", // Slight variation
                ["Email"] = $"john.smith{i}@gmail.com", // Different domain
                ["Phone"] = $"555-123-{i:D4}" // Same phone
            };
        }
    }

    private MatchDefinitionCollection CreateTestMatchDefinitions(Guid customerSourceId, Guid orderSourceId)
    {
        var collection = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Test Match Definitions"
        };

        // Definition 1: Email Exact Match
        var emailMatchDef = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            ProjectId = collection.ProjectId,
            JobId = collection.JobId,
            Name = "Email Exact Match"
        };

        var emailCriteria = new MatchCriteria
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Exact,
            DataType = CriteriaDataType.Text,
            Weight = 1.0
        };
        emailCriteria.FieldMappings.Add(new FieldMapping(customerSourceId, "Customers", "Email"));
        emailCriteria.FieldMappings.Add(new FieldMapping(orderSourceId, "Orders", "CustomerEmail"));
        emailMatchDef.Criteria.Add(emailCriteria);

        // Definition 2: Name Fuzzy Match
        var nameMatchDef = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            ProjectId = collection.ProjectId,
            JobId = collection.JobId,
            Name = "Name Fuzzy Match"
        };

        var nameCriteria = new MatchCriteria
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Fuzzy,
            DataType = CriteriaDataType.Text,
            Weight = 1.0
        };
        nameCriteria.Arguments[ArgsValue.FastLevel] = "0.7";
        nameCriteria.FieldMappings.Add(new FieldMapping(customerSourceId, "Customers", "Name"));
        nameCriteria.FieldMappings.Add(new FieldMapping(orderSourceId, "Orders", "CustomerName"));
        nameMatchDef.Criteria.Add(nameCriteria);

        collection.Definitions.Add(emailMatchDef);
        collection.Definitions.Add(nameMatchDef);

        return collection;
    }

    private MatchDefinitionCollection CreateDeduplicationMatchDefinition(Guid customerSourceId)
    {
        var collection = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Customer Deduplication"
        };

        var deduplicationDef = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            ProjectId = collection.ProjectId,
            JobId = collection.JobId,
            Name = "Customer Dedup"
        };

        var nameCriteria = new MatchCriteria
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Fuzzy,
            DataType = CriteriaDataType.Text,
            Weight = 0.6
        };
        nameCriteria.Arguments[ArgsValue.FastLevel] = "0.8";
        nameCriteria.FieldMappings.Add(new FieldMapping(customerSourceId, "Customers", "Name"));

        var emailCriteria = new MatchCriteria
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Fuzzy,
            DataType = CriteriaDataType.Text,
            Weight = 0.4
        };
        emailCriteria.Arguments[ArgsValue.FastLevel] = "0.9";
        emailCriteria.FieldMappings.Add(new FieldMapping(customerSourceId, "Customers", "Email"));

        deduplicationDef.Criteria.Add(nameCriteria);
        deduplicationDef.Criteria.Add(emailCriteria);
        collection.Definitions.Add(deduplicationDef);

        return collection;
    }

    #endregion

    #region Basic Functionality Tests

    [Fact]
    public async Task IndexDataSourceAsync_WithValidData_ShouldIndexSuccessfully()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var config = new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "TestCustomers",
            FieldsToIndex = new List<string> { "Name", "Email", "Phone" },
            UseInMemoryStore = true,
            InMemoryThreshold = 1000
        };

        var testData = GenerateCustomerDataAsync(10);

        // Act
        var result = await _indexer.IndexDataSourceAsync(testData, config, _mockProgressTracker.Object);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(sourceId, result.DataSourceId);
        Assert.Equal("TestCustomers", result.DataSourceName);
        Assert.Equal(10, result.ProcessedRecords);
        Assert.Contains("Name", result.IndexedFields);
        Assert.Contains("Email", result.IndexedFields);
        Assert.Contains("Phone", result.IndexedFields);
        Assert.True(result.IndexingDuration > TimeSpan.Zero);

        _output.WriteLine($"Indexed {result.ProcessedRecords} records in {result.IndexingDuration}");
    }

    [Fact]
    public async Task IndexDataSourceAsync_WithEmptyData_ShouldHandleGracefully()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var config = new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "EmptySource",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        };

        var emptyData = AsyncEnumerable.Empty<IDictionary<string, object>>();

        // Act
        var result = await _indexer.IndexDataSourceAsync(emptyData, config, _mockProgressTracker.Object);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(0, result.ProcessedRecords);
        Assert.Equal(sourceId, result.DataSourceId);
    }

    [Fact]
    public async Task IndexDataSourceAsync_WithNullConfig_ShouldThrowArgumentNullException()
    {
        // Arrange
        var testData = GenerateCustomerDataAsync(5);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            _indexer.IndexDataSourceAsync(testData, null, _mockProgressTracker.Object));
    }

    #endregion

    #region Record Retrieval Tests

    [Fact]
    public async Task GetRecordAsync_WithValidRowNumber_ShouldReturnCorrectRecord()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var config = new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "TestSource",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        };

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(5), config, _mockProgressTracker.Object);

        // Act
        var record = await _indexer.GetRecordAsync(sourceId, 2);

        // Assert
        Assert.NotNull(record);
        Assert.Equal("CUST003", record["Id"]);
        Assert.Equal("Customer 3", record["Name"]);
        Assert.Equal("customer3@example.com", record["Email"]);
    }

    [Fact]
    public async Task GetRecordAsync_WithInvalidRowNumber_ShouldReturnNull()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var config = new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "TestSource",
            FieldsToIndex = new List<string> { "Name" },
            UseInMemoryStore = true
        };

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(3), config, _mockProgressTracker.Object);

        // Act
        var record = await _indexer.GetRecordAsync(sourceId, 10);

        // Assert
        Assert.Null(record);
    }

    [Fact]
    public async Task GetRecordsAsync_WithValidRowNumbers_ShouldReturnCorrectRecords()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var config = new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "TestSource",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        };

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(5), config, _mockProgressTracker.Object);

        // Act
        var records = await _indexer.GetRecordsAsync(sourceId, new[] { 0, 2, 4 });

        // Assert
        Assert.NotNull(records);
        Assert.Equal(3, records.Count);
        Assert.Equal("CUST001", records[0]["Id"]);
        Assert.Equal("CUST003", records[1]["Id"]);
        Assert.Equal("CUST005", records[2]["Id"]);
    }

    #endregion

    #region Cross-Source Record Linkage Tests

    [Fact]
    public async Task GenerateCandidatesFromMatchDefinitionsAsync_WithCrossSourceData_ShouldFindMatches()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();
        var orderSourceId = Guid.NewGuid();

        // Index customer data
        var customerConfig = new DataSourceIndexingConfig
        {
            DataSourceId = customerSourceId,
            DataSourceName = "Customers",
            FieldsToIndex = new List<string> { "Name", "Email", "Phone" },
            UseInMemoryStore = true
        };
        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(5), customerConfig, _mockProgressTracker.Object);

        // Index order data
        var orderConfig = new DataSourceIndexingConfig
        {
            DataSourceId = orderSourceId,
            DataSourceName = "Orders",
            FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail", "CustomerPhone" },
            UseInMemoryStore = true
        };
        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(5), orderConfig, _mockProgressTracker.Object);

        var matchDefinitions = CreateTestMatchDefinitions(customerSourceId, orderSourceId);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinitions))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.NotEmpty(candidates);

        // Verify match definition tracking
        foreach (var candidate in candidates)
        {
            Assert.NotEmpty(candidate.MatchDefinitionIds);
            Assert.True(candidate.MatchDefinitionIds.Count <= 2); // Max 2 definitions in our test

            // Verify cross-source pairing
            Assert.NotEqual(candidate.DataSource1Id, candidate.DataSource2Id);
            Assert.True(candidate.DataSource1Id == customerSourceId || candidate.DataSource1Id == orderSourceId);
            Assert.True(candidate.DataSource2Id == customerSourceId || candidate.DataSource2Id == orderSourceId);
        }

        _output.WriteLine($"Found {candidates.Count} cross-source candidate pairs");

        // Test record retrieval
        var firstCandidate = candidates.First();
        var (record1, record2) = await firstCandidate.GetRecordsAsync();
        Assert.NotNull(record1);
        Assert.NotNull(record2);

        _output.WriteLine($"Sample match: {record1.GetValueOrDefault("Name")} <-> {record2.GetValueOrDefault("CustomerName")}");
    }

    [Fact]
    public async Task GenerateCandidatesFromMatchDefinitionsAsync_WithMultipleDefinitions_ShouldTrackDefinitionIds()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();
        var orderSourceId = Guid.NewGuid();

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = customerSourceId,
            DataSourceName = "Customers",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = orderSourceId,
            DataSourceName = "Orders",
            FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        var matchDefinitions = CreateTestMatchDefinitions(customerSourceId, orderSourceId);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinitions))
        {
            candidates.Add(candidate);
        }

        // Assert
        var candidatesWithMultipleDefinitions = candidates.Where(c => c.MatchDefinitionIds.Count > 1).ToList();

        if (candidatesWithMultipleDefinitions.Any())
        {
            _output.WriteLine($"Found {candidatesWithMultipleDefinitions.Count} candidates matching multiple definitions");

            foreach (var candidate in candidatesWithMultipleDefinitions.Take(3))
            {
                _output.WriteLine($"Candidate has {candidate.MatchDefinitionIds.Count} definitions: {candidate.MatchDefinitionIdsString}");
                Assert.Contains(',', candidate.MatchDefinitionIdsString); // Should be comma-separated
            }
        }

        // Verify all candidates have at least one definition
        Assert.All(candidates, c => Assert.NotEmpty(c.MatchDefinitionIds));
    }

    [Fact]
    public async Task GenerateCrossSourceCandidatesAsync_LegacyMethod_ShouldWork()
    {
        // Arrange
        var sourceId1 = Guid.NewGuid();
        var sourceId2 = Guid.NewGuid();

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = sourceId1,
            DataSourceName = "Source1",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = sourceId2,
            DataSourceName = "Source2",
            FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCrossSourceCandidatesAsync(
            sourceId1, sourceId2, new List<string> { "Name", "Email" }, 0.1))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.NotEmpty(candidates);
        Assert.All(candidates, c =>
        {
            Assert.NotEqual(c.DataSource1Id, c.DataSource2Id);
            Assert.True(c.DataSource1Id == sourceId1 || c.DataSource1Id == sourceId2);
        });

        _output.WriteLine($"Legacy method found {candidates.Count} candidates");
    }

    #endregion

    #region Within-Source Deduplication Tests

    [Fact]
    public async Task GenerateCandidatesFromMatchDefinitionsAsync_WithDeduplicationData_ShouldFindDuplicates()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();

        var customerConfig = new DataSourceIndexingConfig
        {
            DataSourceId = customerSourceId,
            DataSourceName = "CustomersWithDuplicates",
            FieldsToIndex = new List<string> { "Name", "Email", "Phone" },
            UseInMemoryStore = true
        };

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataWithDuplicatesAsync(10), customerConfig, _mockProgressTracker.Object);

        var matchDefinitions = CreateDeduplicationMatchDefinition(customerSourceId);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinitions))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.NotEmpty(candidates);

        // Verify within-source pairing
        foreach (var candidate in candidates)
        {
            Assert.Equal(candidate.DataSource1Id, candidate.DataSource2Id); // Same source
            Assert.Equal(customerSourceId, candidate.DataSource1Id);
            Assert.NotEqual(candidate.Row1Number, candidate.Row2Number); // Different rows
        }

        _output.WriteLine($"Found {candidates.Count} potential duplicate pairs");

        // Test record retrieval for duplicates
        var firstCandidate = candidates.First();
        var (record1, record2) = await firstCandidate.GetRecordsAsync();
        Assert.NotNull(record1);
        Assert.NotNull(record2);

        _output.WriteLine($"Sample duplicate: '{record1.GetValueOrDefault("Name")}' vs '{record2.GetValueOrDefault("Name")}'");
    }

    [Fact]
    public async Task GenerateWithinSourceCandidatesAsync_LegacyMethod_ShouldFindDuplicates()
    {
        // Arrange
        var sourceId = Guid.NewGuid();

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataWithDuplicatesAsync(6), new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "TestSource",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateWithinSourceCandidatesAsync(
            sourceId, new List<string> { "Name", "Email" }, 0.3))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.NotEmpty(candidates);
        Assert.All(candidates, c =>
        {
            Assert.Equal(c.DataSource1Id, c.DataSource2Id);
            Assert.Equal(sourceId, c.DataSource1Id);
            Assert.NotEqual(c.Row1Number, c.Row2Number);
        });

        _output.WriteLine($"Legacy deduplication found {candidates.Count} candidates");
    }

    #endregion

    #region Multi-Source Complex Scenarios

    [Fact]
    public async Task GenerateCandidatesFromMatchDefinitionsAsync_WithThreeSources_ShouldHandleComplexScenario()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();
        var orderSourceId = Guid.NewGuid();
        var accountSourceId = Guid.NewGuid();

        // Index three different sources
        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = customerSourceId,
            DataSourceName = "Customers",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = orderSourceId,
            DataSourceName = "Orders",
            FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Generate account data (similar to customer data)
        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = accountSourceId,
            DataSourceName = "Accounts",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Create multi-source match definition
        var multiSourceDef = CreateMultiSourceMatchDefinition(customerSourceId, orderSourceId, accountSourceId);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(multiSourceDef))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.NotEmpty(candidates);

        var uniqueSourcePairs = candidates
            .Select(c => (c.DataSource1Id, c.DataSource2Id))
            .Distinct()
            .Count();

        Assert.True(uniqueSourcePairs >= 2); // Should have multiple source combinations

        _output.WriteLine($"Multi-source scenario generated {candidates.Count} candidates across {uniqueSourcePairs} source pairs");
    }

    private MatchDefinitionCollection CreateMultiSourceMatchDefinition(Guid sourceId1, Guid sourceId2, Guid sourceId3)
    {
        var collection = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Multi-Source Match"
        };

        var definition = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            ProjectId = collection.ProjectId,
            JobId = collection.JobId,
            Name = "Multi-Source Email Match"
        };

        var criteria = new MatchCriteria
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Exact,
            DataType = CriteriaDataType.Text,
            Weight = 1.0
        };

        // Add mappings for all three sources
        criteria.FieldMappings.Add(new FieldMapping(sourceId1, "Source1", "Email"));
        criteria.FieldMappings.Add(new FieldMapping(sourceId2, "Source2", "CustomerEmail"));
        criteria.FieldMappings.Add(new FieldMapping(sourceId3, "Source3", "Email"));

        definition.Criteria.Add(criteria);
        collection.Definitions.Add(definition);

        return collection;
    }

    #endregion

    #region Statistics and Monitoring Tests

    [Fact]
    public async Task GetStatistics_AfterIndexing_ShouldReturnCorrectStats()
    {
        // Arrange
        var sourceId1 = Guid.NewGuid();
        var sourceId2 = Guid.NewGuid();

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(5), new DataSourceIndexingConfig
        {
            DataSourceId = sourceId1,
            DataSourceName = "Customers",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = sourceId2,
            DataSourceName = "Orders",
            FieldsToIndex = new List<string> { "CustomerName" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Act
        var stats = _indexer.GetStatistics();

        // Assert
        Assert.NotNull(stats);
        Assert.Equal(2, stats.TotalDataSources);
        Assert.Equal(8, stats.TotalRecords); // 5 + 3
        Assert.True(stats.TotalIndexedFields > 0);
        Assert.True(stats.IndexSizeBytes > 0);
        Assert.Equal(2, stats.DataSources.Count);

        var customerStats = stats.DataSources.FirstOrDefault(ds => ds.DataSourceId == sourceId1);
        Assert.NotNull(customerStats);
        Assert.Equal(5, customerStats.RecordCount);

        var orderStats = stats.DataSources.FirstOrDefault(ds => ds.DataSourceId == sourceId2);
        Assert.NotNull(orderStats);
        Assert.Equal(3, orderStats.RecordCount);

        _output.WriteLine($"Statistics: {stats.TotalRecords} records, {stats.IndexSizeMB} index, {stats.TotalStorageSizeMB} storage");
    }

    [Fact]
    public void ClearCaches_ShouldExecuteWithoutError()
    {
        // Act & Assert - Should not throw
        _indexer.ClearCaches();

        // Verify logger was called
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Caches cleared")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.Once);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task GenerateCandidatesFromMatchDefinitionsAsync_WithNullMatchDefinitions_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(null))
            {
                // Should not reach here
            }
        });
    }

    [Fact]
    public async Task GenerateCandidatesFromMatchDefinitionsAsync_WithEmptyMatchDefinitions_ShouldReturnEmpty()
    {
        // Arrange
        var emptyDefinitions = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Empty",
            Definitions = new List<MatchDefinition>()
        };

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(emptyDefinitions))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.Empty(candidates);
    }

    [Fact]
    public async Task GenerateCrossSourceCandidatesAsync_WithNonExistentSources_ShouldReturnEmpty()
    {
        // Arrange
        var nonExistentSource1 = Guid.NewGuid();
        var nonExistentSource2 = Guid.NewGuid();

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCrossSourceCandidatesAsync(
            nonExistentSource1, nonExistentSource2, new List<string> { "Name" }))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.Empty(candidates);
    }

    [Fact]
    public async Task GetRecordAsync_WithNonExistentSource_ShouldReturnNull()
    {
        // Arrange
        var nonExistentSource = Guid.NewGuid();

        // Act
        var record = await _indexer.GetRecordAsync(nonExistentSource, 0);

        // Assert
        Assert.Null(record);
    }

    #endregion

    #region Performance and Stress Tests

    [Fact]
    public async Task IndexDataSourceAsync_WithLargeDataset_ShouldHandleEfficiently()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var config = new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "LargeDataset",
            FieldsToIndex = new List<string> { "Name", "Email", "Phone" },
            UseInMemoryStore = true,
            InMemoryThreshold = 2000
        };

        var startTime = DateTime.UtcNow;

        // Act
        var result = await _indexer.IndexDataSourceAsync(
            GenerateCustomerDataAsync(1000), config, _mockProgressTracker.Object);

        var duration = DateTime.UtcNow - startTime;

        // Assert
        Assert.Equal(1000, result.ProcessedRecords);
        Assert.True(duration < TimeSpan.FromSeconds(10)); // Should complete within 10 seconds

        _output.WriteLine($"Indexed {result.ProcessedRecords} records in {duration.TotalMilliseconds:F0}ms");

        // Verify we can still retrieve records efficiently
        var retrievalStart = DateTime.UtcNow;
        var record = await _indexer.GetRecordAsync(sourceId, 500);
        var retrievalDuration = DateTime.UtcNow - retrievalStart;

        Assert.NotNull(record);
        Assert.True(retrievalDuration < TimeSpan.FromMilliseconds(100)); // Should be very fast
    }

    [Fact]
    public async Task GenerateCandidatesFromMatchDefinitionsAsync_WithLargeDataset_ShouldHandleEfficiently()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();
        var orderSourceId = Guid.NewGuid();

        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(100), new DataSourceIndexingConfig
        {
            DataSourceId = customerSourceId,
            DataSourceName = "Customers",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(100), new DataSourceIndexingConfig
        {
            DataSourceId = orderSourceId,
            DataSourceName = "Orders",
            FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        var matchDefinitions = CreateTestMatchDefinitions(customerSourceId, orderSourceId);
        var startTime = DateTime.UtcNow;

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinitions))
        {
            candidates.Add(candidate);
            if (candidates.Count >= 1000) break; // Limit for test performance
        }

        var duration = DateTime.UtcNow - startTime;

        // Assert
        Assert.NotEmpty(candidates);
        Assert.True(duration < TimeSpan.FromSeconds(30)); // Should complete within 30 seconds

        _output.WriteLine($"Generated {candidates.Count} candidates in {duration.TotalMilliseconds:F0}ms");
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task FullWorkflow_IndexThenMatchThenRetrieve_ShouldWorkEndToEnd()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();
        var orderSourceId = Guid.NewGuid();

        // Step 1: Index both data sources
        await _indexer.IndexDataSourceAsync(GenerateCustomerDataAsync(5), new DataSourceIndexingConfig
        {
            DataSourceId = customerSourceId,
            DataSourceName = "Customers",
            FieldsToIndex = new List<string> { "Name", "Email", "Phone" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(5), new DataSourceIndexingConfig
        {
            DataSourceId = orderSourceId,
            DataSourceName = "Orders",
            FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail", "CustomerPhone" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Step 2: Create match definitions
        var matchDefinitions = CreateTestMatchDefinitions(customerSourceId, orderSourceId);

        // Step 3: Generate candidates
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinitions))
        {
            candidates.Add(candidate);
        }

        // Step 4: Retrieve records for each candidate
        var retrievedPairs = new List<(IDictionary<string, object>, IDictionary<string, object>)>();
        foreach (var candidate in candidates.Take(3)) // Test first 3 for performance
        {
            var (record1, record2) = await candidate.GetRecordsAsync();
            retrievedPairs.Add((record1, record2));
        }

        // Step 5: Get comprehensive statistics
        var stats = _indexer.GetStatistics();

        // Assert
        Assert.NotEmpty(candidates);
        Assert.NotEmpty(retrievedPairs);
        Assert.Equal(2, stats.TotalDataSources);
        Assert.Equal(10, stats.TotalRecords);

        // Verify match definition tracking works
        Assert.All(candidates, c => Assert.NotEmpty(c.MatchDefinitionIds));

        // Verify records were retrieved correctly
        Assert.All(retrievedPairs, pair =>
        {
            Assert.NotNull(pair.Item1);
            Assert.NotNull(pair.Item2);
        });

        _output.WriteLine($"End-to-end test: {candidates.Count} candidates, {retrievedPairs.Count} records retrieved");
        _output.WriteLine($"Statistics: {stats.TotalRecords} records across {stats.TotalDataSources} sources");
    }

    [Fact]
    public async Task MixedScenario_CrossSourceAndDeduplication_ShouldHandleBoth()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();
        var orderSourceId = Guid.NewGuid();

        // Index customers with duplicates
        await _indexer.IndexDataSourceAsync(GenerateCustomerDataWithDuplicatesAsync(6), new DataSourceIndexingConfig
        {
            DataSourceId = customerSourceId,
            DataSourceName = "Customers",
            FieldsToIndex = new List<string> { "Name", "Email", "Phone" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Index orders
        await _indexer.IndexDataSourceAsync(GenerateOrderDataAsync(3), new DataSourceIndexingConfig
        {
            DataSourceId = orderSourceId,
            DataSourceName = "Orders",
            FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail", "CustomerPhone" },
            UseInMemoryStore = true
        }, _mockProgressTracker.Object);

        // Create mixed match definitions (both cross-source and deduplication)
        var mixedDefinitions = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Mixed Scenario"
        };

        // Add cross-source definition
        var crossSourceDef = CreateTestMatchDefinitions(customerSourceId, orderSourceId).Definitions.First();
        mixedDefinitions.Definitions.Add(crossSourceDef);

        // Add deduplication definition
        var deduplicationDef = CreateDeduplicationMatchDefinition(customerSourceId).Definitions.First();
        mixedDefinitions.Definitions.Add(deduplicationDef);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(mixedDefinitions))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.NotEmpty(candidates);

        var crossSourceCandidates = candidates.Where(c => c.DataSource1Id != c.DataSource2Id).ToList();
        var withinSourceCandidates = candidates.Where(c => c.DataSource1Id == c.DataSource2Id).ToList();

        Assert.NotEmpty(crossSourceCandidates);
        Assert.NotEmpty(withinSourceCandidates);

        _output.WriteLine($"Mixed scenario: {crossSourceCandidates.Count} cross-source, {withinSourceCandidates.Count} within-source candidates");

        // Verify match definition tracking
        Assert.All(candidates, c => Assert.NotEmpty(c.MatchDefinitionIds));
    }

    #endregion

    #region Cleanup and Disposal Tests

    [Fact]
    public void Dispose_ShouldCleanupResourcesProperly()
    {
        // Arrange
        var indexer = new ProductionQGramIndexer(
            Options.Create(_options),
            _mockLogger.Object);

        // Act
        indexer.Dispose();

        // Assert - Should not throw
        // Verify disposal doesn't cause issues
        Assert.True(true); // If we get here, disposal worked

        // Trying to use after disposal should handle gracefully
        var stats = indexer.GetStatistics();
        Assert.NotNull(stats); // Should return empty stats without throwing
    }

    #endregion

    public void Dispose()
    {
        _indexer?.Dispose();
    }
}

#region Helper Classes and Extensions

/// <summary>
/// Extension methods for test assertions
/// </summary>
public static class TestExtensions
{
    public static object GetValueOrDefault(this IDictionary<string, object> dictionary, string key)
    {
        return dictionary?.TryGetValue(key, out var value) == true ? value : null;
    }
}

/// <summary>
/// Test-specific options factory
/// </summary>
public static class TestOptionsFactory
{
    public static QGramIndexerOptions CreateTestOptions()
    {
        return new QGramIndexerOptions
        {
            QGramSize = 3,
            DefaultInMemoryThreshold = 1000,
            EnableCompression = false,
            MaxParallelism = 2,
            CandidateChannelCapacity = 100,
            DiskBufferSize = 64 * 1024,
            IndexSaveFrequency = 1000
        };
    }
}

#endregion
