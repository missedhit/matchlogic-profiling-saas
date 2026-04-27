using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Complex test scenarios for ProductionQGramIndexer focusing on threshold validation,
/// partial matches, and criteria rejection cases
/// </summary>
public class ProductionQGramIndexerTests_Complex : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<ILogger<ProductionQGramIndexer>> _mockLogger;
    private readonly QGramIndexerOptions _options;
    private readonly ProductionQGramIndexer _indexer;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker;

    public ProductionQGramIndexerTests_Complex(ITestOutputHelper output)
    {
        _output = output;
        _mockLogger = new Mock<ILogger<ProductionQGramIndexer>>();
        _options = new QGramIndexerOptions
        {
            QGramSize = 3,
            DefaultInMemoryThreshold = 1000,
            EnableCompression = false,
            MaxParallelism = 2,
            CandidateChannelCapacity = 100
        };

        var optionsMock = new Mock<IOptions<QGramIndexerOptions>>();
        optionsMock.Setup(x => x.Value).Returns(_options);

        _indexer = new ProductionQGramIndexer(optionsMock.Object, _mockLogger.Object);
        _mockProgressTracker = new Mock<IStepProgressTracker>();
    }

    #region Test Data Generators for Complex Scenarios

    /// <summary>
    /// Generates customer data with varying degrees of similarity for threshold testing
    /// </summary>
    private async IAsyncEnumerable<IDictionary<string, object>> GenerateCustomerDataWithVaryingSimilarity()
    {
        // Exact matches
        yield return new Dictionary<string, object>
        {
            ["Id"] = "CUST001",
            ["Name"] = "John Smith",
            ["Email"] = "john.smith@example.com",
            ["Phone"] = "555-1234"
        };

        // Near match (high similarity ~0.85)
        yield return new Dictionary<string, object>
        {
            ["Id"] = "CUST002",
            ["Name"] = "Jon Smith",  // One character difference
            ["Email"] = "jon.smith@example.com",
            ["Phone"] = "555-1234"
        };

        // Medium similarity (~0.6)
        yield return new Dictionary<string, object>
        {
            ["Id"] = "CUST003",
            ["Name"] = "John Smyth",  // Different spelling
            ["Email"] = "j.smyth@example.com",
            ["Phone"] = "555-1235"
        };

        // Low similarity (~0.3)
        yield return new Dictionary<string, object>
        {
            ["Id"] = "CUST004",
            ["Name"] = "Jane Doe",
            ["Email"] = "jane.doe@different.com",
            ["Phone"] = "999-8888"
        };

        // No similarity
        yield return new Dictionary<string, object>
        {
            ["Id"] = "CUST005",
            ["Name"] = "Robert Williams",
            ["Email"] = "robert@company.org",
            ["Phone"] = "777-9999"
        };

        // Edge cases
        yield return new Dictionary<string, object>
        {
            ["Id"] = "CUST006",
            ["Name"] = "",  // Empty name
            ["Email"] = "empty@example.com",
            ["Phone"] = "555-0000"
        };

        yield return new Dictionary<string, object>
        {
            ["Id"] = "CUST007",
            ["Name"] = "A",  // Single character
            ["Email"] = "a@example.com",
            ["Phone"] = "1"
        };
    }

    /// <summary>
    /// Generates order data that partially matches customer data
    /// </summary>
    private async IAsyncEnumerable<IDictionary<string, object>> GenerateOrderDataWithPartialMatches()
    {
        // Exact match on all fields
        yield return new Dictionary<string, object>
        {
            ["Id"] = "ORD001",
            ["CustomerName"] = "John Smith",
            ["CustomerEmail"] = "john.smith@example.com",
            ["CustomerPhone"] = "555-1234"
        };

        // Match on email only, different name
        yield return new Dictionary<string, object>
        {
            ["Id"] = "ORD002",
            ["CustomerName"] = "Jonathan Schmidt",  // Different name
            ["CustomerEmail"] = "john.smith@example.com",  // Same email
            ["CustomerPhone"] = "555-9999"  // Different phone
        };

        // Match on name only, different email
        yield return new Dictionary<string, object>
        {
            ["Id"] = "ORD003",
            ["CustomerName"] = "John Smith",  // Same name
            ["CustomerEmail"] = "different@email.com",  // Different email
            ["CustomerPhone"] = "444-5555"
        };

        // Near match on name, exact on email
        yield return new Dictionary<string, object>
        {
            ["Id"] = "ORD004",
            ["CustomerName"] = "Jon Smith",  // Near match
            ["CustomerEmail"] = "jon.smith@example.com",  // Exact match
            ["CustomerPhone"] = "555-1234"
        };

        // No matches at all
        yield return new Dictionary<string, object>
        {
            ["Id"] = "ORD005",
            ["CustomerName"] = "Alice Cooper",
            ["CustomerEmail"] = "alice@rock.com",
            ["CustomerPhone"] = "666-6666"
        };
    }

    /// <summary>
    /// Generates data specifically for boundary threshold testing
    /// </summary>
    private async IAsyncEnumerable<IDictionary<string, object>> GenerateThresholdBoundaryData()
    {
        // Base record
        yield return new Dictionary<string, object>
        {
            ["Id"] = "BASE001",
            ["Name"] = "Alexander Hamilton",
            ["Email"] = "alexander.hamilton@usa.gov"
        };

        // ~0.9 similarity
        yield return new Dictionary<string, object>
        {
            ["Id"] = "SIM90",
            ["Name"] = "Alexander Hamiltn",  // One character missing
            ["Email"] = "alexander.hamilton@usa.gov"
        };

        // ~0.8 similarity
        yield return new Dictionary<string, object>
        {
            ["Id"] = "SIM80",
            ["Name"] = "Alexnder Hamilton",  // Missing 'a'
            ["Email"] = "alexander.hamilton@usa.gov"
        };

        // ~0.7 similarity
        yield return new Dictionary<string, object>
        {
            ["Id"] = "SIM70",
            ["Name"] = "Alex Hamilton",  // Shortened first name
            ["Email"] = "alex.hamilton@usa.gov"
        };

        // ~0.6 similarity
        yield return new Dictionary<string, object>
        {
            ["Id"] = "SIM60",
            ["Name"] = "A. Hamilton",  // Abbreviated
            ["Email"] = "a.hamilton@usa.gov"
        };

        // ~0.5 similarity
        yield return new Dictionary<string, object>
        {
            ["Id"] = "SIM50",
            ["Name"] = "Hamilton",  // Last name only
            ["Email"] = "hamilton@usa.gov"
        };
    }

    #endregion

    #region Exact Matching Validation Tests

    [Fact]
    public async Task ExactMatching_WithNearPerfectMatch_ShouldMatch()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var config = new DataSourceIndexingConfig
        {
            DataSourceId = sourceId,
            DataSourceName = "ExactMatchTest",
            FieldsToIndex = new List<string> { "Name", "Email" },
            UseInMemoryStore = true
        };

        // Index data with exact and near-exact matches
        var testData = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "1", ["Name"] = "John Smith", ["Email"] = "john@example.com" },
            new() { ["Id"] = "2", ["Name"] = "John Smith", ["Email"] = "john@example.com" }, // Exact duplicate
            new() { ["Id"] = "3", ["Name"] = "John Smit", ["Email"] = "john@example.com" }  // One char different
        };

        await _indexer.IndexDataSourceAsync(testData.ToAsyncEnumerable(), config, _mockProgressTracker.Object);

        var matchDefinition = CreateExactMatchDefinition(sourceId, "Name", 0.99);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinition))
        {
            candidates.Add(candidate);
        }

        // Assert
        // Should find match between records 1 and 2 (exact match)
        // Should NOT find match between 1 and 3 or 2 and 3 (below 0.99 threshold)
        var exactMatches = candidates.Where(c =>
            (c.Row1Number == 0 && c.Row2Number == 1) ||
            (c.Row1Number == 1 && c.Row2Number == 0)).ToList();

        Assert.NotEmpty(exactMatches);

        // Verify that near-matches are rejected
        var nearMatches = candidates.Where(c =>
            c.Row1Number == 2 || c.Row2Number == 2).ToList();

        Assert.Empty(nearMatches); // Should be empty due to 0.99 threshold

        _output.WriteLine($"Found {exactMatches.Count} exact matches, rejected {3 - candidates.Count} near-matches");
    }

    [Fact]
    public async Task ExactMatching_WithSlightDifference_ShouldNotMatch()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        await IndexTestData(sourceId, new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "1", ["Email"] = "john.smith@example.com" },
            new() { ["Id"] = "2", ["Email"] = "john.smith@example.con" }, // .con instead of .com
            new() { ["Id"] = "3", ["Email"] = "john-smith@example.com" }  // - instead of .
        });

        var matchDefinition = CreateExactMatchDefinition(sourceId, "Email", 0.99);

        // Act
        var candidates = await GetCandidates(matchDefinition);

        // Assert
        Assert.Empty(candidates); // None should match with 0.99 threshold
        _output.WriteLine("Correctly rejected all non-exact matches");
    }

    #endregion

    #region Threshold Boundary Tests

    [Fact]
    public async Task FuzzyMatching_AtThresholdBoundary_ShouldRespectThreshold()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        await _indexer.IndexDataSourceAsync(
            GenerateThresholdBoundaryData(),
            new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = "ThresholdTest",
                FieldsToIndex = new List<string> { "Name", "Email" },
                UseInMemoryStore = true
            },
            _mockProgressTracker.Object);

        // Test with 0.7 threshold
        var matchDef07 = CreateFuzzyMatchDefinition(sourceId, "Name", 0.7);
        var matchDef08 = CreateFuzzyMatchDefinition(sourceId, "Name", 0.8);
        var matchDef09 = CreateFuzzyMatchDefinition(sourceId, "Name", 0.9);

        // Act
        var candidates07 = await GetCandidates(matchDef07);
        var candidates08 = await GetCandidates(matchDef08);
        var candidates09 = await GetCandidates(matchDef09);

        // Assert
        Assert.True(candidates07.Count >= candidates08.Count, "Lower threshold should find more matches");
        Assert.True(candidates08.Count >= candidates09.Count, "Lower threshold should find more matches");

        _output.WriteLine($"Threshold 0.7: {candidates07.Count} matches");
        _output.WriteLine($"Threshold 0.8: {candidates08.Count} matches");
        _output.WriteLine($"Threshold 0.9: {candidates09.Count} matches");
    }

    [Fact]
    public async Task FuzzyMatching_BelowThreshold_ShouldNotMatch()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "1", ["Name"] = "Christopher" },
            new() { ["Id"] = "2", ["Name"] = "Chris" },  // ~0.5 similarity
            new() { ["Id"] = "3", ["Name"] = "Topher" }  // ~0.4 similarity
        };

        await IndexTestData(sourceId, testData);
        var matchDef = CreateFuzzyMatchDefinition(sourceId, "Name", 0.8);

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        Assert.Empty(candidates); // All pairs should be below 0.8 threshold
        _output.WriteLine("Correctly rejected all below-threshold matches");
    }

    #endregion

    #region Multi-Criteria Tests

    [Fact]
    public async Task MultiCriteria_AllMustBeSatisfied_ShouldOnlyMatchWhenAllPass()
    {
        // Arrange
        var customerSourceId = Guid.NewGuid();
        var orderSourceId = Guid.NewGuid();

        await _indexer.IndexDataSourceAsync(
            GenerateCustomerDataWithVaryingSimilarity(),
            new DataSourceIndexingConfig
            {
                DataSourceId = customerSourceId,
                DataSourceName = "Customers",
                FieldsToIndex = new List<string> { "Name", "Email", "Phone" },
                UseInMemoryStore = true
            },
            _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(
            GenerateOrderDataWithPartialMatches(),
            new DataSourceIndexingConfig
            {
                DataSourceId = orderSourceId,
                DataSourceName = "Orders",
                FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail", "CustomerPhone" },
                UseInMemoryStore = true
            },
            _mockProgressTracker.Object);

        // Create definition requiring BOTH name AND email to match
        var matchDef = CreateMultiCriteriaDefinition(customerSourceId, orderSourceId,
            requireBothNameAndEmail: true);

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        // Should only find ORD001 (matches both) and ORD004 (matches email exactly, name fuzzy)
        // Should NOT find ORD002 (only email matches) or ORD003 (only name matches)
        Assert.True(candidates.Count <= 3, "Should only match when both criteria are satisfied");

        _output.WriteLine($"Found {candidates.Count} matches where both criteria satisfied");
    }

    [Fact]
    public async Task MultiCriteria_PartialSatisfaction_ShouldNotMatch()
    {
        // Arrange
        var sourceId1 = Guid.NewGuid();
        var sourceId2 = Guid.NewGuid();

        var source1Data = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "1", ["Name"] = "John Smith", ["Email"] = "john@example.com" }
        };

        var source2Data = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "2", ["CustomerName"] = "John Smith", ["CustomerEmail"] = "different@example.com" },
            new() { ["Id"] = "3", ["CustomerName"] = "Different Name", ["CustomerEmail"] = "john@example.com" }
        };

        await IndexTestData(sourceId1, source1Data, new[] { "Name", "Email" });
        await IndexTestData(sourceId2, source2Data, new[] { "CustomerName", "CustomerEmail" }, "Orders");

        var matchDef = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Multi-Criteria Test",
            Definitions = new List<MatchDefinition>
            {
                new()
                {
                    Id = Guid.NewGuid(),
                    DataSourcePairId = Guid.NewGuid(),
                    Name = "Both Must Match",
                    Criteria = new List<MatchCriteria>
                    {
                        // Both criteria in same definition - both must be satisfied
                        new()
                        {
                            Id = Guid.NewGuid(),
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 0.5,
                            FieldMappings = new List<FieldMapping>
                            {
                                new(sourceId1, "Source1", "Name"),
                                new(sourceId2, "Source2", "CustomerName")
                            }
                        },
                        new()
                        {
                            Id = Guid.NewGuid(),
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 0.5,
                            FieldMappings = new List<FieldMapping>
                            {
                                new(sourceId1, "Source1", "Email"),
                                new(sourceId2, "Source2", "CustomerEmail")
                            }
                        }
                    }
                }
            }
        };

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        Assert.Empty(candidates); // No records match on BOTH fields
        _output.WriteLine("Correctly rejected all partial matches");
    }

    #endregion

    #region Edge Cases and Null Handling

    [Fact]
    public async Task Matching_WithEmptyAndNullValues_ShouldHandleGracefully()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "1", ["Name"] = "John", ["Email"] = "" },  // Empty email
            new() { ["Id"] = "2", ["Name"] = "", ["Email"] = "john@example.com" },  // Empty name
            new() { ["Id"] = "3", ["Name"] = null, ["Email"] = "test@example.com" },  // Null name
            new() { ["Id"] = "4", ["Name"] = "John", ["Email"] = null },  // Null email
            new() { ["Id"] = "5", ["Name"] = "", ["Email"] = "" }  // Both empty
        };

        await IndexTestData(sourceId, testData);
        var matchDef = CreateFuzzyMatchDefinition(sourceId, "Name", 0.5);

        // Act & Assert - Should not throw
        var candidates = await GetCandidates(matchDef);

        _output.WriteLine($"Handled {testData.Count} records with empty/null values, found {candidates.Count} matches");
    }

    [Fact]
    public async Task Matching_WithSpecialCharacters_ShouldHandleCorrectly()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "1", ["Name"] = "O'Brien", ["Email"] = "obrien@example.com" },
            new() { ["Id"] = "2", ["Name"] = "O Brien", ["Email"] = "obrien@example.com" },
            new() { ["Id"] = "3", ["Name"] = "Jean-Pierre", ["Email"] = "jp@example.com" },
            new() { ["Id"] = "4", ["Name"] = "Jean Pierre", ["Email"] = "jp@example.com" },
            new() { ["Id"] = "5", ["Name"] = "Smith & Jones", ["Email"] = "sj@example.com" },
            new() { ["Id"] = "6", ["Name"] = "Smith and Jones", ["Email"] = "sj@example.com" }
        };

        await IndexTestData(sourceId, testData);
        var matchDef = CreateFuzzyMatchDefinition(sourceId, "Name", 0.5);

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        Assert.NotEmpty(candidates); // Should find some matches despite special characters
        _output.WriteLine($"Found {candidates.Count} matches with special characters");
    }

    #endregion

    #region Performance with Selective Matching

    [Fact]
    public async Task LargeDataset_WithHighThreshold_ShouldFilterMostPairs()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>();

        // Generate 100 records with varying similarity
        for (int i = 0; i < 100; i++)
        {
            testData.Add(new Dictionary<string, object>
            {
                ["Id"] = $"ID{i:D3}",
                ["Name"] = $"Person {i % 10}", // Only 10 unique name patterns
                ["Email"] = $"person{i}@example.com" // All unique emails
            });
        }

        await IndexTestData(sourceId, testData);

        // High threshold should filter most pairs
        var matchDef = CreateFuzzyMatchDefinition(sourceId, "Email", 0.95);

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        // With unique emails and 0.95 threshold, should find very few or no matches
        Assert.True(candidates.Count < 10, $"High threshold should filter most pairs, but found {candidates.Count}");
        _output.WriteLine($"Out of {testData.Count * (testData.Count - 1) / 2} possible pairs, only {candidates.Count} passed the 0.95 threshold");
    }

    #endregion

    #region Cross-Source with Mixed Criteria Types

    [Fact]
    public async Task CrossSource_WithMixedExactAndFuzzy_ShouldRespectEachType()
    {
        // Arrange
        var sourceId1 = Guid.NewGuid();
        var sourceId2 = Guid.NewGuid();

        var source1Data = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "1", ["Code"] = "ABC123", ["Description"] = "Product One" },
            new() { ["Id"] = "2", ["Code"] = "ABC124", ["Description"] = "Product Two" }
        };

        var source2Data = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = "A", ["ProductCode"] = "ABC123", ["ProductDesc"] = "Product 1" },  // Exact code, fuzzy desc
            new() { ["Id"] = "B", ["ProductCode"] = "ABC124", ["ProductDesc"] = "Different Product" }  // Exact code, different desc
        };

        await IndexTestData(sourceId1, source1Data, new[] { "Code", "Description" });
        await IndexTestData(sourceId2, source2Data, new[] { "ProductCode", "ProductDesc" }, "Products");

        var matchDef = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Mixed Criteria",
            Definitions = new List<MatchDefinition>
            {
                new()
                {
                    Id = Guid.NewGuid(),
                    DataSourcePairId = Guid.NewGuid(),
                    Name = "Code Exact + Desc Fuzzy",
                    Criteria = new List<MatchCriteria>
                    {
                        new()
                        {
                            Id = Guid.NewGuid(),
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 0.7,
                            FieldMappings = new List<FieldMapping>
                            {
                                new(sourceId1, "Source1", "Code"),
                                new(sourceId2, "Source2", "ProductCode")
                            }
                        },
                        new()
                        {
                            Id = Guid.NewGuid(),
                            MatchingType = MatchingType.Fuzzy,
                            DataType = CriteriaDataType.Text,
                            Weight = 0.3,
                            Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.6" } },
                            FieldMappings = new List<FieldMapping>
                            {
                                new(sourceId1, "Source1", "Description"),
                                new(sourceId2, "Source2", "ProductDesc")
                            }
                        }
                    }
                }
            }
        };

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        // Should find match for record 1-A (exact code, similar desc)
        // Should NOT find match for record 2-B (exact code but very different desc)
        Assert.Single(candidates);
        _output.WriteLine($"Found {candidates.Count} match with mixed exact/fuzzy criteria");
    }

    #endregion

    #region MultipleMatchDefinitions

    [Fact]
    public async Task MultipleMatchDefinitions_ShouldTrackVaryingQualificationCounts()
    {
        // Arrange
        var sourceId1 = Guid.NewGuid();
        var sourceId2 = Guid.NewGuid();

        // Create test data with various matching characteristics
        var source1Data = new List<Dictionary<string, object>>
    {
        new() { ["Id"] = "1", ["Name"] = "John Smith", ["Email"] = "john@example.com", ["Phone"] = "555-1234", ["City"] = "New York" },
        new() { ["Id"] = "2", ["Name"] = "Jane Doe", ["Email"] = "jane@example.com", ["Phone"] = "555-5678", ["City"] = "Boston" },
        new() { ["Id"] = "3", ["Name"] = "Bob Johnson", ["Email"] = "bob@example.com", ["Phone"] = "555-9999", ["City"] = "Chicago" }
    };

        var source2Data = new List<Dictionary<string, object>>
    {
        // Will match with record 1 on all three definitions (exact email, fuzzy name, exact city)
        new() { ["Id"] = "A", ["CustomerName"] = "Jon Smith", ["CustomerEmail"] = "john@example.com", ["CustomerPhone"] = "555-1234", ["CustomerCity"] = "New York" },
        
        // Will match with record 1 on two definitions (exact email, exact city, but very different name)
        new() { ["Id"] = "B", ["CustomerName"] = "Johnny Smithers", ["CustomerEmail"] = "john@example.com", ["CustomerPhone"] = "555-0000", ["CustomerCity"] = "New York" },
        
        // Will match with record 1 on one definition only (exact email, but different city and name)
        new() { ["Id"] = "C", ["CustomerName"] = "Michael Brown", ["CustomerEmail"] = "john@example.com", ["CustomerPhone"] = "555-7777", ["CustomerCity"] = "Los Angeles" },
        
        // Will match with record 2 on one definition (fuzzy name match only)
        new() { ["Id"] = "D", ["CustomerName"] = "Jane Do", ["CustomerEmail"] = "different@example.com", ["CustomerPhone"] = "555-0000", ["CustomerCity"] = "Miami" },
        
        // No matches with any
        new() { ["Id"] = "E", ["CustomerName"] = "Alice Cooper", ["CustomerEmail"] = "alice@example.com", ["CustomerPhone"] = "555-3333", ["CustomerCity"] = "Seattle" }
    };

        // Index both sources
        await _indexer.IndexDataSourceAsync(
            source1Data.ToAsyncEnumerable(),
            new DataSourceIndexingConfig
            {
                DataSourceId = sourceId1,
                DataSourceName = "Customers",
                FieldsToIndex = new List<string> { "Name", "Email", "Phone", "City" },
                UseInMemoryStore = true
            },
            _mockProgressTracker.Object);

        await _indexer.IndexDataSourceAsync(
            source2Data.ToAsyncEnumerable(),
            new DataSourceIndexingConfig
            {
                DataSourceId = sourceId2,
                DataSourceName = "Orders",
                FieldsToIndex = new List<string> { "CustomerName", "CustomerEmail", "CustomerPhone", "CustomerCity" },
                UseInMemoryStore = true
            },
            _mockProgressTracker.Object);

        // Create three different match definitions
        var matchDefinitions = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Multiple Definition Test",
            Definitions = new List<MatchDefinition>()
        };

        // Definition 1: Email Exact Match
        var emailDef = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            Name = "Email Exact Match",
            Criteria = new List<MatchCriteria>
        {
            new()
            {
                Id = Guid.NewGuid(),
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text,
                Weight = 1.0,
                FieldMappings = new List<FieldMapping>
                {
                    new(sourceId1, "Customers", "Email"),
                    new(sourceId2, "Orders", "CustomerEmail")
                }
            }
        }
        };

        // Definition 2: Name Fuzzy Match (high threshold)
        var nameDef = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            Name = "Name Fuzzy Match",
            Criteria = new List<MatchCriteria>
        {
            new()
            {
                Id = Guid.NewGuid(),
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Weight = 1.0,
                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.5" } },
                FieldMappings = new List<FieldMapping>
                {
                    new(sourceId1, "Customers", "Name"),
                    new(sourceId2, "Orders", "CustomerName")
                }
            }
        }
        };

        // Definition 3: City Exact Match
        var cityDef = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            Name = "City Exact Match",
            Criteria = new List<MatchCriteria>
        {
            new()
            {
                Id = Guid.NewGuid(),
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text,
                Weight = 1.0,
                FieldMappings = new List<FieldMapping>
                {
                    new(sourceId1, "Customers", "City"),
                    new(sourceId2, "Orders", "CustomerCity")
                }
            }
        }
        };

        matchDefinitions.Definitions.Add(emailDef);
        matchDefinitions.Definitions.Add(nameDef);
        matchDefinitions.Definitions.Add(cityDef);

        // Act
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDefinitions))
        {
            candidates.Add(candidate);
        }

        // Assert
        Assert.NotEmpty(candidates);

        // Group candidates by the number of match definitions they qualified for
        var candidatesByDefinitionCount = candidates
            .GroupBy(c => c.MatchDefinitionIds.Count)
            .OrderByDescending(g => g.Key)
            .ToDictionary(g => g.Key, g => g.ToList());

        // Log the results for visibility
        foreach (var group in candidatesByDefinitionCount)
        {
            _output.WriteLine($"Candidates qualifying for {group.Key} definition(s): {group.Value.Count}");

            foreach (var candidate in group.Value.Take(3)) // Show first 3 of each group
            {
                var (record1, record2) = await candidate.GetRecordsAsync();
                _output.WriteLine($"  - {record1["Name"]} ({record1["Id"]}) <-> {record2["CustomerName"]} ({record2["Id"]})");
                _output.WriteLine($"    Match Definition IDs: {candidate.MatchDefinitionIdsString}");
                _output.WriteLine($"    Matched via: {string.Join(", ", GetMatchDefinitionNames(candidate.MatchDefinitionIds, matchDefinitions))}");
            }
        }

        // Verify we have candidates with different numbers of match definitions
        Assert.True(candidatesByDefinitionCount.Count >= 2,
            "Should have candidates qualifying for different numbers of definitions");

        // Verify specific expected matches

        // 1. Find the candidate for Customer 1 (John Smith) and Order A (Jon Smith)
        var john_jonCandidate = candidates.FirstOrDefault(c =>
            ((c.Row1Number == 0 && c.Row2Number == 0) || (c.Row1Number == 0 && c.Row2Number == 0)));

        if (john_jonCandidate != null)
        {
            // Should match on all three definitions (email exact, name fuzzy, city exact)
            Assert.Equal(3, john_jonCandidate.MatchDefinitionIds.Count);
            Assert.Contains(emailDef.Id, john_jonCandidate.MatchDefinitionIds);
            Assert.Contains(nameDef.Id, john_jonCandidate.MatchDefinitionIds);
            Assert.Contains(cityDef.Id, john_jonCandidate.MatchDefinitionIds);
            _output.WriteLine($"✓ John Smith <-> Jon Smith matched on {john_jonCandidate.MatchDefinitionIds.Count} definitions as expected");
        }

        // 2. Find candidates that match on exactly 2 definitions
        var twoDefCandidates = candidatesByDefinitionCount.GetValueOrDefault(2, new List<CandidatePair>());
        if (twoDefCandidates.Any())
        {
            _output.WriteLine($"✓ Found {twoDefCandidates.Count} candidates matching on exactly 2 definitions");

            // Verify they have exactly 2 definition IDs
            Assert.All(twoDefCandidates, c => Assert.Equal(2, c.MatchDefinitionIds.Count));
        }

        // 3. Find candidates that match on exactly 1 definition
        var oneDefCandidates = candidatesByDefinitionCount.GetValueOrDefault(1, new List<CandidatePair>());
        if (oneDefCandidates.Any())
        {
            _output.WriteLine($"✓ Found {oneDefCandidates.Count} candidates matching on exactly 1 definition");

            // Verify they have exactly 1 definition ID
            Assert.All(oneDefCandidates, c => Assert.Single(c.MatchDefinitionIds));
        }

        // 4. Verify the MatchDefinitionIdsString format
        var sampleCandidate = candidates.First();
        if (sampleCandidate.MatchDefinitionIds.Count > 1)
        {
            Assert.Contains(",", sampleCandidate.MatchDefinitionIdsString);
            _output.WriteLine($"✓ Multi-definition candidate has comma-separated IDs: {sampleCandidate.MatchDefinitionIdsString}");
        }

        // 5. Summary statistics
        _output.WriteLine("\n=== Summary ===");
        _output.WriteLine($"Total candidates found: {candidates.Count}");
        _output.WriteLine($"Max definitions per candidate: {candidates.Max(c => c.MatchDefinitionIds.Count)}");
        _output.WriteLine($"Min definitions per candidate: {candidates.Min(c => c.MatchDefinitionIds.Count)}");
        _output.WriteLine($"Average definitions per candidate: {candidates.Average(c => c.MatchDefinitionIds.Count):F2}");

        // Final assertion: Ensure we have variety in match definition counts
        var uniqueDefinitionCounts = candidates.Select(c => c.MatchDefinitionIds.Count).Distinct().Count();
        Assert.True(uniqueDefinitionCounts >= 2,
            $"Should have candidates with at least 2 different numbers of match definitions, but found {uniqueDefinitionCounts}");
    }

    // Helper method to get match definition names
    private List<string> GetMatchDefinitionNames(HashSet<Guid> definitionIds, MatchDefinitionCollection collection)
    {
        return collection.Definitions
            .Where(d => definitionIds.Contains(d.Id))
            .Select(d => d.Name)
            .ToList();
    }

    #endregion

    #region Helper Methods

    private async Task IndexTestData(
        Guid sourceId,
        List<Dictionary<string, object>> data,
        string[] fields = null,
        string sourceName = "TestSource")
    {
        fields ??= data.First().Keys.Where(k => k != "Id").ToArray();

        await _indexer.IndexDataSourceAsync(
            data.ToAsyncEnumerable(),
            new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = sourceName,
                FieldsToIndex = fields.ToList(),
                UseInMemoryStore = true
            },
            _mockProgressTracker.Object);
    }

    private async Task<List<CandidatePair>> GetCandidates(MatchDefinitionCollection matchDef)
    {
        var candidates = new List<CandidatePair>();
        await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
        {
            candidates.Add(candidate);
        }
        return candidates;
    }

    private MatchDefinitionCollection CreateExactMatchDefinition(Guid sourceId, string fieldName, double threshold = 0.99)
    {
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Exact Match Test",
            Definitions = new List<MatchDefinition>
            {
                new()
                {
                    Id = Guid.NewGuid(),
                    DataSourcePairId = Guid.NewGuid(),
                    Name = "Exact",
                    Criteria = new List<MatchCriteria>
                    {
                        new()
                        {
                            Id = Guid.NewGuid(),
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 1.0,
                            FieldMappings = new List<FieldMapping>
                            {
                                new(sourceId, "Source", fieldName)
                            }
                        }
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateFuzzyMatchDefinition(Guid sourceId, string fieldName, double threshold)
    {
        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Fuzzy Match Test",
            Definitions = new List<MatchDefinition>
            {
                new()
                {
                    Id = Guid.NewGuid(),
                    DataSourcePairId = Guid.NewGuid(),
                    Name = "Fuzzy",
                    Criteria = new List<MatchCriteria>
                    {
                        new()
                        {
                            Id = Guid.NewGuid(),
                            MatchingType = MatchingType.Fuzzy,
                            DataType = CriteriaDataType.Text,
                            Weight = 1.0,
                            Arguments = new Dictionary<ArgsValue, string>
                            {
                                { ArgsValue.FastLevel, threshold.ToString() }
                            },
                            FieldMappings = new List<FieldMapping>
                            {
                                new(sourceId, "TestSource", fieldName)
                            }
                        }
                    }
                }
            }
        };
    }

    private MatchDefinitionCollection CreateMultiCriteriaDefinition(
        Guid sourceId1,
        Guid sourceId2,
        bool requireBothNameAndEmail)
    {
        var definition = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            DataSourcePairId = Guid.NewGuid(),
            Name = "Multi-Criteria",
            Criteria = new List<MatchCriteria>()
        };

        if (requireBothNameAndEmail)
        {
            // Both criteria in same definition - both must match
            definition.Criteria.Add(new MatchCriteria
            {
                Id = Guid.NewGuid(),
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Weight = 0.5,
                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.7" } },
                FieldMappings = new List<FieldMapping>
                {
                    new(sourceId1, "Customers", "Name"),
                    new(sourceId2, "Orders", "CustomerName")
                }
            });

            definition.Criteria.Add(new MatchCriteria
            {
                Id = Guid.NewGuid(),
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text,
                Weight = 0.5,
                FieldMappings = new List<FieldMapping>
                {
                    new(sourceId1, "Customers", "Email"),
                    new(sourceId2, "Orders", "CustomerEmail")
                }
            });
        }

        return new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Multi-Criteria Collection",
            Definitions = new List<MatchDefinition> { definition }
        };
    }

    #endregion

    #region Deduplication with Strict Thresholds

    [Fact]
    public async Task Deduplication_WithExactMatching_ShouldOnlyFindIdenticalRecords()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
       {
           new() { ["Id"] = "1", ["Name"] = "John Smith", ["Email"] = "john@example.com", ["Phone"] = "555-1234" },
           new() { ["Id"] = "2", ["Name"] = "John Smith", ["Email"] = "john@example.com", ["Phone"] = "555-1234" }, // Exact duplicate
           new() { ["Id"] = "3", ["Name"] = "John Smith", ["Email"] = "john@example.co", ["Phone"] = "555-1234" },  // One char diff in email
           new() { ["Id"] = "4", ["Name"] = "Jon Smith", ["Email"] = "john@example.com", ["Phone"] = "555-1234" },   // One char diff in name
       };

        await IndexTestData(sourceId, testData);

        var matchDef = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Exact Dedup",
            Definitions = new List<MatchDefinition>
           {
               new()
               {
                   Id = Guid.NewGuid(),
                   DataSourcePairId = Guid.NewGuid(),
                   Name = "Exact Match All Fields",
                   Criteria = new List<MatchCriteria>
                   {
                       new()
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Exact,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.5,
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "Name") }
                       },
                       new()
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Exact,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.5,
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "Email") }
                       }
                   }
               }
           }
        };

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        // Should only find the pair (1,2) as they are exact matches
        Assert.Single(candidates);
        var match = candidates.First();
        Assert.True((match.Row1Number == 0 && match.Row2Number == 1) ||
                   (match.Row1Number == 1 && match.Row2Number == 0));

        _output.WriteLine($"Found {candidates.Count} exact duplicate(s) out of {testData.Count} records");
    }

    [Fact]
    public async Task Deduplication_WithProgressiveThresholds_ShouldFindMoreMatchesWithLowerThreshold()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
       {
           new() { ["Id"] = "1", ["Company"] = "Microsoft Corporation" },
           new() { ["Id"] = "2", ["Company"] = "Microsoft Corp." },      // ~0.85 similarity
           new() { ["Id"] = "3", ["Company"] = "Microsoft Corp" },        // ~0.90 similarity
           new() { ["Id"] = "4", ["Company"] = "Microsoft" },             // ~0.70 similarity
           new() { ["Id"] = "5", ["Company"] = "Microsft Corporation" },  // Typo, ~0.95 similarity
           new() { ["Id"] = "6", ["Company"] = "Apple Inc." }             // No similarity
       };

        await IndexTestData(sourceId, testData, new[] { "Company" });

        // Test with different thresholds
        var thresholds = new[] { 0.95, 0.85, 0.70 };
        var results = new Dictionary<double, int>();

        foreach (var threshold in thresholds)
        {
            var matchDef = CreateFuzzyMatchDefinition(sourceId, "Company", threshold);
            var candidates = await GetCandidates(matchDef);
            results[threshold] = candidates.Count;
        }

        // Assert
        Assert.True(results[0.70] >= results[0.85], "Lower threshold should find more or equal matches");
        Assert.True(results[0.85] >= results[0.95], "Lower threshold should find more or equal matches");

        foreach (var kvp in results)
        {
            _output.WriteLine($"Threshold {kvp.Key:F2}: {kvp.Value} matches");
        }
    }

    #endregion

    #region Complex Multi-Field Scenarios

    [Fact]
    public async Task MultiField_WithDifferentThresholdsPerField_ShouldRespectIndividualThresholds()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
       {
           new() { ["Id"] = "1", ["FirstName"] = "Jonathan", ["LastName"] = "Smith", ["City"] = "New York" },
           new() { ["Id"] = "2", ["FirstName"] = "Jon", ["LastName"] = "Smith", ["City"] = "New York" },      // First name fuzzy match
           new() { ["Id"] = "3", ["FirstName"] = "Jonathan", ["LastName"] = "Smyth", ["City"] = "New York" }, // Last name fuzzy match
           new() { ["Id"] = "4", ["FirstName"] = "Jon", ["LastName"] = "Smyth", ["City"] = "NYC" },          // All fuzzy
           new() { ["Id"] = "5", ["FirstName"] = "James", ["LastName"] = "Johnson", ["City"] = "Boston" }     // No match
       };

        await IndexTestData(sourceId, testData);

        var matchDef = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Multi-Field Test",
            Definitions = new List<MatchDefinition>
           {
               new()
               {
                   Id = Guid.NewGuid(),
                   DataSourcePairId = Guid.NewGuid(),
                   Name = "Different Thresholds",
                   Criteria = new List<MatchCriteria>
                   {
                       new() // Strict on last name
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Fuzzy,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.5,
                           Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.9" } },
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "LastName") }
                       },
                       new() // Lenient on first name
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Fuzzy,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.3,
                           Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.6" } },
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "FirstName") }
                       },
                       new() // Exact on city
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Exact,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.2,
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "City") }
                       }
                   }
               }
           }
        };

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        // Should find (1,2) - same last name, fuzzy first name, same city
        // Should NOT find (1,3) - fuzzy last name might not meet 0.9 threshold
        // Should NOT find (1,4) - different city (exact match required)
        // Should NOT find any with record 5

        _output.WriteLine($"Found {candidates.Count} matches with different thresholds per field");

        // Verify no matches with record 5
        var matchesWithRecord5 = candidates.Where(c => c.Row1Number == 4 || c.Row2Number == 4);
        Assert.Empty(matchesWithRecord5);
    }

    #endregion

    #region Zero and One Match Scenarios

    [Fact]
    public async Task NoCommonQGrams_ShouldFindNoMatches()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
       {
           new() { ["Id"] = "1", ["Code"] = "AAAAA" },
           new() { ["Id"] = "2", ["Code"] = "BBBBB" },
           new() { ["Id"] = "3", ["Code"] = "CCCCC" },
           new() { ["Id"] = "4", ["Code"] = "DDDDD" },
           new() { ["Id"] = "5", ["Code"] = "EEEEE" }
       };

        await IndexTestData(sourceId, testData, new[] { "Code" });
        var matchDef = CreateFuzzyMatchDefinition(sourceId, "Code", 0.5);

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        Assert.Empty(candidates); // No common q-grams means no candidates
        _output.WriteLine("Correctly found no matches when no common q-grams exist");
    }

    [Fact]
    public async Task SingleCharacterData_WithQGramSize3_ShouldHandleGracefully()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
       {
           new() { ["Id"] = "1", ["Initial"] = "A" },
           new() { ["Id"] = "2", ["Initial"] = "B" },
           new() { ["Id"] = "3", ["Initial"] = "A" },
           new() { ["Id"] = "4", ["Initial"] = "C" }
       };

        await IndexTestData(sourceId, testData, new[] { "Initial" });
        var matchDef = CreateExactMatchDefinition(sourceId, "Initial");

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        // With q-gram size 3 and single character data, behavior depends on implementation
        // Should at least not crash
        _output.WriteLine($"Handled single character data, found {candidates.Count} matches");
    }

    #endregion

    #region Validation of Criteria Independence

    [Fact]
    public async Task MultipleCriteriaInDefinition_AllMustPass_NotJustOne()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
       {
           new() { ["Id"] = "1", ["Field1"] = "ABC", ["Field2"] = "XYZ", ["Field3"] = "123" },
           new() { ["Id"] = "2", ["Field1"] = "ABC", ["Field2"] = "XYZ", ["Field3"] = "456" }, // Field3 different
           new() { ["Id"] = "3", ["Field1"] = "ABC", ["Field2"] = "UVW", ["Field3"] = "123" }, // Field2 different
           new() { ["Id"] = "4", ["Field1"] = "DEF", ["Field2"] = "XYZ", ["Field3"] = "123" }, // Field1 different
           new() { ["Id"] = "5", ["Field1"] = "ABC", ["Field2"] = "XYZ", ["Field3"] = "123" }  // All match with 1
       };

        await IndexTestData(sourceId, testData);

        var matchDef = new MatchDefinitionCollection
        {
            Id = Guid.NewGuid(),
            ProjectId = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "All Criteria Must Match",
            Definitions = new List<MatchDefinition>
           {
               new()
               {
                   Id = Guid.NewGuid(),
                   DataSourcePairId = Guid.NewGuid(),
                   Name = "Three Exact Criteria",
                   Criteria = new List<MatchCriteria>
                   {
                       new()
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Exact,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.33,
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "Field1") }
                       },
                       new()
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Exact,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.33,
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "Field2") }
                       },
                       new()
                       {
                           Id = Guid.NewGuid(),
                           MatchingType = MatchingType.Exact,
                           DataType = CriteriaDataType.Text,
                           Weight = 0.34,
                           FieldMappings = new List<FieldMapping> { new(sourceId, "Source", "Field3") }
                       }
                   }
               }
           }
        };

        // Act
        var candidates = await GetCandidates(matchDef);

        // Assert
        // Should only find match between records 1 and 5 (all fields match)
        // Should NOT find matches with 2, 3, or 4 (each differs in one field)
        Assert.Single(candidates);
        var match = candidates.First();
        Assert.True((match.Row1Number == 0 && match.Row2Number == 4) ||
                   (match.Row1Number == 4 && match.Row2Number == 0));

        _output.WriteLine($"Correctly found only {candidates.Count} match where ALL criteria passed");
    }

    #endregion

    #region Case Sensitivity Tests

    [Fact]
    public async Task CaseSensitivity_ShouldHandleMixedCase()
    {
        // Arrange
        var sourceId = Guid.NewGuid();
        var testData = new List<Dictionary<string, object>>
       {
           new() { ["Id"] = "1", ["Name"] = "john smith" },
           new() { ["Id"] = "2", ["Name"] = "John Smith" },
           new() { ["Id"] = "3", ["Name"] = "JOHN SMITH" },
           new() { ["Id"] = "4", ["Name"] = "JoHn SmItH" },
           new() { ["Id"] = "5", ["Name"] = "johnsmith" }  // No space
       };

        await IndexTestData(sourceId, testData);

        // Test if case affects matching
        var exactMatchDef = CreateExactMatchDefinition(sourceId, "Name");
        var fuzzyMatchDef = CreateFuzzyMatchDefinition(sourceId, "Name", 0.8);

        // Act
        var exactCandidates = await GetCandidates(exactMatchDef);
        var fuzzyCandidates = await GetCandidates(fuzzyMatchDef);

        // Assert
        _output.WriteLine($"Exact matching found {exactCandidates.Count} matches with mixed case");
        _output.WriteLine($"Fuzzy matching found {fuzzyCandidates.Count} matches with mixed case");

        // Fuzzy should find more matches than exact if case matters
        Assert.True(fuzzyCandidates.Count >= exactCandidates.Count);
    }

    #endregion

    #region Unicode and International Characters

    //[Fact]
    //public async Task InternationalCharacters_ShouldMatchCorrectly()
    //{
    //    // Arrange
    //    var sourceId = Guid.NewGuid();
    //    var testData = new List<Dictionary<string, object>>
    //   {
    //       new() { ["Id"] = "1", ["Name"] = "Müller" },
    //       new() { ["Id"] = "2", ["Name"] = "Mueller" },  // Alternative spelling
    //       new() { ["Id"] = "3", ["Name"] = "Muller" },   // Without umlaut
    //       new() { ["Id"] = "4", ["Name"] = "José" },
    //       new() { ["Id"] = "5", ["Name"] = "Jose" },     // Without accent
    //       new() { ["Id"] = "6", ["Name"] = "北京" },      // Chinese characters
    //       new() { ["Id"] = "7", ["Name"] = "Beijing" }   // Romanized
    //   };

    //    await IndexTestData(sourceId, testData);
    //    var matchDef = CreateFuzzyMatchDefinition(sourceId, "Name", 0.7);

    //    // Act
    //    var candidates = await GetCandidates(matchDef);

    //    // Assert
    //    _output.WriteLine($"Found {candidates.Count} matches with international characters");
    //    // Should find some matches between similar spellings
    //    Assert.NotEmpty(candidates);
    //}

    #endregion

    public void Dispose()
    {
        _indexer?.Dispose();
    }
}

#region Extension Methods for Testing

public static class AsyncEnumerableExtensions
{
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> source)
    {
        foreach (var item in source)
        {
            yield return item;
        }
        await Task.CompletedTask;
    }
}

#endregion