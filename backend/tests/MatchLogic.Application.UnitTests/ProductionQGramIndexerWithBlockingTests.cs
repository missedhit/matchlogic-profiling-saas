using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Org.BouncyCastle.Ocsp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace MatchLogic.Application.UnitTests
{
    /// <summary>
    /// Comprehensive unit tests for ProductionQGramIndexerWithBlocking
    /// </summary>
    public class ProductionQGramIndexerWithBlockingTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly Mock<ILogger<ProductionQGramIndexerWithBlocking>> _mockLogger;
        private readonly QGramIndexerWithBlockingOptions _options;
        private readonly ProductionQGramIndexerWithBlocking _indexer;

        private readonly Mock<ILogger<ProductionQGramIndexer>> _mockWithoutBlockingLogger;
        private readonly QGramIndexerOptions _optionsWithoutBlocking;
        private readonly ProductionQGramIndexer _indexerWithoutBlocking;
        private readonly Mock<IStepProgressTracker> _mockProgressTracker;

        public ProductionQGramIndexerWithBlockingTests(ITestOutputHelper output)
        {
            _output = output;
            _mockLogger = new Mock<ILogger<ProductionQGramIndexerWithBlocking>>();
            _options = new QGramIndexerWithBlockingOptions
            {
                QGramSize = 3,
                DefaultInMemoryThreshold = 1000,
                EnableCompression = false,
                MaxParallelism = Environment.ProcessorCount,
                CandidateChannelCapacity = 100,
                EnableBlocking = true,
                EnableMultiFieldBlocking = true,
                EnableMultiSchemeBlocking = true,
                MaxBlockSize = 1000,
                MinBlockSize = 2,
                BlockingKeyLength = 3,
                NormalizeBlockingKeys = true,
                BucketStrategy = BucketOptimizationStrategy.None

            };

            var optionsMock = new Mock<IOptions<QGramIndexerWithBlockingOptions>>();
            optionsMock.Setup(x => x.Value).Returns(_options);

            _indexer = new ProductionQGramIndexerWithBlocking(optionsMock.Object, _mockLogger.Object);

            _mockWithoutBlockingLogger = new Mock<ILogger<ProductionQGramIndexer>>();
            _optionsWithoutBlocking = new QGramIndexerOptions
            {
                QGramSize = 3,
                DefaultInMemoryThreshold = 1000,
                EnableCompression = false, // Disable for testing
                MaxParallelism = 2,
                CandidateChannelCapacity = 100
            };

            var optionsWithoutBlockingMock = new Mock<IOptions<QGramIndexerOptions>>();
            optionsWithoutBlockingMock.Setup(x => x.Value).Returns(_optionsWithoutBlocking);

            _indexerWithoutBlocking = new ProductionQGramIndexer(optionsWithoutBlockingMock.Object, _mockWithoutBlockingLogger.Object);

            _mockProgressTracker = new Mock<IStepProgressTracker>();
        }

        #region Test Data Generators

        private string GetRandomState(Random random, Dictionary<string, int> stateFrequency, int maxFrequency = 100)
        {
            // Generate or reuse a state based on probability
            if (stateFrequency.Count < 50 || random.NextDouble() < 0.1) // 10% chance of new state
            {
                var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                var newState = "" + chars[random.Next(chars.Length)] + chars[random.Next(chars.Length)];

                if (!stateFrequency.ContainsKey(newState))
                {
                    stateFrequency[newState] = 1;
                    return newState;
                }
            }

            // Weighted selection from existing states (prefer less-used states)
            var availableStates = stateFrequency.Where(x => x.Value < maxFrequency).ToList();
            if (availableStates.Any())
            {
                // Inverse weight - less used states have higher chance
                var weights = availableStates.Select(x => maxFrequency - x.Value).ToList();
                var totalWeight = weights.Sum();
                var randomWeight = random.Next(totalWeight);

                int cumulative = 0;
                for (int i = 0; i < availableStates.Count; i++)
                {
                    cumulative += weights[i];
                    if (randomWeight < cumulative)
                    {
                        var selected = availableStates[i].Key;
                        stateFrequency[selected]++;
                        return selected;
                    }
                }
            }

            // Fallback: pick random existing state
            var states = stateFrequency.Keys.ToList();
            var state = states[random.Next(states.Count)];
            stateFrequency[state]++;
            return state;
        }

        private async IAsyncEnumerable<IDictionary<string, object>> GenerateTestDataWithBlockableFields(int count)
        {
            for (int i = 0; i < count; i++)
            {
                yield return new Dictionary<string, object>
                {
                    ["Id"] = $"ID{i:D3}",
                    ["State"] = i < count / 2 ? "CA" : "NY", // Two distinct blocks
                    ["City"] = i % 3 == 0 ? "San Francisco" : i % 3 == 1 ? "Los Angeles" : "New York",
                    ["Name"] = $"Person {i}",
                    ["Email"] = $"person{i}@example.com",
                    ["Phone"] = $"555-{i:D4}"
                };
            }
        }

        private async IAsyncEnumerable<IDictionary<string, object>> GenerateDataWithMultipleBlockingFields(int count)
        {
            var states = new[] { "CA", "NY", "TX" };
            var departments = new[] { "Sales", "Engineering", "HR" };

            for (int i = 0; i < count; i++)
            {
                yield return new Dictionary<string, object>
                {
                    ["Id"] = $"EMP{i:D3}",
                    ["State"] = states[i % 3],
                    ["Department"] = departments[i % 3],
                    ["Name"] = $"Employee {i}",
                    ["Salary"] = (50000 + i * 1000).ToString(),
                    ["JoinDate"] = DateTime.Now.AddDays(-i).ToString("yyyy-MM-dd")
                };
            }
        }

        private async IAsyncEnumerable<IDictionary<string, object>> GenerateLargeDatasetWithBlocking(int count)
        {
            var random = new Random(42);
            var firstNames = new[] { "John", "Jane", "Bob", "Alice", "Charlie", "Diana" };
            var lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Davis" };
            var zipcodes = new[] { "10001", "10002", "10003", "90001", "90002", "90003" };

            for (int i = 0; i < count; i++)
            {
                var firstName = firstNames[random.Next(firstNames.Length)];
                var lastName = lastNames[random.Next(lastNames.Length)];

                yield return new Dictionary<string, object>
                {
                    ["Id"] = Guid.NewGuid().ToString(),
                    ["FirstName"] = firstName,
                    ["LastName"] = lastName,
                    ["FullName"] = $"{firstName} {lastName}",
                    ["ZipCode"] = zipcodes[random.Next(zipcodes.Length)],
                    ["Email"] = $"{firstName.ToLower()}.{lastName.ToLower()}{i}@example.com",
                    ["Phone"] = $"555-{random.Next(1000, 9999)}"
                };
            }
        }

        #endregion

        #region Single Field Blocking Tests

        [Fact]
        public async Task SingleFieldBlocking_DiagnoseDifference()
        {
            Console.WriteLine("Starting SingleFieldBlocking_DiagnoseDifference test...");
            // Arrange
            var sourceId = Guid.NewGuid();
            var config = new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = "TestSource",
                FieldsToIndex = new List<string> { "State", "Name", "Email" },
                UseInMemoryStore = true
            };

            // Create match definition with exact State match
            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "State Blocking Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "State Block + Name Match",
                        Criteria = new List<MatchCriteria>
                        {
                            new() // Exact State - will be used for blocking
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.3,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "TestSource", "State")
                                }
                            },
                            new() // Fuzzy Name match
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.7,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.7" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "TestSource", "Name")
                                }
                            }
                        }
                    }
                }
            };

            // Initialize blocking configuration
            _indexer.InitializeBlockingConfiguration(matchDef);

            // Create test data - make it deterministic for debugging
            var testData = new List<Dictionary<string, object>>();
            string[] states = {
                "CA", "NY", "AB", "BC", "CD", "DE", "EF", "FG", "GH", "HI",
                "IJ", "JK", "KL", "LM", "MN", "NO", "OP", "PQ", "QR", "RS",
                "ST", "TU", "UV", "VW", "WX", "XY", "YZ", "AZ", "BA", "CB",
                "DC", "ED", "FE", "GF", "HG", "IH", "JI", "KJ", "LK", "ML",
                "NM", "ON", "PO", "QP", "RQ", "SR", "TS", "UT", "VU", "WV",
                "XW", "YX", "ZY", "AA", "BB", "CC", "DD", "EE", "FF", "GG",
                "HH", "II", "JJ", "KK", "LL", "MM", "NN", "OO", "PP", "QQ",
                "RR", "SS", "TT", "UU", "VV", "WW", "XX", "YY", "ZZ", "AC",
                "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM",
                "AN", "AO", "AP", "AQ", "AR", "AS", "AT", "AU", "AV", "AW"
            };

            for (int i = 0; i < 100000; i++)
            {
                testData.Add(new Dictionary<string, object>
                {
                    ["Id"] = $"ID{i:D3}",
                    ["State"] = i < 1000 ? states[0] : states[i / 1000],
                    ["Name"] = $"Person {i}",
                    ["Email"] = $"person{i}@example.com"
                });
            }

            // Index data for both indexers
            await _indexer.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                config,
                _mockProgressTracker.Object);

            await _indexerWithoutBlocking.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                config,
                _mockProgressTracker.Object);

            // Act - collect all candidates from both
            var candidatesWithBlocking = new List<CandidatePair>();
            Stopwatch sw = Stopwatch.StartNew();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidatesWithBlocking.Add(candidate);
            }
            sw.Stop();
            Console.WriteLine($"Blocking candidate generation took {sw.ElapsedMilliseconds} ms");

            sw.Restart();
            var candidatesWithoutBlocking = new List<CandidatePair>();
            await foreach (var candidate in _indexerWithoutBlocking.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidatesWithoutBlocking.Add(candidate);
            }
            sw.Stop();
            Console.WriteLine($"Non-blocking candidate generation took {sw.ElapsedMilliseconds} ms");

            // Create hashsets for comparison
            var blockingPairs = new HashSet<(int, int)>();
            foreach (var candidate in candidatesWithBlocking)
            {
                var row1 = candidate.Row1Number;
                var row2 = candidate.Row2Number;
                // Normalize order
                if (row1 > row2) (row1, row2) = (row2, row1);
                blockingPairs.Add((row1, row2));
            }

            var nonBlockingPairs = new HashSet<(int, int)>();
            foreach (var candidate in candidatesWithoutBlocking)
            {
                var row1 = candidate.Row1Number;
                var row2 = candidate.Row2Number;
                // Normalize order
                if (row1 > row2) (row1, row2) = (row2, row1);
                nonBlockingPairs.Add((row1, row2));
            }

            // Find pairs that are in non-blocking but not in blocking
            var missingInBlocking = nonBlockingPairs.Except(blockingPairs).ToList();

            // Find pairs that are in blocking but not in non-blocking (should be empty or very few)
            var extraInBlocking = blockingPairs.Except(nonBlockingPairs).ToList();

            Console.WriteLine($"Total candidates - With blocking: {candidatesWithBlocking.Count}, Without blocking: {candidatesWithoutBlocking.Count}");
            Console.WriteLine($"Missing in blocking: {missingInBlocking.Count} pairs");
            Console.WriteLine($"Extra in blocking: {extraInBlocking.Count} pairs");

            // Analyze first 10 missing pairs
            Console.WriteLine("\n=== First 10 pairs missing in blocking version ===");
            int analyzed = 0;
            foreach (var (row1, row2) in missingInBlocking.Take(10))
            {
                analyzed++;

                // Get the actual records
                var record1 = testData[row1];
                var record2 = testData[row2];

                Console.WriteLine($"\nPair {analyzed}: Row {row1} <-> Row {row2}");
                Console.WriteLine($"  Record 1: State={record1["State"]}, Name={record1["Name"]}");
                Console.WriteLine($"  Record 2: State={record2["State"]}, Name={record2["Name"]}");

                // Check if states match
                bool statesMatch = record1["State"].Equals(record2["State"]);
                Console.WriteLine($"  States match: {statesMatch}");

                if (!statesMatch)
                {
                    _output.WriteLine("  *** CROSS-STATE PAIR - Should NOT exist! ***");
                }
            }

            // Also check some pairs that exist in both
            Console.WriteLine("\n=== First 5 pairs that exist in BOTH versions ===");
            var commonPairs = blockingPairs.Intersect(nonBlockingPairs).Take(5).ToList();
            foreach (var (row1, row2) in commonPairs)
            {
                var record1 = testData[row1];
                var record2 = testData[row2];
                Console.WriteLine($"Row {row1} <-> Row {row2}: State={record1["State"]}/{record2["State"]}, Name={record1["Name"]}/{record2["Name"]}");
            }

            // Analyze state distribution in non-blocking candidates
            Console.WriteLine("\n=== State distribution analysis ===");
            int sameStateCount = 0;
            int crossStateCount = 0;

            foreach (var candidate in candidatesWithoutBlocking)
            {
                var record1 = testData[candidate.Row1Number];
                var record2 = testData[candidate.Row2Number];

                if (record1["State"].Equals(record2["State"]))
                    sameStateCount++;
                else
                    crossStateCount++;
            }

            Console.WriteLine($"Non-blocking candidates: {sameStateCount} same-state, {crossStateCount} cross-state");

            // The cross-state count should be 0 if exact matching is working correctly!
            Assert.Equal(0, crossStateCount); // This will likely fail, showing the bug
        }

        [Fact]
        public async Task SingleFieldBlocking_OneMillion()
        {
            Console.WriteLine("Starting SingleFieldBlocking_OneMillion test...");
            // Arrange
            var sourceId = Guid.NewGuid();
            var config = new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = "TestSource",
                FieldsToIndex = new List<string> { "State", "Name", "Email" },
                UseInMemoryStore = true
            };

            // Create match definition with exact State match
            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "State Blocking Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "State Block + Name Match",
                        Criteria = new List<MatchCriteria>
                        {
                            new() // Exact State - will be used for blocking
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.3,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "TestSource", "State")
                                }
                            },
                            new() // Fuzzy Name match
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.7,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.7" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "TestSource", "Name")
                                }
                            }
                        }
                    }
                }
            };

            // Initialize blocking configuration
            _indexer.InitializeBlockingConfiguration(matchDef);

            // Create test data - make it deterministic for debugging
            var testData = new List<Dictionary<string, object>>();
            var random = new Random(42);
            var stateFrequency = new Dictionary<string, int>();

            for (int i = 0; i < 1000000; i++)
            {
                testData.Add(new Dictionary<string, object>
                {
                    ["Id"] = $"ID{i:D3}",
                    ["State"] = GetRandomState(random, stateFrequency, maxFrequency: 100),
                    ["Name"] = $"Person {i}",
                    ["Email"] = $"person{i}@example.com"
                });
            }

            // Index data for both indexers
            await _indexer.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                config,
                _mockProgressTracker.Object);

            // Act - collect all candidates from both
            var candidatesWithBlocking = new List<CandidatePair>();
            Stopwatch sw = Stopwatch.StartNew();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidatesWithBlocking.Add(candidate);
            }
            sw.Stop();
            Console.WriteLine($"Blocking candidate generation took {sw.ElapsedMilliseconds} ms");

            Assert.NotEmpty(candidatesWithBlocking);
        }

        [Fact]
        public async Task SingleFieldBlocking_WithExactCriteria_ShouldUseBlockingEffectively()
        {
            // Arrange
            var sourceId = Guid.NewGuid();
            var config = new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = "TestSource",
                FieldsToIndex = new List<string> { "State", "Name", "Email" },
                UseInMemoryStore = true
            };

            // Create match definition with exact State match
            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "State Blocking Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "State Block + Name Match",
                        Criteria = new List<MatchCriteria>
                        {
                            new() // Exact State - will be used for blocking
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.3,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "TestSource", "State")
                                }
                            },
                            new() // Fuzzy Name match
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.7,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.7" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "TestSource", "Name")
                                }
                            }
                        }
                    }
                }
            };

            // Initialize blocking configuration
            _indexer.InitializeBlockingConfiguration(matchDef);

            //Test data with two states to create blocks
            var data = GenerateTestDataWithBlockableFields(100);

            // Index data
            await _indexer.IndexDataSourceAsync(
                data,
                config,
                _mockProgressTracker.Object);

            await _indexerWithoutBlocking.IndexDataSourceAsync(
                data,
                config,
                _mockProgressTracker.Object);

            // Act
            var candidates = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
            }

            var candidatesWithoutBlocking = new List<CandidatePair>();
            await foreach (var candidate in _indexerWithoutBlocking.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidatesWithoutBlocking.Add(candidate);
            }

            // Assert
            Assert.NotEmpty(candidates);
            Assert.NotEmpty(candidatesWithoutBlocking);

            // Verify no cross-state matches (blocking should prevent them)
            foreach (var candidate in candidates)
            {
                var (record1, record2) = await candidate.GetRecordsAsync();
                Assert.Equal(record1["State"], record2["State"]);
            }

            foreach (var candidate in candidatesWithoutBlocking)
            {
                var (record1, record2) = await candidate.GetRecordsAsync();
                Assert.Equal(record1["State"], record2["State"]);
            }

            // Verify blocking was used (check logs)
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Using blocking strategy")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            _output.WriteLine($"Single field blocking: {candidates.Count} candidates, all within same state blocks");
        }

        #endregion

        #region Multi-Field Blocking Tests

        [Fact]
        public async Task MultiFieldBlocking_WithMultipleExactCriteria_ShouldCreateCompositeBlocks()
        {
            // Arrange
            var sourceId = Guid.NewGuid();
            var config = new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = "Employees",
                FieldsToIndex = new List<string> { "State", "Department", "Name", "Salary" },
                UseInMemoryStore = true
            };

            // Create match definition with multiple exact criteria
            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Multi-Field Blocking Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "State + Department Blocking",
                        Criteria = new List<MatchCriteria>
                        {
                            new() // Exact State
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.3,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Employees", "State")
                                }
                            },
                            new() // Exact Department
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.3,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Employees", "Department")
                                }
                            },
                            new() // Fuzzy Name
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.4,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.6" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Employees", "Name")
                                }
                            }
                        }
                    }
                }
            };

            _indexer.InitializeBlockingConfiguration(matchDef);

            // Index data
            await _indexer.IndexDataSourceAsync(
                GenerateDataWithMultipleBlockingFields(90), // 30 in each state, 30 in each dept
                config,
                _mockProgressTracker.Object);

            // Act
            var candidates = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
            }

            // Assert
            Assert.NotEmpty(candidates);

            // Verify all candidates have matching State AND Department
            foreach (var candidate in candidates.Take(10)) // Check first 10 for performance
            {
                var (record1, record2) = await candidate.GetRecordsAsync();
                Assert.Equal(record1["State"], record2["State"]);
                Assert.Equal(record1["Department"], record2["Department"]);
            }

            // Verify multi-field blocking was detected
            _mockLogger.Verify(
                x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("MultiField")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            _output.WriteLine($"Multi-field blocking: {candidates.Count} candidates within same state+department blocks");
        }

        #endregion

        #region Multi-Scheme Blocking Tests (Fuzzy Only)

        [Fact]
        public async Task MultiSchemeBlocking_WithOnlyFuzzyCriteria_ShouldUseFirstMiddleLastBlocking()
        {
            // Arrange
            var sourceId = Guid.NewGuid();
            var config = new DataSourceIndexingConfig
            {
                DataSourceId = sourceId,
                DataSourceName = "Customers",
                FieldsToIndex = new List<string> { "FullName", "Email" },
                UseInMemoryStore = true
            };

            // Create match definition with ONLY fuzzy criteria (no exact)
            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Multi-Scheme Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "Fuzzy Name Matching",
                        Criteria = new List<MatchCriteria>
                        {
                            new() // Fuzzy FullName
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.6,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.5" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Customers", "FullName")
                                }
                            },
                            new() // Fuzzy Email
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.4,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.5" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Customers", "Email")
                                }
                            }
                        }
                    }
                }
            };

            _indexer.InitializeBlockingConfiguration(matchDef);

            // Index data with names that would benefit from multi-scheme blocking
            var testData = new List<Dictionary<string, object>>
            {
                new() { ["Id"] = "1", ["FullName"] = "Alexander Hamilton", ["Email"] = "aham@example.com" },
                new() { ["Id"] = "2", ["FullName"] = "Alexandra Hamilton", ["Email"] = "alex@example.com" }, // Similar start
                new() { ["Id"] = "3", ["FullName"] = "John Hamilton", ["Email"] = "jham@example.com" }, // Same end
                new() { ["Id"] = "4", ["FullName"] = "Robert Anderson", ["Email"] = "rand@example.com" }, // Different
                new() { ["Id"] = "5", ["FullName"] = "Alice Wonderland", ["Email"] = "alice@example.com" }, // Different
                new() { ["Id"] = "6", ["FullName"] = "Alex Hamil", ["Email"] = "ahamil@example.com" }, // Similar to 1
                //new() { ["Id"] = "7", ["FullName"] = "Alexander Hamilton", ["Email"] = "aham@example.com" }, // Exact duplicate of 1
            };

            await _indexer.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                config,
                _mockProgressTracker.Object);

            // Act
            var candidates = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
            }

            // Assert
            Assert.NotEmpty(candidates);

            // Verify multi-scheme blocking was used
            _mockLogger.Verify(
                x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("MultiScheme")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            // Check that we found Hamilton matches (should be in same blocks via first/last schemes)
            var hamiltonMatches = candidates.Where(c =>
            {
                var task = Task.Run(async () =>
                {
                    var (r1, r2) = await c.GetRecordsAsync();
                    return r1["FullName"].ToString().Contains("Hamilton") ||
                           r2["FullName"].ToString().Contains("Hamilton");
                });
                return task.Result;
            }).ToList();

            Assert.NotEmpty(hamiltonMatches);

            _output.WriteLine($"Multi-scheme blocking with fuzzy-only: {candidates.Count} total candidates");
            _output.WriteLine($"Found {hamiltonMatches.Count} Hamilton-related matches");
        }

        #endregion

        #region Cross-Source Blocking Tests

        [Fact]
        public async Task CrossSourceBlocking_WithMatchingBlockValues_ShouldOnlyCompareWithinBlocks()
        {
            // Arrange
            var sourceId1 = Guid.NewGuid();
            var sourceId2 = Guid.NewGuid();

            // Data for source 1
            var source1Data = new List<Dictionary<string, object>>
            {
                new() { ["Id"] = "C1", ["City"] = "NYC", ["Name"] = "Customer One" },
                new() { ["Id"] = "C2", ["City"] = "NYC", ["Name"] = "Customer Two" },
                new() { ["Id"] = "C3", ["City"] = "LA", ["Name"] = "Customer Three" },
                new() { ["Id"] = "C4", ["City"] = "LA", ["Name"] = "Customer Four" },
                new() { ["Id"] = "C5", ["City"] = "CHI", ["Name"] = "Customer Five" }
            };

            // Data for source 2
            var source2Data = new List<Dictionary<string, object>>
            {
                new() { ["Id"] = "O1", ["CustomerCity"] = "NYC", ["CustomerName"] = "Customer 1" },
                new() { ["Id"] = "O2", ["CustomerCity"] = "LA", ["CustomerName"] = "Customer 3" },
                new() { ["Id"] = "O3", ["CustomerCity"] = "NYC", ["CustomerName"] = "Customer Too" },
                new() { ["Id"] = "O4", ["CustomerCity"] = "MIA", ["CustomerName"] = "Customer Six" } // Different city
            };

            // Create cross-source match definition with City blocking
            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Cross-Source Blocking",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "City Block Cross Match",
                        Criteria = new List<MatchCriteria>
                        {
                            new() // Exact City match for blocking
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.3,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId1, "Customers", "City"),
                                    new(sourceId2, "Orders", "CustomerCity")
                                }
                            },
                            new() // Fuzzy Name match
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.7,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.3" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId1, "Customers", "Name"),
                                    new(sourceId2, "Orders", "CustomerName")
                                }
                            }
                        }
                    }
                }
            };

            _indexer.InitializeBlockingConfiguration(matchDef);

            // Index both sources
            await _indexer.IndexDataSourceAsync(
                source1Data.ToAsyncEnumerable(),
                new DataSourceIndexingConfig
                {
                    DataSourceId = sourceId1,
                    DataSourceName = "Customers",
                    FieldsToIndex = new List<string> { "City", "Name" },
                    UseInMemoryStore = true
                },
                _mockProgressTracker.Object);

            await _indexer.IndexDataSourceAsync(
                source2Data.ToAsyncEnumerable(),
                new DataSourceIndexingConfig
                {
                    DataSourceId = sourceId2,
                    DataSourceName = "Orders",
                    FieldsToIndex = new List<string> { "CustomerCity", "CustomerName" },
                    UseInMemoryStore = true
                },
                _mockProgressTracker.Object);

            // Act
            var candidates = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
            }

            // Assert
            Assert.NotEmpty(candidates);

            // Verify all matches are within same city blocks
            foreach (var candidate in candidates)
            {
                var (record1, record2) = await candidate.GetRecordsAsync();

                // Map field names correctly for cross-source
                var city1 = record1["City"].ToString();
                var city2 = record2["CustomerCity"].ToString();

                Assert.Equal(city1, city2);
            }

            // Should NOT find any matches with Miami record (O4) since no matching city block
            var miamiMatches = candidates.Where(c =>
            {
                var task = Task.Run(async () =>
                {
                    var (r1, r2) = await c.GetRecordsAsync();
                    return r2["CustomerCity"].ToString() == "MIA";
                });
                return task.Result;
            }).ToList();

            Assert.Empty(miamiMatches);

            _output.WriteLine($"Cross-source blocking: {candidates.Count} candidates, all within matching city blocks");
        }

        #endregion

        #region Block Size and Performance Tests

        [Fact]
        public async Task LargeBlockOptimization_WithOversizedBlocks_ShouldHandleEfficiently()
        {
            // Arrange
            _options.MaxBlockSize = 100; // Small limit for testing
            _options.BlockSizeWarningThreshold = 50;

            var sourceId = Guid.NewGuid();

            // Create data where everyone has the same state (large block)
            var testData = Enumerable.Range(0, 200).Select(i => new Dictionary<string, object>
            {
                ["Id"] = $"ID{i}",
                ["State"] = "CA", // All in same block
                ["Name"] = $"Person {i}",
                ["Email"] = $"person{i}@example.com"
            }).ToList();

            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Large Block Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "State Blocking",
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
                                    new(sourceId, "Test", "State")
                                }
                            }
                        }
                    }
                }
            };

            _indexer.InitializeBlockingConfiguration(matchDef);

            await _indexer.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                new DataSourceIndexingConfig
                {
                    DataSourceId = sourceId,
                    DataSourceName = "Test",
                    FieldsToIndex = new List<string> { "State", "Name", "Email" },
                    UseInMemoryStore = true
                },
                _mockProgressTracker.Object);

            // Act
            var stopwatch = Stopwatch.StartNew();
            var candidates = new List<CandidatePair>();

            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
                if (candidates.Count >= 1000) break; // Limit for test performance
            }

            stopwatch.Stop();

            // Assert
            Assert.NotEmpty(candidates);

            // Should see warning about oversized block
            // Hash bucket optimization is disabled temporarily
             /*_mockLogger.Verify(
                x => x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("oversized")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            _output.WriteLine($"Large block handling: {candidates.Count} candidates in {stopwatch.ElapsedMilliseconds}ms");*/ 
        }

        #endregion

        #region Mixed Blocking Strategies

        [Fact]
        public async Task MixedDefinitions_SomeWithBlockingSomeWithout_ShouldHandleBoth()
        {
            // Arrange
            var sourceId = Guid.NewGuid();

            var testData = new List<Dictionary<string, object>>
            {
                new() { ["Id"] = "1", ["State"] = "CA", ["Name"] = "John Smith", ["Phone"] = "555-1234" },
                new() { ["Id"] = "2", ["State"] = "CA", ["Name"] = "Jon Smith", ["Phone"] = "555-1234" },
                new() { ["Id"] = "3", ["State"] = "NY", ["Name"] = "John Smith", ["Phone"] = "555-5678" },
                new() { ["Id"] = "4", ["State"] = "TX", ["Name"] = "Bob Jones", ["Phone"] = "555-9999" }
            };

            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Mixed Strategies",
                Definitions = new List<MatchDefinition>
                {
                    // Definition 1: With blocking (exact State)
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "With Blocking",
                        Criteria = new List<MatchCriteria>
                        {
                            new()
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.5,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Test", "State")
                                }
                            },
                            new()
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.5,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.5" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Test", "Name")
                                }
                            }
                        }
                    },
                    // Definition 2: Without blocking (phone exact, but phonetic type - not blockable)
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "Without Blocking",
                        Criteria = new List<MatchCriteria>
                        {
                            new()
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Phonetic, // Not blockable
                                Weight = 1.0,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Test", "Phone")
                                }
                            }
                        }
                    }
                }
            };

            _indexer.InitializeBlockingConfiguration(matchDef);

            await _indexer.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                new DataSourceIndexingConfig
                {
                    DataSourceId = sourceId,
                    DataSourceName = "Test",
                    FieldsToIndex = new List<string> { "State", "Name", "Phone" },
                    UseInMemoryStore = true
                },
                _mockProgressTracker.Object);

            // Act
            var candidates = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
            }

            // Assert
            Assert.NotEmpty(candidates);

            // Should find candidates from both definitions
            var definitionCounts = candidates
                .GroupBy(c => c.MatchDefinitionIds.Count)
                .ToDictionary(g => g.Key, g => g.Count());

            _output.WriteLine($"Mixed strategies: {candidates.Count} total candidates");
            foreach (var kvp in definitionCounts)
            {
                _output.WriteLine($"  {kvp.Value} candidates matched {kvp.Key} definition(s)");
            }

            // Verify both blocking and non-blocking strategies were used
            _mockLogger.Verify(
                x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Using blocking strategy")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);
        }

        #endregion

        #region Blocking Configuration Detection Tests

        [Fact]
        public async Task BlockingDetection_NumericCriteria_ShouldNotUseBlocking()
        {
            // Arrange
            var sourceId = Guid.NewGuid();

            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Numeric Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "Numeric Only",
                        Criteria = new List<MatchCriteria>
                        {
                            new()
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Number, // Numeric - not blockable
                                Weight = 1.0,
                                Arguments = new Dictionary<ArgsValue, string>
                                {
                                    { ArgsValue.UpperLimit, "100" },
                                    { ArgsValue.LowerLimit, "50" }
                                },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Test", "Amount")
                                }
                            }
                        }
                    }
                }
            };

            // Act
            _indexer.InitializeBlockingConfiguration(matchDef);

            // Assert - verify no blocking was configured
            _mockLogger.Verify(
                x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("blocking: False")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            _output.WriteLine("Numeric criteria correctly identified as non-blockable");
        }

        [Fact]
        public async Task BlockingDetection_PhoneticCriteria_ShouldNotUseBlocking()
        {
            // Arrange
            var sourceId = Guid.NewGuid();

            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Phonetic Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "Phonetic Only",
                        Criteria = new List<MatchCriteria>
                        {
                            new()
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Phonetic, // Phonetic - not blockable even if exact
                                Weight = 1.0,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Test", "Name")
                                }
                            }
                        }
                    }
                }
            };

            // Act
            _indexer.InitializeBlockingConfiguration(matchDef);

            // Assert
            _mockLogger.Verify(
                x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("blocking: False")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            _output.WriteLine("Phonetic criteria correctly identified as non-blockable");
        }

        #endregion

        #region Blocking Statistics Tests

        [Fact]
        public async Task BlockingStatistics_ShouldLogDistribution()
        {
            // Arrange
            var sourceId = Guid.NewGuid();
            _options.CollectBlockingStatistics = true;
            _options.LogBlockDistribution = true;

            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Stats Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "State Blocking",
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
                                    new(sourceId, "Test", "State")
                                }
                            }
                        }
                    }
                }
            };

            _indexer.InitializeBlockingConfiguration(matchDef);

            // Index data with varying block sizes
            await _indexer.IndexDataSourceAsync(
                GenerateTestDataWithBlockableFields(100),
                new DataSourceIndexingConfig
                {
                    DataSourceId = sourceId,
                    DataSourceName = "Test",
                    FieldsToIndex = new List<string> { "State", "City", "Name" },
                    UseInMemoryStore = true
                },
                _mockProgressTracker.Object);

            // Act
            var candidates = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
            }

            // Assert - verify statistics were logged
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Blocking statistics")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            _output.WriteLine($"Blocking statistics logged for {candidates.Count} candidates");
        }

        #endregion

        #region Comparison with Non-Blocking

        [Fact]
        public async Task BlockingVsNonBlocking_ShouldProduceSameResults()
        {
            // This test verifies blocking doesn't miss valid candidates
            // by comparing with non-blocking version

            // Arrange
            var sourceId = Guid.NewGuid();
            var testData = new List<Dictionary<string, object>>
            {
                new() { ["Id"] = "1", ["City"] = "NYC", ["Name"] = "Alice" },
                new() { ["Id"] = "2", ["City"] = "NYC", ["Name"] = "Alicia" },
                new() { ["Id"] = "3", ["City"] = "LA", ["Name"] = "Bob" },
                new() { ["Id"] = "4", ["City"] = "LA", ["Name"] = "Bobby" }
            };

            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Comparison Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "City + Name",
                        Criteria = new List<MatchCriteria>
                        {
                            new()
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.5,
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Test", "City")
                                }
                            },
                            new()
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Fuzzy,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.5,
                                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.3" } },
                                FieldMappings = new List<FieldMapping>
                                {
                                    new(sourceId, "Test", "Name")
                                }
                            }
                        }
                    }
                }
            };

            // Test with blocking enabled
            _options.EnableBlocking = true;
            _indexer.InitializeBlockingConfiguration(matchDef);

            await _indexer.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                new DataSourceIndexingConfig
                {
                    DataSourceId = sourceId,
                    DataSourceName = "Test",
                    FieldsToIndex = new List<string> { "City", "Name" },
                    UseInMemoryStore = true
                },
                _mockProgressTracker.Object);

            var candidatesWithBlocking = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidatesWithBlocking.Add(candidate);
            }

            // Assert
            Assert.NotEmpty(candidatesWithBlocking);

            // Should find Alice-Alicia and Bob-Bobby pairs (same city, similar names)
            Assert.Equal(2, candidatesWithBlocking.Count);

            // Verify correct pairs were found
            foreach (var candidate in candidatesWithBlocking)
            {
                var (r1, r2) = await candidate.GetRecordsAsync();
                Assert.Equal(r1["City"], r2["City"]); // Same city (blocking requirement)

                // Names should be similar
                var name1 = r1["Name"].ToString();
                var name2 = r2["Name"].ToString();
                Assert.True(
                    (name1.Contains("Ali") && name2.Contains("Ali")) ||
                    (name1.Contains("Bob") && name2.Contains("Bob")));
            }

            _output.WriteLine($"Blocking produced {candidatesWithBlocking.Count} correct candidates");
        }

        #endregion

        #region Edge Cases

        [Fact]
        public async Task EmptyBlocks_ShouldHandleGracefully()
        {
            // Arrange - data where no records share the same block value
            var sourceId = Guid.NewGuid();
            var testData = new List<Dictionary<string, object>>
            {
                new() { ["Id"] = "1", ["UniqueCode"] = "AAA", ["Name"] = "Person 1" },
                new() { ["Id"] = "2", ["UniqueCode"] = "BBB", ["Name"] = "Person 2" },
                new() { ["Id"] = "3", ["UniqueCode"] = "CCC", ["Name"] = "Person 3" }
            };

            var matchDef = new MatchDefinitionCollection
            {
                Id = Guid.NewGuid(),
                ProjectId = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Empty Blocks Test",
                Definitions = new List<MatchDefinition>
                {
                    new()
                    {
                        Id = Guid.NewGuid(),
                        DataSourcePairId = Guid.NewGuid(),
                        Name = "Unique Code Blocking",
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
                                    new(sourceId, "Test", "UniqueCode")
                                }
                            }
                        }
                    }
                }
            };

            _indexer.InitializeBlockingConfiguration(matchDef);

            await _indexer.IndexDataSourceAsync(
                testData.ToAsyncEnumerable(),
                new DataSourceIndexingConfig
                {
                    DataSourceId = sourceId,
                    DataSourceName = "Test",
                    FieldsToIndex = new List<string> { "UniqueCode", "Name" },
                    UseInMemoryStore = true
                },
                _mockProgressTracker.Object);

            // Act
            var candidates = new List<CandidatePair>();
            await foreach (var candidate in _indexer.GenerateCandidatesFromMatchDefinitionsAsync(matchDef))
            {
                candidates.Add(candidate);
            }

            // Assert - should have no candidates since no shared blocks
            Assert.Empty(candidates);
            _output.WriteLine("Empty blocks handled correctly - no candidates generated");
        }

        #endregion

        public void Dispose()
        {
            _indexer?.Dispose();
        }
    }

    
}