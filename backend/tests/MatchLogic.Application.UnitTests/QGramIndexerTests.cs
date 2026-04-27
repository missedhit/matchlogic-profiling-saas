using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class QGramIndexerTests : IDisposable
{
    private readonly ILogger _logger;
    private readonly ILogger<OptimizedQGramIndexer> _loggerForOptimizedQGramIndexer;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker;
    public QGramIndexerTests()
    {
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<QGramIndexer>();
        _mockProgressTracker = new Mock<IStepProgressTracker>();
        _loggerForOptimizedQGramIndexer = loggerFactory.CreateLogger<OptimizedQGramIndexer>();
    }

    [Fact]
    public async Task CreateIndexAsync_ShouldCreateCorrectIndex()
    {
        // Arrange
        var fieldsToIndex = new Dictionary<string, double>
        {
            { "name", 0.7 },
            { "address", 0.6 }
        };
        var indexer = new QGramIndexer(q: 2, fieldsToIndex, _logger);
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } },
            new Dictionary<string, object> { { "name", "Jane Doe" }, { "address", "456 Elm St" } }
        };

        // Act
        var (invertedIndex, entries) = await indexer.CreateIndexAsync(records.ToAsyncEnumerable(),_mockProgressTracker.Object);

        // Assert
        Assert.Equal(2, entries.Count);
        Assert.Equal(2, invertedIndex.Count);
        Assert.True(invertedIndex.ContainsKey("name"));
        Assert.True(invertedIndex.ContainsKey("address"));

        // Check if "Jo" q-gram hash exists in the name index
        var joHash = indexer.HashQGram("Jo");
        Assert.True(invertedIndex["name"].ContainsKey(joHash));
        Assert.Contains(0, invertedIndex["name"][joHash]);
    }

    [Fact]
    public async Task GenerateCandidatePairsAsync_ShouldFindMatchingPairs()
    {
        // Arrange
        var fieldsToIndex = new Dictionary<string, double>
        {
            { "name", 0.9 },
            { "address", 0.9 }
        };
        var indexer = new QGramIndexer(q: 2, fieldsToIndex, _logger);
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } },
            new Dictionary<string, object> { { "name", "Jane Doe" }, { "address", "456 Elm St" } },
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } }
        };

        // Act
        var (invertedIndex, entries) = await indexer.CreateIndexAsync(records.ToAsyncEnumerable(), _mockProgressTracker.Object);
        var candidatePairs = await indexer.GenerateCandidatePairsAsync(invertedIndex, entries, _mockProgressTracker.Object).ToListAsync();

        // Assert
        Assert.Single(candidatePairs);
        var (record1, record2) = candidatePairs[0];
        Assert.Equal(records[0], record1);
        Assert.Equal(records[2], record2);
    }

    [Fact]
    public async Task GenerateCandidatePairsAsync_ShouldFindMatchingPairs_Optimized()
    {
        // Arrange
        var fieldsToIndex = new Dictionary<string, double>
        {
            { "name", 0.9 },
            { "address", 0.9 }
        };
        var indexer = new OptimizedQGramIndexer(q: 2, fieldsToIndex, _loggerForOptimizedQGramIndexer);
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } },
            new Dictionary<string, object> { { "name", "Jane Doe" }, { "address", "456 Elm St" } },
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } }
        };

        // Act
        var (invertedIndex, entries) = await indexer.CreateIndexAsync(records.ToAsyncEnumerable(), _mockProgressTracker.Object);
        var candidatePairs = await indexer.GenerateCandidatePairsAsync(invertedIndex, entries, _mockProgressTracker.Object).ToListAsync();

        // Assert
        Assert.Single(candidatePairs);
        var (record1, record2) = candidatePairs[0];
        Assert.Equal(records[0], record1);
        Assert.Equal(records[2], record2);
    }

    [Fact]
    public async Task GenerateCandidatePairsAsync_ShouldFindMatchingPairs_Optimized_DemoFile1()
    {
        // Arrange
        var fieldsToIndex = new Dictionary<string, double>
        {
            { "name", 0.6 }//,
            //{ "address", 0.9 }
        };
        var indexer = new OptimizedQGramIndexer(q: 3, fieldsToIndex, _loggerForOptimizedQGramIndexer);
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "name", "Dennys" } },//, { "address", "123 Main St" } },
            new Dictionary<string, object> { { "name", "Denny's" } } //, { "address", "456 Elm St" } },
        };

        // Act
        var (invertedIndex, entries) = await indexer.CreateIndexAsync(records.ToAsyncEnumerable(), _mockProgressTracker.Object);
        var candidatePairs = await indexer.GenerateCandidatePairsAsync(invertedIndex, entries, _mockProgressTracker.Object).ToListAsync();

        // Assert
        Assert.Single(candidatePairs);
        var (record1, record2) = candidatePairs[0];
        Assert.Equal(records[0], record1);
        Assert.Equal(records[1], record2);
    }

    [Fact]
    public async Task GenerateCandidatePairsAsync_ShouldFindMatchingPairsWithLowThreshold()
    {
        // Arrange
        var fieldsToIndex = new Dictionary<string, double>
        {
            { "name", 0.1 },
            { "address", 0.1 }
        };
        var indexer = new QGramIndexer(q: 2, fieldsToIndex, _logger);
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } },
            new Dictionary<string, object> { { "name", "Jane Doe" }, { "address", "456 Elm St" } },
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } }
        };

        // Act
        var (invertedIndex, entries) = await indexer.CreateIndexAsync(records.ToAsyncEnumerable(), _mockProgressTracker.Object);
        var candidatePairs = await indexer.GenerateCandidatePairsAsync(invertedIndex, entries, _mockProgressTracker.Object).ToListAsync();

        // Assert
        Assert.Equal(3, candidatePairs.Count);
        Assert.Contains(candidatePairs, pair => AreRecordsSame(pair.Item1, records[0]) && AreRecordsSame(pair.Item2, records[2]));
        Assert.Contains(candidatePairs, pair => AreRecordsSame(pair.Item1, records[0]) && AreRecordsSame(pair.Item2, records[1]));
        Assert.Contains(candidatePairs, pair => AreRecordsSame(pair.Item1, records[1]) && AreRecordsSame(pair.Item2, records[2]));
    }

    [Fact]
    public async Task GenerateCandidatePairsAsync_ShouldNotFindPairsWhenNoMatches()
    {
        // Arrange
        var fieldsToIndex = new Dictionary<string, double>
        {
            { "name", 0.7 },
            { "address", 0.6 }
        };
        var indexer = new QGramIndexer(q: 2, fieldsToIndex, _logger);
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "name", "John Doe" }, { "address", "123 Main St" } },
            new Dictionary<string, object> { { "name", "Jane Smith" }, { "address", "456 Elm St" } },
            new Dictionary<string, object> { { "name", "Alice Johnson" }, { "address", "789 Oak Ave" } }
        };

        // Act
        var (invertedIndex, entries) = await indexer.CreateIndexAsync(records.ToAsyncEnumerable(), _mockProgressTracker.Object);
        var candidatePairs = await indexer.GenerateCandidatePairsAsync(invertedIndex, entries, _mockProgressTracker.Object).ToListAsync();

        // Assert
        Assert.Empty(candidatePairs);
    }

    [Fact]
    public async Task ComprehensiveTest_LargeDatasetWithMultipleFields()
    {
        // Arrange
        int q = 2;
        var fieldsToIndex = new Dictionary<string, double>
        {
            { "firstName", 0.8 },
            { "lastName", 0.9 },
            { "address", 0.7 },
            { "city", 0.8 },
            { "email", 0.9 }
        };
        var indexer = new QGramIndexer(q, fieldsToIndex, _logger);
        var records = GenerateLargeTestDataset();

        // Act
        var (invertedIndex, entries) = await indexer.CreateIndexAsync(records.ToAsyncEnumerable(), _mockProgressTracker.Object);
        var candidatePairs = await indexer.GenerateCandidatePairsAsync(invertedIndex, entries, _mockProgressTracker.Object).ToListAsync();

        // Assert
        Assert.Equal(1050, entries.Count);
        Assert.Equal(5, invertedIndex.Count);
        Assert.All(invertedIndex.Keys, key => Assert.True(fieldsToIndex.ContainsKey(key)));

        // Check if we have some candidate pairs
        Assert.True(candidatePairs.Count > 0, "Expected to find some candidate pairs");

        // Verify some of the candidate pairs
        foreach (var (record1, record2) in candidatePairs.Take(5))
        {
            // Check if all fields are similar enough
            Assert.True(
                IsFieldSimilar(record1["firstName"], record2["firstName"], 0.8, q) &&
                IsFieldSimilar(record1["lastName"], record2["lastName"], 0.9, q) &&
                IsFieldSimilar(record1["address"], record2["address"], 0.7, q) &&
                IsFieldSimilar(record1["city"], record2["city"], 0.8, q) &&
                IsFieldSimilar(record1["email"], record2["email"], 0.9, q),
                "Candidate pair doesn't meet similarity threshold for any field"
            );
        }

        // Log some statistics
        _logger.LogInformation("Total records: {RecordCount}", records.Count);
        _logger.LogInformation("Total candidate pairs: {PairCount}", candidatePairs.Count);
        _logger.LogInformation("Pair ratio: {PairRatio}", (double)candidatePairs.Count / records.Count);
    }

    private bool AreRecordsSame(IDictionary<string, object> record1, IDictionary<string, object> record2)
    {
        if (record1.Count != record2.Count) return false;
        return record1.All(kvp => record2.TryGetValue(kvp.Key, out var value) &&
                                 value.ToString() == kvp.Value.ToString());
    }
    private List<Dictionary<string, object>> GenerateLargeTestDataset()
    {
        var random = new Random(42); // Use a seed for reproducibility
        var dataset = new List<Dictionary<string, object>>();

        string[] firstNames = { "John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Lisa", "William", "Mary" };
        string[] lastNames = { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez" };
        string[] cities = { "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose" };
        string[] streetTypes = { "St", "Ave", "Blvd", "Ln", "Rd" };

        for (int i = 0; i < 1000; i++)
        {
            var firstName = firstNames[random.Next(firstNames.Length)];
            var lastName = lastNames[random.Next(lastNames.Length)];
            var city = cities[random.Next(cities.Length)];
            var streetNumber = random.Next(1, 9999);
            var streetName = GenerateRandomString(random, 5, 10);
            var streetType = streetTypes[random.Next(streetTypes.Length)];

            var record = new Dictionary<string, object>
            {
                { "firstName", firstName },
                { "lastName", lastName },
                { "address", $"{streetNumber} {streetName} {streetType}" },
                { "city", city },
                { "email", $"{firstName.ToLower()}.{lastName.ToLower()}@example.com" }
            };

            // Introduce some variations and errors
            if (random.Next(100) < 10) // 10% chance of typo in first name
            {
                record["firstName"] = IntroduceTypo(firstName);
            }
            if (random.Next(100) < 5) // 5% chance of typo in last name
            {
                record["lastName"] = IntroduceTypo(lastName);
            }
            if (random.Next(100) < 15) // 15% chance of different email format
            {
                record["email"] = $"{firstName[0].ToString().ToLower()}{lastName.ToLower()}@example.com";
            }
            if (random.Next(100) < 20) // 20% chance of abbreviation in address
            {
                record["address"] = ((string)record["address"]).Replace("Street", "St").Replace("Avenue", "Ave");
            }

            dataset.Add(record);
        }

        // Add some intentional near-duplicates
        for (int i = 0; i < 50; i++)
        {
            var originalIndex = random.Next(dataset.Count);
            var nearDuplicate = new Dictionary<string, object>(dataset[originalIndex]);

            // Modify one or two fields slightly
            var fieldsToModify = random.Next(1, 3);
            for (int j = 0; j < fieldsToModify; j++)
            {
                var fieldToModify = nearDuplicate.Keys.ElementAt(random.Next(nearDuplicate.Count));
                nearDuplicate[fieldToModify] = IntroduceTypo((string)nearDuplicate[fieldToModify]);
            }

            dataset.Add(nearDuplicate);
        }

        return dataset;
    }

    private string GenerateRandomString(Random random, int minLength, int maxLength)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int length = random.Next(minLength, maxLength + 1);
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
    }

    private string IntroduceTypo(string input)
    {
        var random = new Random();
        var chars = input.ToCharArray();
        int index = random.Next(chars.Length);
        chars[index] = (char)('a' + random.Next(26)); // Replace with a random lowercase letter
        return new string(chars);
    }

    private bool IsFieldSimilar(object field1, object field2, double threshold, int q)
    {
        if (field1 is string s1 && field2 is string s2)
        {
            return CalculateJaccardSimilarity(s1, s2, q) >= threshold;
        }
        return false;
    }

    private double CalculateJaccardSimilarity(string s1, string s2, int q)
    {
        var set1 = new HashSet<string>(GenerateQGrams(s1, q));
        var set2 = new HashSet<string>(GenerateQGrams(s2, q));

        int intersectionCount = set1.Intersect(set2).Count();
        int unionCount = set1.Union(set2).Count();

        return (double)intersectionCount / unionCount;
    }

    private IEnumerable<string> GenerateQGrams(string input, int q)
    {
        if (string.IsNullOrEmpty(input) || input.Length < q)
            yield return input;
        else
            for (int i = 0; i <= input.Length - q; i++)
                yield return input.Substring(i, q);
    }


    public void Dispose()
    {
    }
}


