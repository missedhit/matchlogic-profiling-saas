using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

namespace MatchLogic.Application.UnitTests;
public class RecordComparisonServiceTests
{
    private readonly Mock<ILogger<RecordComparisonService>> _loggerMock;
    private readonly Mock<ITelemetry> _telemetryMock;
    private readonly Mock<IComparatorBuilder> _comparatorBuilderMock; //
    private readonly Mock<IComparator> _comparatorMock;
    private readonly RecordLinkageOptions _options;
    private readonly RecordComparisonService _service;

    public RecordComparisonServiceTests()
    {
        _loggerMock = new Mock<ILogger<RecordComparisonService>>();
        _telemetryMock = new Mock<ITelemetry>();
        _comparatorBuilderMock = new Mock<IComparatorBuilder>();
        _comparatorMock = new Mock<IComparator>();
        _options = new RecordLinkageOptions
        {
            BatchSize = 100,
            MaxDegreeOfParallelism = 4
        };

        _telemetryMock.Setup(t => t.MeasureOperation(It.IsAny<string>()))
            .Returns(new Mock<IDisposable>().Object);

        _comparatorBuilderMock.Setup(b => b.WithArgs(It.IsAny<Dictionary<ArgsValue, string>>()))
    .Returns(_comparatorBuilderMock.Object);
        _comparatorBuilderMock.Setup(b => b.Build())
            .Returns(_comparatorMock.Object);

        _service = new RecordComparisonService(
            _loggerMock.Object,
            _telemetryMock.Object,
            _comparatorBuilderMock.Object,
            Options.Create(_options));
    }

    private static async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> CreateAsyncPairs(
        IEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> pairs,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var pair in pairs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return pair;
        }
    }
    [Fact]
    public async Task CompareRecordsAsync_WithNoRecords_ReturnsEmptyResults()
    {
        // Arrange
        var candidatePairs = AsyncEnumerable.Empty<(IDictionary<string, object>, IDictionary<string, object>)>();
        var criteria = Array.Empty<MatchCriteria>();

        // Act
        var results = new List<IDictionary<string, object>>();
        await foreach (var result in _service.CompareRecordsAsync(candidatePairs, criteria))
        {
            results.Add(result);
        }

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task CompareRecordsAsync_WithNoCriteria_ReturnsMatchesWithPerfectScore()
    {
        // Arrange
        var record1 = new Dictionary<string, object> { ["field1"] = "value1" };
        var record2 = new Dictionary<string, object> { ["field1"] = "value2" };
        var pair = (record1, record2);
        List<(IDictionary<string, object>, IDictionary<string, object>)> a = [pair];
        var candidatePairs = CreateAsyncPairs(a);

        var criteria = Array.Empty<MatchCriteria>();

        // Act
        var results = new List<IDictionary<string, object>>();
        await foreach (var result in _service.CompareRecordsAsync(candidatePairs, criteria))
        {
            results.Add(result);
        }

        // Assert
        Assert.Single(results);
        var returnItem = results[0];
        Assert.Equal(double.PositiveInfinity, returnItem[RecordComparisonService.FinalScoreField]);
        Assert.Equal(double.PositiveInfinity, returnItem[RecordComparisonService.WeightedScoreField]);
    }

    [Fact]
    public async Task CompareRecordsAsync_WithSingleCriterion_CalculatesScoreCorrectly()
    {
        // Arrange
        var record1 = new Dictionary<string, object> { ["name"] = "John" };
        var record2 = new Dictionary<string, object> { ["name"] = "Jon" };
        var pair = (record1, record2);
        List<(IDictionary<string, object>, IDictionary<string, object>)> a = [pair];
        var candidatePairs = CreateAsyncPairs(a);

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                Weight = 1.0,
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text
            }
        };

        _comparatorMock.Setup(c => c.Compare(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(0.8);

        // Act
        var results = new List<IDictionary<string, object>>();
        await foreach (var keyValues in _service.CompareRecordsAsync(candidatePairs, criteria))
        {
            results.Add(keyValues);
        }

        // Assert
        Assert.Single(results);
        var result = results[0];
        Assert.Equal(0.8, result[RecordComparisonService.FinalScoreField]);
        Assert.Equal(0.8, result[RecordComparisonService.WeightedScoreField]);
        Assert.Equal(0.8, result["name_Score"]);
        Assert.Equal(1.0, result["name_Weight"]);
    }

    [Fact]
    public async Task CompareRecordsAsync_WithMissingField_SkipsRecord()
    {
        // Arrange
        var record1 = new Dictionary<string, object> { ["name"] = "John" };
        var record2 = new Dictionary<string, object> { ["different_field"] = "Jon" };
        var pair = (record1, record2);
        List<(IDictionary<string, object>, IDictionary<string, object>)> a = [pair];
        var candidatePairs = CreateAsyncPairs(a);

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                Weight = 1.0,
                MatchingType = MatchingType.Fuzzy
            }
        };

        // Act
        var results = new List<IDictionary<string, object>>();
        await foreach (var result in _service.CompareRecordsAsync(candidatePairs, criteria))
        {
            results.Add(result);
        }

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task CompareRecordsAsync_WithScoreBelowThreshold_SkipsRecord()
    {
        // Arrange
        var record1 = new Dictionary<string, object> { ["name"] = "John" };
        var record2 = new Dictionary<string, object> { ["name"] = "Different" };
        var pair = (record1, record2);
        List<(IDictionary<string, object>, IDictionary<string, object>)> a = [pair];
        var candidatePairs = CreateAsyncPairs(a);

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                Weight = 1.0,
                MatchingType = MatchingType.Fuzzy
            }
        };

        _comparatorMock.Setup(c => c.Compare(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(0.0);

        // Act
        var results = new List<IDictionary<string, object>>();
        await foreach (var result in _service.CompareRecordsAsync(candidatePairs, criteria))
        {
            results.Add(result);
        }

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task CompareRecordsAsync_WithExactMatch_HandlesCorrectly()
    {
        // Arrange
        var record1 = new Dictionary<string, object> { ["id"] = "123" };
        var record2 = new Dictionary<string, object> { ["id"] = "123" };
        var pair = (record1, record2);
        List<(IDictionary<string, object>, IDictionary<string, object>)> a = [pair];
        var candidatePairs = CreateAsyncPairs(a);

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "id",
                Weight = 1.0,
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
              Arguments = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.Level, "0.8" }
        }
            }
        };

        // Act
        var results = new List<IDictionary<string, object>>();
        _comparatorMock.Setup(c => c.Compare("123", "123")).Returns(1);
        await foreach (var result in _service.CompareRecordsAsync(candidatePairs, criteria))
        {
            results.Add(result);
        }

        // Assert
        Assert.Single(results);
        var returnItem = results[0];
        Assert.Equal(1.0, returnItem[RecordComparisonService.FinalScoreField]);
    }

    [Fact]
    public async Task CompareRecordsAsync_WithMultipleCriteria_CalculatesWeightedScoreCorrectly()
    {
        // Arrange
        var record1 = new Dictionary<string, object>
        {
            ["firstName"] = "John",
            ["lastName"] = "Smith"
        };
        var record2 = new Dictionary<string, object>
        {
            ["firstName"] = "Jon",
            ["lastName"] = "Smith"
        };
        var pair = (record1, record2);
        List<(IDictionary<string, object>, IDictionary<string, object>)> a = [pair];
        var candidatePairs = CreateAsyncPairs(a);

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "firstName",
                Weight = 0.4,
                MatchingType = MatchingType.Fuzzy,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.Level, "0.8" }
                }
            },
            new MatchCriteria
            {
                FieldName = "lastName",
                Weight = 0.6,
                MatchingType = MatchingType.Fuzzy,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.Level, "0.8" }
                }
            }
        };

        _comparatorMock.Setup(c => c.Compare("John", "Jon")).Returns(0.8);
        _comparatorMock.Setup(c => c.Compare("Smith", "Smith")).Returns(1.0);

        // Act
        var results = new List<IDictionary<string, object>>();
        await foreach (var result in _service.CompareRecordsAsync(candidatePairs, criteria))
        {
            results.Add(result);
        }

        // Assert
        Assert.Single(results);
        var returnItem = results[0];
        Assert.Equal(0.9, returnItem[RecordComparisonService.FinalScoreField]);
        Assert.Equal(0.92, returnItem[RecordComparisonService.WeightedScoreField]);
    }

    [Fact]
    public async Task CompareRecordsAsync_WithCancellation_StopsProcessing()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var record1 = new Dictionary<string, object> { ["name"] = "John" };
        var record2 = new Dictionary<string, object> { ["name"] = "Jon" };
        async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> GeneratePairs(
            [EnumeratorCancellation] CancellationToken token)
        {
            yield return (record1, record2);
            await Task.Delay(100, token); // Small delay to ensure cancellation happens
            cts.Cancel();
            token.ThrowIfCancellationRequested();
            yield return (record1, record2); // This should never be reached
        }

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                Weight = 1.0,
                MatchingType = MatchingType.Fuzzy
            }
        };

        _comparatorMock.Setup(c => c.Compare(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(0.8);

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var result in _service.CompareRecordsAsync(GeneratePairs(cts.Token), criteria, cts.Token))
            {
                // Should process first pair then throw on cancellation
            }
        });
    }

    [Fact]
    public void ResetPairIdCounter_ResetsCounter()
    {
        // Arrange
        var record1 = new Dictionary<string, object> { ["name"] = "John" };
        var record2 = new Dictionary<string, object> { ["name"] = "Jon" };
        var pair = (record1, record2);
        List<(IDictionary<string, object>, IDictionary<string, object>)> a = [pair];
        var candidatePairs = CreateAsyncPairs(a);

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                Weight = 1.0,
                MatchingType = MatchingType.Fuzzy
            }
        };

        _comparatorMock.Setup(c => c.Compare(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(0.8);

        // Act
        _service.ResetPairIdCounter();

        // Assert
        // Process one pair and verify the ID starts from 1
        var result = _service.CompareRecordsAsync(candidatePairs, criteria).FirstAsync().Result;
        Assert.Equal(1L, result[RecordComparisonService.PairIdField]);
    }

    [Fact]
    public async Task DisposeAsync_DisposesCorrectly()
    {
        // Act
        await _service.DisposeAsync();

        // Arrange & Assert
        var candidatePairs = AsyncEnumerable.Empty<(IDictionary<string, object>, IDictionary<string, object>)>();
        var criteria = Array.Empty<MatchCriteria>();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await foreach (var result in _service.CompareRecordsAsync(candidatePairs, criteria))
            {
                // Should throw before getting here
            }
        });
    }

    [Fact]
    public async Task DisposeAsync_CalledMultipleTimes_OnlyDisposesOnce()
    {
        // Act
        await _service.DisposeAsync();
        await _service.DisposeAsync(); // Should not throw

        // Assert
        // Verification is implicit in the fact that the second call doesn't throw
    }
}
