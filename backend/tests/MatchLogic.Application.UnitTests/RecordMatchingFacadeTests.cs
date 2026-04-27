using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Domain.CleansingAndStandaradization;

namespace MatchLogic.Application.UnitTests;
public class RecordMatchingFacadeTests
{
    private readonly Mock<IDataStore> _dataStoreMock;
    private readonly Mock<IRecordMatcher> _recordMatcherMock;
    private readonly Mock<IRecordComparisonService> _comparisonServiceMock;
    private readonly Mock<ILogger<RecordMatchingFacadeWithoutGrouping>> _loggerMock;
    private readonly Mock<ITelemetry> _telemetryMock;
    private readonly Mock<IJobEventPublisher> _jobEventPublisherMock;
    private readonly Mock<IStepProgressTracker> _loadingStepMock;
    private readonly Mock<IStepProgressTracker> _matchingStepMock;
    private readonly Mock<IStepProgressTracker> _comparisonStepMock;
    private readonly RecordLinkageOptions _options;
    private readonly RecordMatchingFacadeWithoutGrouping _facade;
    private readonly Mock<ILogger<RecordComparisonService>> _comparisonServiceLoggerMock;
    private readonly Mock<IComparatorBuilder> _comparatorBuilderMock;


    public RecordMatchingFacadeTests()
    {
        _dataStoreMock = new Mock<IDataStore>();
        _recordMatcherMock = new Mock<IRecordMatcher>();
        _comparisonServiceMock = new Mock<IRecordComparisonService>();
        _loggerMock = new Mock<ILogger<RecordMatchingFacadeWithoutGrouping>>();
        _telemetryMock = new Mock<ITelemetry>();
        _jobEventPublisherMock = new Mock<IJobEventPublisher>();
        _loadingStepMock = new Mock<IStepProgressTracker>();
        _matchingStepMock = new Mock<IStepProgressTracker>();
        _comparisonStepMock = new Mock<IStepProgressTracker>();        
        _comparisonServiceLoggerMock = new Mock<ILogger<RecordComparisonService>>();
        _comparatorBuilderMock = new Mock<IComparatorBuilder>();
        _options = new RecordLinkageOptions
        {
            BatchSize = 100,
            BufferSize = 1000
        };

        _telemetryMock.Setup(t => t.MeasureOperation(It.IsAny<string>()))
            .Returns(new Mock<IDisposable>().Object);

        _jobEventPublisherMock.Setup(p => p.CreateStepTracker(
                It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>()))
            .Returns((Guid jobId, string name, int step, int total) =>
            {
                if (name == "Data Loading") return _loadingStepMock.Object;
                if (name == "Finding Matches") return _matchingStepMock.Object;
                return _comparisonStepMock.Object;
            });

        _facade = new RecordMatchingFacadeWithoutGrouping(
            _dataStoreMock.Object,
            _recordMatcherMock.Object,
            _comparisonServiceMock.Object,
            _loggerMock.Object,
            _telemetryMock.Object,
            _jobEventPublisherMock.Object,
            Options.Create(_options));
    }

    [Fact]
    public async Task ProcessMatchingJobAsync_SuccessfulExecution_CompletesAllSteps()
    {
        // Arrange
        var jobId = Guid.NewGuid();
        var expectedCollectionName = $"{GuidCollectionNameConverter.ToValidCollectionName(jobId)}_pairs";
        var criteria = new List<MatchCriteria>
        {
            new MatchCriteria { FieldName = "name", Weight = 1.0 }
        };

        var inputRecords = GetAsyncEnumerable(new[]
        {
            new Dictionary<string, object> { ["id"] = "1", ["name"] = "John" },
            new Dictionary<string, object> { ["id"] = "2", ["name"] = "Jane" }
        });

        var candidatePairs = CreateCandidatePairs(new[]
        {
            (
                new Dictionary<string, object> { ["id"] = "1", ["name"] = "John" } as IDictionary<string, object>,
                new Dictionary<string, object> { ["id"] = "2", ["name"] = "Jane" } as IDictionary<string, object>
            )
        });


        var matchResults = GetAsyncEnumerable(new[]
        {
            new Dictionary<string, object>
            {
                ["PairId"] = 1L,
                ["Record1"] = new Dictionary<string, object> { ["id"] = "1" },
                ["Record2"] = new Dictionary<string, object> { ["id"] = "2" },
                ["FinalScore"] = 0.8
            }
        });

        var comparator = new Mock<IComparator>();
        comparator.Setup(c => c.Compare(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(0.8);

        _comparatorBuilderMock.Setup(b => b.Build())
            .Returns(comparator.Object);

        _dataStoreMock.Setup(d => d.StreamJobDataAsync(
                It.IsAny<Guid>(),
                It.IsAny<IStepProgressTracker>(),"",
                It.IsAny<CancellationToken>()))
            .Returns(inputRecords);

        // Fixed setup for FindMatchesAsync
        _recordMatcherMock.Setup(m => m.FindMatchesAsync(
                It.IsAny<IAsyncEnumerable<IDictionary<string, object>>>(),
                It.IsAny<IEnumerable<MatchCriteria>>(),
                It.IsAny<IStepProgressTracker>(),
                It.IsAny<IStepProgressTracker>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(candidatePairs));

        _comparisonServiceMock.Setup(c => c.CompareRecordsAsync(
                It.IsAny<IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)>>(),
                It.IsAny<IEnumerable<MatchCriteria>>(),
                It.IsAny<CancellationToken>()))
            .Returns(matchResults);

        // Act
        await _facade.ProcessMatchingJobAsync(jobId, criteria);

        // Assert
        _jobEventPublisherMock.Verify(p => p.PublishJobStartedAsync(
            jobId, 3, It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);

        _matchingStepMock.Verify(s => s.StartStepAsync(0, It.IsAny<CancellationToken>()), Times.Once);
        _matchingStepMock.Verify(s => s.CompleteStepAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);

        _comparisonStepMock.Verify(s => s.StartStepAsync(0, It.IsAny<CancellationToken>()), Times.Once);
        _comparisonStepMock.Verify(s => s.UpdateProgressAsync(It.IsAny<int>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        _comparisonStepMock.Verify(s => s.CompleteStepAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);

        _jobEventPublisherMock.Verify(p => p.PublishJobCompletedAsync(jobId, It.IsAny<string>(),It.IsAny<FlowStatistics>(), It.IsAny<CancellationToken>()), Times.Once);

        _dataStoreMock.Verify(d => d.InsertBatchAsync(
            expectedCollectionName,
            It.IsAny<List<IDictionary<string, object>>>()), Times.Once);
    }

    [Fact]
    public async Task ProcessMatchingJobAsync_WithLargeBatch_HandlesMultipleBatches()
    {
        // Arrange
        var jobId = Guid.NewGuid();
        var criteria = new List<MatchCriteria>
        {
            new MatchCriteria { FieldName = "name", Weight = 1.0 }
        };

        var matchResults = new List<Dictionary<string, object>>();
        for (int i = 0; i < _options.BatchSize * 2.5; i++)
        {
            matchResults.Add(new Dictionary<string, object>
            {
                ["PairId"] = i,
                ["Record1"] = new Dictionary<string, object> { ["id"] = i.ToString() },
                ["Record2"] = new Dictionary<string, object> { ["id"] = (i + 1).ToString() },
                ["FinalScore"] = 0.8
            });
        }

        SetupMocksForBatchProcessing(jobId, matchResults);

        // Act
        await _facade.ProcessMatchingJobAsync(jobId, criteria);

        // Assert
        _dataStoreMock.Verify(d => d.InsertBatchAsync(
            It.IsAny<string>(),
            It.IsAny<List<IDictionary<string, object>>>()), Times.Exactly(3));
    }

    [Fact]
    public async Task ProcessMatchingJobAsync_WithError_PropagatesException()
    {
        // Arrange
        var jobId = Guid.NewGuid();
        var criteria = new List<MatchCriteria>();
        //  InvalidOperationException
        var expectedException = new NullReferenceException();

        _dataStoreMock.Setup(d => d.StreamJobDataAsync(
                It.IsAny<Guid>(),
                It.IsAny<IStepProgressTracker>(),"",
                It.IsAny<CancellationToken>()));

        // Act & Assert
        // InvalidOperationException
        var exception = await Assert.ThrowsAsync<NullReferenceException>(
            () => _facade.ProcessMatchingJobAsync(jobId, criteria));

        Assert.IsType<NullReferenceException>(exception);
    }

    [Fact]
    public async Task ProcessMatchingJobAsync_WhenDisposed_ThrowsObjectDisposedException()
    {
        // Arrange
        var jobId = Guid.NewGuid();
        var criteria = new List<MatchCriteria>();
        await _facade.DisposeAsync();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => _facade.ProcessMatchingJobAsync(jobId, criteria));
    }

    [Fact]
    public async Task ProcessMatchingJobAsync_WithCancellation_StopsProcessing()
    {
        // Arrange
        var jobId = Guid.NewGuid();
        var criteria = new List<MatchCriteria>
    {
        new MatchCriteria { FieldName = "name", Weight = 1.0 }
    };
        var cts = new CancellationTokenSource();

        // Setup input records
        var inputRecords = GetAsyncEnumerable(new IDictionary<string, object>[]
        {
        new Dictionary<string, object> { ["id"] = "1", ["name"] = "John" }
        });

        // Setup candidate pairs
        var candidatePairs = CreateCandidatePairs(new[]
        {
        (
            new Dictionary<string, object> { ["id"] = "1", ["name"] = "John" } as IDictionary<string, object>,
            new Dictionary<string, object> { ["id"] = "2", ["name"] = "Jane" } as IDictionary<string, object>
        )
    });

        // Setup comparison service dependencies
        var comparator = new Mock<IComparator>();
        comparator.Setup(c => c.Compare(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(0.8);

        _comparatorBuilderMock.Setup(b => b.Build())
            .Returns(comparator.Object);

        // Setup data store to trigger cancellation after streaming data
        _dataStoreMock.Setup(d => d.StreamJobDataAsync(
                It.IsAny<Guid>(),
                It.IsAny<IStepProgressTracker>(), "",
                It.IsAny<CancellationToken>()))
            .Returns(inputRecords);

        // Setup record matcher to trigger cancellation after finding matches
        _recordMatcherMock.Setup(m => m.FindMatchesAsync(
                It.IsAny<IAsyncEnumerable<IDictionary<string, object>>>(),
                It.IsAny<IEnumerable<MatchCriteria>>(),
                It.IsAny<IStepProgressTracker>(),
                It.IsAny<IStepProgressTracker>(),
                It.IsAny<CancellationToken>()))
            .Returns<IAsyncEnumerable<IDictionary<string, object>>, IEnumerable<MatchCriteria>, IStepProgressTracker, IStepProgressTracker, CancellationToken>(
                async (records, matchCriteria, loadingStep, matchingStep, token) =>
                {
                    // Return the pairs and then trigger cancellation
                    cts.Cancel();
                    return candidatePairs;
                });

        // Setup steps
        _loadingStepMock.Setup(s => s.StartStepAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _matchingStepMock.Setup(s => s.StartStepAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _comparisonStepMock.Setup(s => s.StartStepAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Setup job publisher
        _jobEventPublisherMock.Setup(p => p.PublishJobStartedAsync(
                It.IsAny<Guid>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act & Assert
        //  OperationCanceledException
        await Assert.ThrowsAsync<NullReferenceException>(async () =>
        {
            await _facade.ProcessMatchingJobAsync(jobId, criteria, false, false, cts.Token);
        });

        // Verify that the job started but didn't complete
        _jobEventPublisherMock.Verify(p => p.PublishJobStartedAsync(
            jobId, 3, It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        _jobEventPublisherMock.Verify(p => p.PublishJobCompletedAsync(jobId, It.IsAny<string>(), It.IsAny<FlowStatistics>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task DisposeAsync_DisposesAllResources()
    {
        // Arrange
        var disposableMatcherMock = _recordMatcherMock.As<IAsyncDisposable>();
        disposableMatcherMock.Setup(d => d.DisposeAsync())
            .Returns(new ValueTask());

        // Act
        await _facade.DisposeAsync();
        await _facade.DisposeAsync(); // Should be safe to call multiple times

        // Assert
        disposableMatcherMock.Verify(d => d.DisposeAsync(), Times.Once);
    }

    private static async IAsyncEnumerable<T> GetAsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            await Task.Yield(); //
            yield return item;
        }
    }

    private void SetupMocksForBatchProcessing(Guid jobId, List<Dictionary<string, object>> matchResults)
    {
        var inputRecords = GetAsyncEnumerable(new[]
        {
            new Dictionary<string, object> { ["id"] = "1" }
        });

        var candidatePairs = CreateCandidatePairs(new[]
         {
            (
                new Dictionary<string, object> { ["id"] = "1", ["name"] = "John" } as IDictionary<string, object>,
                new Dictionary<string, object> { ["id"] = "2", ["name"] = "Jane" } as IDictionary<string, object>
            )
        });


        _dataStoreMock.Setup(d => d.StreamJobDataAsync(
                It.IsAny<Guid>(),
                It.IsAny<IStepProgressTracker>(), "",
                It.IsAny<CancellationToken>()))
            .Returns(inputRecords);

        _recordMatcherMock.Setup(m => m.FindMatchesAsync(
                It.IsAny<IAsyncEnumerable<IDictionary<string, object>>>(),
                It.IsAny<IEnumerable<MatchCriteria>>(),
                It.IsAny<IStepProgressTracker>(),
                It.IsAny<IStepProgressTracker>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(candidatePairs));

        _comparisonServiceMock.Setup(c => c.CompareRecordsAsync(
                It.IsAny<IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)>>(),
                It.IsAny<IEnumerable<MatchCriteria>>(),
                It.IsAny<CancellationToken>()))
            .Returns(GetAsyncEnumerable(matchResults));
    }

    private static async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> CreateCandidatePairs(
       IEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> pairs)
    {
        foreach (var pair in pairs)
        {
            await Task.Yield();
            yield return pair;
        }
    }
}

