using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Common;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using MatchLogic.Application.Features.DataMatching.FellegiSunter;

namespace MatchLogic.Application.Features.DataMatching;

public class RecordMatchingFacadeWithGrouping : IAsyncDisposable, IRecordMatchingFacade
{
    private readonly IDataStore _dataStore;
    private readonly IRecordMatcher _recordMatcher;
    private readonly IRecordComparisonService _comparisonService;
    private readonly MatchGroupingServiceFactory _groupingServiceFactory;
    private readonly ILogger<RecordMatchingFacadeWithGrouping> _logger;
    private readonly ITelemetry _telemetry;
    private readonly RecordLinkageOptions _options;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly SemaphoreSlim _writeSemaphore;
    private bool _disposed;
    private readonly IRecordHasher _recordHasher;
    private readonly ProbabilisticRecordLinkage _probabilisticRecordLinkage;

    public RecordMatchingFacadeWithGrouping(
        IDataStore dataStore,
        IRecordMatcher recordMatcher,
        IRecordComparisonService comparisonService,
        MatchGroupingServiceFactory groupingServiceFactory,
        ILogger<RecordMatchingFacadeWithGrouping> logger,
        ITelemetry telemetry,
        IJobEventPublisher jobEventPublisher,
        IOptions<RecordLinkageOptions> options,
        IRecordHasher recordHasher,
        ProbabilisticRecordLinkage probabilisticRecordLinkage)
    {
        _dataStore = dataStore;
        _recordMatcher = recordMatcher;
        _comparisonService = comparisonService;
        _groupingServiceFactory = groupingServiceFactory;
        _logger = logger;
        _telemetry = telemetry;
        _options = options.Value;
        _jobEventPublisher = jobEventPublisher;
        _writeSemaphore = new SemaphoreSlim(1, 1);
        _recordHasher = recordHasher;
        _probabilisticRecordLinkage = probabilisticRecordLinkage;
    }

    public async Task ProcessMatchingJobAsync(
        Guid sourceJobId,
        IEnumerable<MatchCriteria> matchCriteria,
        bool mergeOverlappingGroups = false,
        bool useProbabilisticMatching = false,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var operation = _telemetry.MeasureOperation("process_matching_job");
        var pairsCollectionName = $"{GuidCollectionNameConverter.ToValidCollectionName(sourceJobId)}_pairs";
        var groupsCollectionName = $"{GuidCollectionNameConverter.ToValidCollectionName(sourceJobId)}_groups";

        try
        {
            _logger.LogInformation("Starting matching process for job {JobId}", sourceJobId);
            await _jobEventPublisher.PublishJobStartedAsync(sourceJobId, 4, $"Starting matching process for job {sourceJobId}");

            var loadingStep = _jobEventPublisher.CreateStepTracker(sourceJobId, "Data Loading", 1, 4);
            var matchingStep = _jobEventPublisher.CreateStepTracker(sourceJobId, "Finding Matches", 2, 4);
            var comparisonStep = _jobEventPublisher.CreateStepTracker(sourceJobId, "Comparing Records", 3, 4);
            var groupingStep = _jobEventPublisher.CreateStepTracker(sourceJobId, "Creating Groups", 4, 4);

            await matchingStep.StartStepAsync(0, cancellationToken);
            await comparisonStep.StartStepAsync(0, cancellationToken);
            await groupingStep.StartStepAsync(0, cancellationToken);

            var records = _dataStore.StreamJobDataAsync(sourceJobId, loadingStep, cancellationToken: cancellationToken);
            

            //One channel for datastore and second channel for grouping
            var broadcastChannel = new BroadcastChannel<IDictionary<string, object>>(_options.BufferSize, 2); 
            var groupChannel = Channel.CreateBounded<MatchGroup>(_options.BufferSize);

            var broadcastTask = broadcastChannel.StartBroadcastAsync(cancellationToken);
            var writerTask = ProcessMatchWriterAsync(broadcastChannel.Readers[0], pairsCollectionName, comparisonStep, cancellationToken);
            var groupingTask = ProcessGroupingAsync(broadcastChannel.Readers[1], groupChannel.Writer, mergeOverlappingGroups, cancellationToken);
            var groupWriterTask = ProcessGroupWriterAsync(groupChannel.Reader, groupsCollectionName, groupingStep, cancellationToken);

            if(useProbabilisticMatching)
            {
                _logger.LogInformation("Using probabilistic matching for job {JobId}", sourceJobId);
                _probabilisticRecordLinkage.Initialize(sourceJobId, matchCriteria.ToList());
                await _probabilisticRecordLinkage.Train();
                await foreach (var matchResult in _probabilisticRecordLinkage.FindMatchesAsync(records, cancellationToken))
                {
                    await broadcastChannel.Writer.WriteAsync(MatchResultConverter.ToNestedDictionary(matchResult), cancellationToken);
                }
            }
            else
            {
                _logger.LogInformation("Using deterministic matching for job {JobId}", sourceJobId);
                var candidatePairs = await _recordMatcher.FindMatchesAsync(records, matchCriteria, loadingStep, matchingStep, cancellationToken);
                await foreach (var matchResult in _comparisonService.CompareRecordsAsync(candidatePairs, matchCriteria, cancellationToken))
                {
                    await broadcastChannel.Writer.WriteAsync(matchResult, cancellationToken);
                }
            }
            

            broadcastChannel.Writer.Complete();
            await Task.WhenAll(broadcastTask, writerTask, groupingTask);

            groupChannel.Writer.Complete();
            await groupWriterTask;

            await matchingStep.CompleteStepAsync("Matching data completed");
            await comparisonStep.CompleteStepAsync("Comparison completed");
            await groupingStep.CompleteStepAsync("Grouping completed");
            await _jobEventPublisher.PublishJobCompletedAsync(sourceJobId);

            _logger.LogInformation("Completed matching process for job {JobId}", sourceJobId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing matching job {JobId}", sourceJobId);
            throw;
        }
    }

    private async Task ProcessGroupingAsync(
        ChannelReader<IDictionary<string, object>> matchReader,
        ChannelWriter<MatchGroup> groupWriter,
        bool mergeOverlappingGroups,
        CancellationToken cancellationToken)
    {
        var groupingService = _groupingServiceFactory.CreateMatchGroupingService(mergeOverlappingGroups);
        await foreach (var group in groupingService.CreateMatchGroupsAsync(
            matchReader.ReadAllAsync(cancellationToken),
            true,
            mergeOverlappingGroups,
            cancellationToken))
        {
            group.GroupHash = _recordHasher.ComputeGroupHash(group.Records);
            await groupWriter.WriteAsync(group, cancellationToken);
        }
    }

    private async Task ProcessGroupWriterAsync(
        ChannelReader<MatchGroup> groupReader,
        string collectionName,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        var batchBuffer = new List<IDictionary<string, object>>(_options.BatchSize);
        var totalGroups = 0;

        await foreach (var group in groupReader.ReadAllAsync(cancellationToken))
        {
            var groupDict = new Dictionary<string, object>
            {
                ["GroupId"] = group.GroupId,
                ["GroupHash"] = group.GroupHash,
                ["Records"] = group.Records
            };

            batchBuffer.Add(groupDict);
            totalGroups++;

            if (batchBuffer.Count >= _options.BatchSize)
            {
                await progressTracker.UpdateProgressAsync(_options.BatchSize, $"Processed group {totalGroups}");
                await FlushGroupsBatchAsync(batchBuffer, collectionName);
            }
        }

        if (batchBuffer.Count > 0)
        {
            await progressTracker.UpdateProgressAsync(batchBuffer.Count, $"Processed group {totalGroups}");
            await FlushGroupsBatchAsync(batchBuffer, collectionName);
        }
    }

    private async Task FlushGroupsBatchAsync(
        List<IDictionary<string, object>> batch,
        string collectionName)
    {
        try
        {
            await _writeSemaphore.WaitAsync();
            try
            {
                await _dataStore.InsertBatchAsync(collectionName, batch);
                batch.Clear();
            }
            finally
            {
                _writeSemaphore.Release();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing groups batch");
            throw;
        }
    }

    private async Task ProcessMatchWriterAsync(
        ChannelReader<IDictionary<string, object>> matchReader,
        string collectionName,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        var batchBuffer = new List<IDictionary<string, object>>(_options.BatchSize);
        var totalMatches = 0;

        await foreach (var matchResult in matchReader.ReadAllAsync(cancellationToken))
        {
            batchBuffer.Add(matchResult);
            totalMatches++;

            if (batchBuffer.Count >= _options.BatchSize)
            {
                await progressTracker.UpdateProgressAsync(_options.BatchSize, "Compared pair " + totalMatches);
                await FlushMatchingPairsBatchAsync(batchBuffer, collectionName);
                _logger.LogInformation("Wrote batch of {Count} matches. Total matches: {Total}",
                    _options.BatchSize, totalMatches);
            }
        }

        // Flush remaining matches
        if (batchBuffer.Count > 0)
        {
            await progressTracker.UpdateProgressAsync(_options.BatchSize, "Compared pair " + totalMatches);
            await FlushMatchingPairsBatchAsync(batchBuffer, collectionName);
            _logger.LogInformation("Wrote final batch of {Count} matches. Total matches: {Total}",
                batchBuffer.Count, totalMatches);
        }
    }

    private async Task FlushMatchingPairsBatchAsync(
        List<IDictionary<string, object>> batch,
        string collectionName)
    {
        try
        {
            await _writeSemaphore.WaitAsync();
            try
            {
                await _dataStore.InsertBatchAsync(collectionName, batch);
                batch.Clear();
            }
            finally
            {
                _writeSemaphore.Release();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing matching pairs batch");
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        _writeSemaphore.Dispose();

        if (_recordMatcher is IAsyncDisposable disposableMatcher)
        {
            await disposableMatcher.DisposeAsync();
        }

        if (_comparisonService is IAsyncDisposable disposableComparison)
        {
            await disposableComparison.DisposeAsync();
        }
    }
}
