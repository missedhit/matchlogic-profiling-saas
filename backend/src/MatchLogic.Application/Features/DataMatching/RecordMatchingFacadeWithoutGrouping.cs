using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Persistence;
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
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Extensions;
using System.Text.Json;
using MatchLogic.Application.Interfaces.Events;

namespace MatchLogic.Application.Features.DataMatching;
public class RecordMatchingFacadeWithoutGrouping : IAsyncDisposable, IRecordMatchingFacade
{
    private readonly IDataStore _dataStore;
    private readonly IRecordMatcher _recordMatcher;
    private readonly IRecordComparisonService _comparisonService;
    private readonly ILogger<RecordMatchingFacadeWithoutGrouping> _logger;
    private readonly ITelemetry _telemetry;
    private readonly RecordLinkageOptions _options;
    IJobEventPublisher _jobEventPublisher;
    private readonly SemaphoreSlim _writeSemaphore;
    private bool _disposed;

    public RecordMatchingFacadeWithoutGrouping(
        IDataStore dataStore,
        IRecordMatcher recordMatcher,
        IRecordComparisonService comparisonService,
        ILogger<RecordMatchingFacadeWithoutGrouping> logger,
        ITelemetry telemetry,
        IJobEventPublisher jobEventPublisher,
        IOptions<RecordLinkageOptions> options)
    {
        _dataStore = dataStore;
        _recordMatcher = recordMatcher;
        _comparisonService = comparisonService;
        _logger = logger;
        _telemetry = telemetry;
        _options = options.Value;
        _jobEventPublisher = jobEventPublisher;
        _writeSemaphore = new SemaphoreSlim(1, 1);
    }

        public async Task ProcessMatchingJobAsync(
            Guid sourceJobId,
            IEnumerable<MatchCriteria> matchCriteria,
            bool mergeOverlappingGroups = false,
            bool useProbabilistic = false,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            using var operation = _telemetry.MeasureOperation("process_matching_job");
            var pairsCollectionName = $"{GuidCollectionNameConverter.ToValidCollectionName(sourceJobId)}_pairs";

            try
            {
                _logger.LogInformation("Starting matching process for job {JobId}", sourceJobId);

                await _jobEventPublisher.PublishJobStartedAsync(sourceJobId, 3, $"Starting matching process for job {sourceJobId}");

                var loadingStep = _jobEventPublisher.CreateStepTracker(sourceJobId, "Data Loading", 1, 3);
                var matchingStep = _jobEventPublisher.CreateStepTracker(sourceJobId, "Finding Matches", 2, 3);
                var comparisonStep = _jobEventPublisher.CreateStepTracker(sourceJobId, "Comparing Records", 3, 3);



            
            await matchingStep.StartStepAsync(0, cancellationToken);

            await comparisonStep.StartStepAsync(0, cancellationToken);
            // Get input records stream
            var records = _dataStore.StreamJobDataAsync(sourceJobId, loadingStep, cancellationToken: cancellationToken);

                // Generate candidate pairs with actual records
                var candidatePairs = await _recordMatcher.FindMatchesAsync(
                    records,
                    matchCriteria,
                    loadingStep,
                    matchingStep,
                    cancellationToken);

            
            // candidatePairs?.TrackProgress(matchingStep);

            // Create a channel for matched results
            var matchChannel = Channel.CreateBounded<IDictionary<string, object>>(
                    new BoundedChannelOptions(_options.BufferSize)
                    {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleReader = true,
                        SingleWriter = true
                    });

                // Start the writer task
                var writerTask = ProcessMatchWriterAsync(
                    matchChannel.Reader,
                    pairsCollectionName,
                    comparisonStep,
                    cancellationToken);

                
                // Process comparisons and write results
                await foreach (var matchResult in _comparisonService.CompareRecordsAsync(
                    candidatePairs,
                    matchCriteria,
                    cancellationToken))//.TrackProgress(comparisonStep))
                {
                    await matchChannel.Writer.WriteAsync(matchResult, cancellationToken);
                }

                // Signal completion and wait for writer
                matchChannel.Writer.Complete();
                await writerTask;

            //await loadingStep.CompleteStepAsync("Data loading completed");
                await matchingStep.CompleteStepAsync("Matching data completed");
                await comparisonStep.CompleteStepAsync("Comparison completed");
                await _jobEventPublisher.PublishJobCompletedAsync(sourceJobId);
                
                _logger.LogInformation("Completed matching process for job {JobId}", sourceJobId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing matching job {JobId}", sourceJobId);
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
