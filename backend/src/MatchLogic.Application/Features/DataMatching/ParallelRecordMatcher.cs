using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.Events;

namespace MatchLogic.Application.Features.DataMatching;
public class ParallelRecordMatcher : IRecordMatcher
{
    private readonly IBlockingStrategy _blockingStrategy;
    private readonly IQGramIndexer _qGramIndexer;
    private readonly SimpleRecordPairer _simpleRecordPairer;
    private readonly ILogger _logger;
    private readonly RecordLinkageOptions _options;
    private readonly ITelemetry _telemetry;
    private readonly SemaphoreSlim _semaphore;
    private readonly Channel<(IDictionary<string, object>, IDictionary<string, object>)> _matchChannel;
    private bool _disposed;

    public ParallelRecordMatcher(
        IBlockingStrategy blockingStrategy,
        IQGramIndexer qGramIndexer,
        SimpleRecordPairer simpleRecordPairer,
        ILogger<ParallelRecordMatcher> logger,
        IOptions<RecordLinkageOptions> options,
        ITelemetry telemetry)
    {
        _blockingStrategy = blockingStrategy;
        _qGramIndexer = qGramIndexer;
        _simpleRecordPairer = simpleRecordPairer;
        _logger = logger;
        _options = options.Value;
        _telemetry = telemetry;

        _semaphore = new SemaphoreSlim(_options.MaxDegreeOfParallelism);
        _matchChannel = Channel.CreateBounded<(IDictionary<string, object>, IDictionary<string, object>)>(
            new BoundedChannelOptions(_options.BufferSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });
    }

    public async Task<IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)>> FindMatchesAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IEnumerable<MatchCriteria> criteria,
        IStepProgressTracker progressTracker,
        IStepProgressTracker progressTracker1,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var operation = _telemetry.MeasureOperation("find_matches");

        try
        {
            var (exactCriteria, qGramCriteria) = SplitCriteria(criteria);
            this._qGramIndexer.SetCriteria(qGramCriteria);

            // Start background processing
            _ = ProcessRecordsAsync(
                records,
                exactCriteria,
                qGramCriteria,
                progressTracker,
                progressTracker1,
                cancellationToken);

            return _matchChannel.Reader.ReadAllAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error initiating match finding");
            throw;
        }
    }

    private (List<MatchCriteria> Exact, Dictionary<string, double> QGram) SplitCriteria(
        IEnumerable<MatchCriteria> criteria)
    {
        var exactCriteria = criteria
            .Where(c => c.MatchingType == MatchingType.Exact && c.DataType != CriteriaDataType.Phonetic)
            .ToList();

        var qGramCriteria = criteria
            .Where(c => c.MatchingType == MatchingType.Fuzzy && c.DataType != CriteriaDataType.Phonetic && c.DataType != CriteriaDataType.Number)
            .ToDictionary(c => c.FieldName, c => Convert.ToDouble(c.Arguments[ArgsValue.FastLevel]));

        //Always add all the exact criteria to q grams so that pairs are created accordingly and scores are generated
        if (exactCriteria.Any())
        {
            exactCriteria.ForEach(c => qGramCriteria.Add(c.FieldName, 1));
        }

        /*if (!qGramCriteria.Any())
        {
            var numericFuzzyCriteria = criteria.Where(c => c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Number)
                .ToList();
            if (numericFuzzyCriteria.Any())
            {
                numericFuzzyCriteria.ForEach(c => qGramCriteria.Add(c.FieldName, 0.1));
            }
        }

        if (!qGramCriteria.Any())
        {
            var phoneticCriteria = criteria.Where(c => c.DataType == CriteriaDataType.Phonetic)
                .ToList();
            if (phoneticCriteria.Any())
            {
                phoneticCriteria.ForEach(c => qGramCriteria.Add(c.FieldName, 0.3));
            }
        }*/
        return (exactCriteria, qGramCriteria);
    }

    private async Task ProcessRecordsAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        List<MatchCriteria> exactCriteria,
        Dictionary<string, double> qGramCriteria,
        IStepProgressTracker progressTracker,
        IStepProgressTracker progressTracker1,
        CancellationToken cancellationToken)
    {
        try
        {
            if (exactCriteria.Any())
            {
                await ProcessWithBlockingAsync(
                    records,
                    exactCriteria,
                    qGramCriteria,
                    progressTracker,
                    progressTracker1,
                    cancellationToken);
            }
            else if (qGramCriteria.Any())
            {
                await ProcessWithoutBlockingAsync(
                    records,
                    qGramCriteria,
                    progressTracker,
                    progressTracker1,
                    cancellationToken);
            }
            else
            {
                await foreach (var pair in _simpleRecordPairer.GeneratePairsAsync(records, cancellationToken))
                {
                    await _matchChannel.Writer.WriteAsync(pair, cancellationToken);
                    _telemetry.MatchFound();
                    _telemetry.RecordProcessed();
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Processing cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing records");
        }
        finally
        {
            _matchChannel.Writer.Complete();
        }
    }

    private async Task ProcessWithBlockingAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        List<MatchCriteria> exactCriteria,
        Dictionary<string, double> qGramCriteria,
        IStepProgressTracker progressTracker,
        IStepProgressTracker progressTracker1,
        CancellationToken cancellationToken)
    {
        using var operation = _telemetry.MeasureOperation("process_with_blocking");

        var blockProcessingTasks = new List<Task>();
        var processedRecords = 0;

        try
        {
            var blockedRecords = await _blockingStrategy.BlockRecordsAsync(
                records,
                exactCriteria.Select(c => c.FieldName),
                cancellationToken);

            await foreach (var block in blockedRecords.WithCancellation(cancellationToken))
            {
                if (block.Count() > 1)
                {
                    await _semaphore.WaitAsync(cancellationToken);

                    blockProcessingTasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessBlockAsync(
                                block,
                                qGramCriteria,
                                progressTracker,
                                progressTracker1,
                                cancellationToken);

                            Interlocked.Add(ref processedRecords, block.Count());
                            _telemetry.RecordProcessed(block.Count());
                        }
                        finally
                        {
                            _semaphore.Release();
                        }
                    }, cancellationToken));
                }
                else
                {
                    Interlocked.Increment(ref processedRecords);
                    _telemetry.RecordProcessed();
                }

                if (processedRecords % 10000 == 0)
                {
                    _logger.LogInformation("Processed {Count} records", processedRecords);
                }
            }

            await Task.WhenAll(blockProcessingTasks);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error in blocked processing");
            throw;
        }
    }

    private async Task ProcessWithoutBlockingAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        Dictionary<string, double> qGramCriteria,
        IStepProgressTracker progressTracker,
        IStepProgressTracker progressTracker1,
        CancellationToken cancellationToken)
    {
        using var operation = _telemetry.MeasureOperation("process_without_blocking");

        try
        {
            var (invertedIndex, entries) = await _qGramIndexer.CreateIndexAsync(
                records,
                progressTracker,
                cancellationToken);

            await foreach (var matchedPair in _qGramIndexer.GenerateCandidatePairsAsync(
                invertedIndex, entries,progressTracker1, cancellationToken))
            {
                await _matchChannel.Writer.WriteAsync(matchedPair, cancellationToken);
                _telemetry.MatchFound();
                _telemetry.RecordProcessed();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error in non-blocked processing");
            throw;
        }
    }

    private async Task ProcessBlockAsync(
        IGrouping<string, IDictionary<string, object>> block,
        Dictionary<string, double> qGramCriteria, IStepProgressTracker progressTracker,
        IStepProgressTracker progressTracker1,
        CancellationToken cancellationToken)
    {
        var blockList = block.ToList();
        var (invertedIndex, entries) = await _qGramIndexer.CreateIndexAsync(
            blockList.ToAsyncEnumerable(), progressTracker,
            cancellationToken);

        await foreach (var matchedPair in _qGramIndexer.GenerateCandidatePairsAsync(
            invertedIndex, entries,progressTracker1, cancellationToken))
        {
            await _matchChannel.Writer.WriteAsync(matchedPair, cancellationToken);
            _telemetry.MatchFound();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        _semaphore.Dispose();
        await Task.CompletedTask;
    }
}

