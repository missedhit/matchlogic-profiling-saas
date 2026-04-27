using MatchLogic.Application.Interfaces.DataMatching;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Common;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;

public class ParallelBlockingStrategy : IBlockingStrategy
{
    private readonly ILogger _logger;
    private readonly RecordLinkageOptions _options;
    private readonly ITelemetry _telemetry;
    private bool _disposed;
    private const string MetadataField = "_metadata";
    private const string BlockingKeyField = "BlockingKey";

    public ParallelBlockingStrategy(
        ILogger<ParallelBlockingStrategy> logger,
        IOptions<RecordLinkageOptions> options,
        ITelemetry telemetry)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
    }

    // Add AtomicCounter class
    private class AtomicCounter
    {
        private int _value;
        public int Value => _value;
        public int Increment() => Interlocked.Increment(ref _value);
        public int Get() => _value;
    }

    public async Task<IAsyncEnumerable<IGrouping<string, IDictionary<string, object>>>> BlockRecordsAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IEnumerable<string> blockingFields,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(blockingFields);

        var fields = blockingFields.ToList();
        if (!fields.Any())
        {
            throw new ArgumentException("At least one blocking field must be specified", nameof(blockingFields));
        }

        using var operation = _telemetry.MeasureOperation("blocking_records");

        try
        {
            var blockingGroups = new ConcurrentDictionary<string, BlockData>();
            var processedCounter = new AtomicCounter();

            // Create processing pipeline
            var pipeline = CreateProcessingPipeline(blockingGroups, fields, processedCounter, cancellationToken);

            // Process records through pipeline
            await foreach (var record in records.WithCancellation(cancellationToken))
            {
                if (!await pipeline.InputBlock.SendAsync(record, cancellationToken))
                {
                    _logger.LogError("Failed to send record to processing pipeline");
                    throw new InvalidOperationException("Pipeline processing failed");
                }
            }

            // Complete the pipeline and wait for all processing
            pipeline.InputBlock.Complete();
            await pipeline.Completion;

            _logger.LogInformation(
                "Blocking complete. Created {BlockCount} blocks with {RecordCount} total records",
                blockingGroups.Count, processedCounter.Get());

            return blockingGroups
                .ToAsyncEnumerable()
                .Select(kvp => new BlockingGroup(
                    kvp.Key,
                    kvp.Value.Records.ToList()) as IGrouping<string, IDictionary<string, object>>);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error during parallel blocking");
            throw;
        }
    }

    private Pipeline CreateProcessingPipeline(
        ConcurrentDictionary<string, BlockData> blockingGroups,
        IReadOnlyList<string> blockingFields,
        AtomicCounter processedCounter,
        CancellationToken cancellationToken)
    {
        var batchBlock = new BatchBlock<IDictionary<string, object>>(
            _options.BatchSize,
            new GroupingDataflowBlockOptions
            {
                BoundedCapacity = _options.BatchSize * 2,
                CancellationToken = cancellationToken
            });

        var processBlock = new ActionBlock<IDictionary<string, object>[]>(
            async batch =>
            {
                foreach (var record in batch)
                {
                    await ProcessRecordAsync(record, blockingGroups, blockingFields, processedCounter);
                }
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism,
                BoundedCapacity = _options.BufferSize,
                CancellationToken = cancellationToken
            });

        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
        batchBlock.LinkTo(processBlock, linkOptions);

        return new Pipeline(batchBlock, processBlock.Completion);
    }

    private async Task ProcessRecordAsync(
        IDictionary<string, object> record,
        ConcurrentDictionary<string, BlockData> blockingGroups,
        IReadOnlyList<string> blockingFields,
        AtomicCounter processedCounter)
    {
        var blockingKey = GenerateBlockingKey(record, blockingFields);
        var blockData = blockingGroups.GetOrAdd(blockingKey, _ => new BlockData());

        await blockData.Lock.WaitAsync();
        try
        {
            if (record.ContainsKey(MetadataField) && record[MetadataField] != null && (record[MetadataField] is Dictionary<string, object>))
            {
                (record[MetadataField] as Dictionary<string, object>)[BlockingKeyField] = blockingKey;
            }
            blockData.Records.Add(record);
            var count = processedCounter.Increment();
            _telemetry.RecordProcessed();

            if (count % 10000 == 0)
            {
                _logger.LogInformation(
                    "Processed {Count} records for blocking, current block count: {BlockCount}",
                    count, blockingGroups.Count);
            }
        }
        finally
        {
            blockData.Lock.Release();
        }
    }

    private string GenerateBlockingKey(
    IDictionary<string, object> record,
    IEnumerable<string> blockingFields)
    {
        var keyBuilder = new StringBuilder();
        foreach (var field in blockingFields)
        {
            if (record.TryGetValue(field, out var value))
            {
                keyBuilder.Append(value?.ToString() ?? string.Empty).Append('|');
            }
            else
            {
                keyBuilder.Append('|');
            }
        }

        // Remove trailing separator
        if (keyBuilder.Length > 0)
        {
            keyBuilder.Length--;
        }

        return keyBuilder.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await Task.CompletedTask;
    }

    private class BlockData
    {
        public HashSet<IDictionary<string, object>> Records { get; } = new();
        public SemaphoreSlim Lock { get; } = new(1, 1);
    }

    private class BlockingGroup : IGrouping<string, IDictionary<string, object>>
    {
        public string Key { get; }
        private readonly IReadOnlyCollection<IDictionary<string, object>> _records;

        public BlockingGroup(string key, IReadOnlyCollection<IDictionary<string, object>> records)
        {
            Key = key;
            _records = records;
        }

        public IEnumerator<IDictionary<string, object>> GetEnumerator() => _records.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private record Pipeline(
        ITargetBlock<IDictionary<string, object>> InputBlock,
        Task Completion);
}
