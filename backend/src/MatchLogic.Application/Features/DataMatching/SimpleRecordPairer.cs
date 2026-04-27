using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class SimpleRecordPairer : IDisposable
{
    private readonly ILogger<SimpleRecordPairer> _logger;
    private readonly int _maxPairsPerRecord;
    private readonly int _batchSize;
    private bool _disposed;

    public SimpleRecordPairer(
        ILogger<SimpleRecordPairer> logger,
        IOptions<RecordLinkageOptions> options)
    {
        _logger = logger;
        _maxPairsPerRecord = options.Value.MaxPairsPerRecord;
        _batchSize = options.Value.BatchSize; 
    }

    public async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> GeneratePairsAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var buffer = new List<IDictionary<string, object>>();
        var processedCount = 0;

        await foreach (var record in records.WithCancellation(cancellationToken))
        {
            buffer.Add(record);

            if (buffer.Count >= _batchSize)
            {
                await foreach (var pair in ProcessBatchAsync(buffer, processedCount, cancellationToken))
                {
                    yield return pair;
                }
                processedCount += buffer.Count;
                buffer.Clear();
            }
        }

        // Process remaining records
        if (buffer.Count > 0)
        {
            await foreach (var pair in ProcessBatchAsync(buffer, processedCount, cancellationToken))
            {
                yield return pair;
            }
        }
    }

    private async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> ProcessBatchAsync(
        List<IDictionary<string, object>> records,
        int startIndex,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var pairs = new ConcurrentBag<(IDictionary<string, object>, IDictionary<string, object>)>();
        var pairCounters = new ConcurrentDictionary<int, int>();

        // Process records in parallel
        var tasks = new List<Task>();
        for (var i = 0; i < records.Count; i++)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            var recordIndex = i;
            tasks.Add(Task.Run(() =>
            {
                var pairsCreated = 0;
                var currentRecord = records[recordIndex];

                // Create pairs with subsequent records
                for (var j = recordIndex + 1; j < records.Count && pairsCreated < _maxPairsPerRecord; j++)
                {
                    pairs.Add((currentRecord, records[j]));
                    pairsCreated++;
                }

                pairCounters.TryAdd(startIndex + recordIndex, pairsCreated);
            }, cancellationToken));
        }

        await Task.WhenAll(tasks);

        foreach (var pair in pairs)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            yield return pair;
        }

        _logger.LogInformation("Processed batch of {Count} records, created {PairCount} pairs",
            records.Count, pairs.Count);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
    }
}
