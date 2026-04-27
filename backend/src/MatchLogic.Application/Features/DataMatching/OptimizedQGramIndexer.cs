using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class OptimizedQGramIndexer : IDisposable, IQGramIndexer
{
    private readonly int _q;
    private Dictionary<string, double> _fieldsToIndex;
    private readonly ILogger _logger;
    private readonly ArrayPool<char> _charPool;
    private readonly ArrayPool<uint> _hashPool;
    private const int DefaultBatchSize = 1000;
    private const int DefaultBufferSize = 10000;
    private bool _disposed;

    public OptimizedQGramIndexer(int q, Dictionary<string, double> fieldsToIndex, ILogger<OptimizedQGramIndexer> logger)
    {
        if (q < 1) throw new ArgumentOutOfRangeException(nameof(q), "Q must be greater than 0");
        ArgumentNullException.ThrowIfNull(fieldsToIndex);
        ArgumentNullException.ThrowIfNull(logger);

        _q = q;
        _fieldsToIndex = new Dictionary<string, double>(fieldsToIndex, StringComparer.OrdinalIgnoreCase);
        _logger = logger;
        _charPool = ArrayPool<char>.Shared;
        _hashPool = ArrayPool<uint>.Shared;
    }

    public OptimizedQGramIndexer(int q, ILogger logger)
    {
        if (q < 1) throw new ArgumentOutOfRangeException(nameof(q), "Q must be greater than 0");
        ArgumentNullException.ThrowIfNull(logger);

        _q = q;
        _logger = logger;
        _charPool = ArrayPool<char>.Shared;
        _hashPool = ArrayPool<uint>.Shared;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GenerateQGramHashes(ReadOnlySpan<char> input, uint[] hashBuffer, out int hashCount)
    {
        hashCount = 0;

        if (input.Length == 0)
        {
            hashBuffer[hashCount++] = HashQGram(ReadOnlySpan<char>.Empty);
            return;
        }

        if (input.Length < _q)
        {
            hashBuffer[hashCount++] = HashQGram(input);
            return;
        }

        for (int i = 0; i <= input.Length - _q; i++)
        {
            hashBuffer[hashCount++] = HashQGram(input.Slice(i, _q));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint HashQGram(ReadOnlySpan<char> qgram)
    {
        uint hash = 0;
        for (int i = 0; i < qgram.Length; i++)
        {
            hash = 31 * hash + qgram[i];
        }
        return hash;
    }

    public async Task<(Dictionary<string, ConcurrentDictionary<uint, HashSet<int>>> InvertedIndex, List<IndexEntry> Entries)> CreateIndexAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var invertedIndex = _fieldsToIndex.ToDictionary(
            f => f.Key,
            f => new ConcurrentDictionary<uint, HashSet<int>>(),
            StringComparer.OrdinalIgnoreCase
        );

        var entries = new List<IndexEntry>();
        var maxQGrams = 1024; // Reasonable buffer size for most texts
        var hashBuffer = _hashPool.Rent(maxQGrams);

        try
        {
            int recordId = 0;
            await foreach (var record in records.WithCancellation(cancellationToken))
            {
                var entry = new IndexEntry { Record = record };

                foreach (var field in _fieldsToIndex.Keys)
                {
                    if (record.TryGetValue(field, out object value) && value is string stringValue)
                    {
                        GenerateQGramHashes(stringValue.AsSpan(), hashBuffer, out int hashCount);
                        var hashes = new HashSet<uint>();

                        for (int i = 0; i < hashCount; i++)
                        {
                            var hash = hashBuffer[i];
                            hashes.Add(hash);

                            invertedIndex[field].AddOrUpdate(
                                hash,
                                _ => new HashSet<int> { recordId },
                                (_, set) =>
                                {
                                    lock (set)
                                    {
                                        set.Add(recordId);
                                        return set;
                                    }
                                });
                        }

                        entry.FieldHashes[field] = hashes;
                    }
                }

                entries.Add(entry);
                recordId++;
                //OVR check we can write records pulled
                if (recordId % 10000 == 0)
                {                    
                    _logger.LogInformation("Indexed {RecordCount} records", recordId);
                }
            }
            await progressTracker.UpdateProgressAsync(recordId);

            return (invertedIndex, entries);
        }
        finally
        {
            _hashPool.Return(hashBuffer);
        }
    }

    public async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> GenerateCandidatePairsAsync(
        Dictionary<string, ConcurrentDictionary<uint, HashSet<int>>> invertedIndex,
        List<IndexEntry> entries,
        IStepProgressTracker progressTracker,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var batchSize = DefaultBatchSize;
        var batches = (entries.Count + batchSize - 1) / batchSize;
        var candidatePairs = new ConcurrentDictionary<(int, int), byte>();

        for (int batchIndex = 0; batchIndex < batches; batchIndex++)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            var start = batchIndex * batchSize;
            var end = Math.Min(start + batchSize, entries.Count);

            for (int i = start; i < end; i++)
            {
                var candidatesForRecord = await FindCandidatesAsync(i, entries[i], invertedIndex, cancellationToken);

                foreach (var j in candidatesForRecord)
                {
                    if (j > i && candidatePairs.TryAdd((i, j), 0) && AreRecordsMatching(entries[i], entries[j]))
                    {
                        yield return (entries[i].Record, entries[j].Record);
                    }
                }

                if ((i + 1) % 1000 == 0)
                {
                    //OVR check we can write update here
                    await progressTracker.UpdateProgressAsync(i + 1, "Candidate pairs processed " + candidatePairs.Count);
                    _logger.LogInformation("Processed {RecordCount} records for candidate pairs ", i + 1);
                }
            }
        }
    }

    private async Task<HashSet<int>> FindCandidatesAsync(
        int recordId,
        IndexEntry entry,
        Dictionary<string, ConcurrentDictionary<uint, HashSet<int>>> invertedIndex,
        CancellationToken cancellationToken)
    {
        var candidates = new HashSet<int>();

        foreach (var field in _fieldsToIndex.Keys)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            if (entry.FieldHashes.TryGetValue(field, out var hashes))
            {
                foreach (var hash in hashes)
                {
                    if (invertedIndex[field].TryGetValue(hash, out var matchingRecords))
                    {
                        foreach (var match in matchingRecords)
                        {
                            if (match != recordId)
                            {
                                candidates.Add(match);
                            }
                        }
                    }
                }
            }
        }

        await Task.Yield(); // Allow other tasks to run
        return candidates;
    }

    public bool AreRecordsMatching(IndexEntry entry1, IndexEntry entry2)
    {
        foreach (var field in _fieldsToIndex)
        {
            if (entry1.FieldHashes.TryGetValue(field.Key, out var hashes1) &&
                entry2.FieldHashes.TryGetValue(field.Key, out var hashes2))
            {
                int commonQGrams = hashes1.Intersect(hashes2).Count();
                int totalQGrams = Math.Max(hashes1.Count, hashes2.Count); //hashes1.Union(hashes2).Count();

                double similarity = (double)commonQGrams / totalQGrams;

                if (similarity < field.Value)
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
    }

    public void SetCriteria(Dictionary<string, double> criteria)
    {
        this._fieldsToIndex = criteria;
    }
}
