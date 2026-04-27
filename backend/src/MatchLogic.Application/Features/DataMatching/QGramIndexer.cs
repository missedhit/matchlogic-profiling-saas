using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;

public class QGramIndexer : IQGramIndexer
{
    private readonly int _q;
    private Dictionary<string, double> _fieldsToIndex;
    private readonly ILogger _logger;

    public QGramIndexer(int q, Dictionary<string, double> fieldsToIndex, ILogger logger)
    {
        _q = q;
        _fieldsToIndex = fieldsToIndex;
        _logger = logger;
    }

    public QGramIndexer(int q, ILogger logger)
    {
        _q = q;
        _logger = logger;
    }

    private IEnumerable<uint> GenerateQGramHashes(string input)
    {
        if (string.IsNullOrEmpty(input) || input.Length < _q)
            yield return HashQGram(input);
        else
            for (int i = 0; i <= input.Length - _q; i++)
                yield return HashQGram(input.Substring(i, _q));
    }

    public uint HashQGram(string qgram)
    {
        uint hash = 0;
        foreach (char c in qgram)
        {
            hash = 31 * hash + c;
        }
        return hash;
    }

    public async Task<(Dictionary<string, ConcurrentDictionary<uint, HashSet<int>>> InvertedIndex, List<IndexEntry> Entries)> CreateIndexAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken = default)
    {
        var invertedIndex = _fieldsToIndex.ToDictionary(
            f => f.Key,
            f => new ConcurrentDictionary<uint, HashSet<int>>()
        );
        var entries = new List<IndexEntry>();

        int recordId = 0;
        await foreach (var record in records.WithCancellation(cancellationToken))
        {
            var entry = new IndexEntry { Record = record };
            foreach (var field in _fieldsToIndex.Keys)
            {
                if (record.TryGetValue(field, out object value) && value is string stringValue)
                {
                    var hashes = new HashSet<uint>(GenerateQGramHashes(stringValue));
                    entry.FieldHashes[field] = hashes;

                    foreach (var hash in hashes)
                    {
                        invertedIndex[field].AddOrUpdate(hash,
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
                }
            }
            entries.Add(entry);
            recordId++;

            if (recordId % 10000 == 0)
            {
                _logger.LogInformation("Indexed {RecordCount} records", recordId);
            }
        }

        return (invertedIndex, entries);
    }

    public async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> GenerateCandidatePairsAsync(
        Dictionary<string, ConcurrentDictionary<uint, HashSet<int>>> invertedIndex,
        List<IndexEntry> entries,
        IStepProgressTracker progressTracker,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < entries.Count; i++)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            var candidatesForRecord = new HashSet<int>();

            foreach (var field in _fieldsToIndex.Keys)
            {
                if (entries[i].FieldHashes.TryGetValue(field, out var hashes))
                {
                    foreach (var hash in hashes)
                    {
                        if (invertedIndex[field].TryGetValue(hash, out var matchingRecords))
                        {
                            candidatesForRecord.UnionWith(matchingRecords);
                        }
                    }
                }
            }

            foreach (var j in candidatesForRecord)
            {
                if (j > i && AreRecordsMatching(entries[i], entries[j]))
                {
                    yield return (entries[i].Record, entries[j].Record);
                }
            }

            if ((i + 1) % 1000 == 0)
            {
                _logger.LogInformation("Processed {RecordCount} records for candidate pairs", i + 1);
            }
        }
    }

    public bool AreRecordsMatching(IndexEntry entry1, IndexEntry entry2)
    {
        foreach (var field in _fieldsToIndex)
        {
            if (entry1.FieldHashes.TryGetValue(field.Key, out var hashes1) &&
                entry2.FieldHashes.TryGetValue(field.Key, out var hashes2))
            {
                int commonQGrams = hashes1.Intersect(hashes2).Count();
                int totalQGrams = hashes1.Union(hashes2).Count();

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

    public void SetCriteria(Dictionary<string, double> criteria)
    {
        this._fieldsToIndex = criteria;
    }
}