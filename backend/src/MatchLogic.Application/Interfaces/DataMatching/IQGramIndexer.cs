using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataMatching;
public interface IQGramIndexer
{
    Task<(Dictionary<string, ConcurrentDictionary<uint, HashSet<int>>> InvertedIndex, List<IndexEntry> Entries)> CreateIndexAsync(IAsyncEnumerable<IDictionary<string, object>> records,IStepProgressTracker progressTracker, CancellationToken cancellationToken = default);
    IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> GenerateCandidatePairsAsync(Dictionary<string, ConcurrentDictionary<uint, HashSet<int>>> invertedIndex, List<IndexEntry> entries,IStepProgressTracker progressTracker, [EnumeratorCancellation] CancellationToken cancellationToken = default);
    bool AreRecordsMatching(IndexEntry entry1, IndexEntry entry2);
    void SetCriteria(Dictionary<string, double> criteria);
}
