using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class RecordMatcher : IRecordMatcher
{
    private readonly IBlockingStrategy _blockingStrategy;
    private readonly IQGramIndexer _qGramIndexer;
    private readonly ILogger _logger;
    private bool _disposed;

    public RecordMatcher(
        IBlockingStrategy blockingStrategy,
        IQGramIndexer qGramIndexer,
        ILogger logger)
    {
        _blockingStrategy = blockingStrategy;
        _qGramIndexer = qGramIndexer;
        _logger = logger;
    }

    public async Task<IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)>> FindMatchesAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IEnumerable<MatchCriteria> criteria,
        IStepProgressTracker progressTracker,
        IStepProgressTracker stepProgressTracker1,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var exactCriteria = criteria.Where(c => c.MatchingType == MatchingType.Exact && c.DataType != CriteriaDataType.Phonetic).ToList();
        var qGramCriteria = criteria.Where(c => c.MatchingType == MatchingType.Fuzzy && c.DataType != CriteriaDataType.Phonetic)
            .ToDictionary(c => c.FieldName, c => Convert.ToDouble(c.Arguments[ArgsValue.FastLevel]));

        if (!qGramCriteria.Any())
        {
            if (exactCriteria.Any())
            {
                qGramCriteria.Add(exactCriteria.First().FieldName, 1);
            }
        }

        this._qGramIndexer.SetCriteria(qGramCriteria);

        if (exactCriteria.Any())
        {
            var blockedRecords = await _blockingStrategy.BlockRecordsAsync(
                records,
                exactCriteria.Select(c => c.FieldName),
                cancellationToken);

            return ProcessBlockedRecordsAsync(blockedRecords, qGramCriteria,progressTracker, cancellationToken);
        }

        var (invertedIndex, entries) = await _qGramIndexer.CreateIndexAsync(records,progressTracker, cancellationToken);
        return _qGramIndexer.GenerateCandidatePairsAsync(invertedIndex, entries,progressTracker, cancellationToken);
    }

    private async IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> ProcessBlockedRecordsAsync(
        IAsyncEnumerable<IGrouping<string, IDictionary<string, object>>> blockedRecords,
        Dictionary<string, double> qGramCriteria,
        IStepProgressTracker progressTracker,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var block in blockedRecords.WithCancellation(cancellationToken))
        {
            if (block.Count() > 1)
            {
                var blockList = block.ToList();
                var (invertedIndex, entries) = await _qGramIndexer.CreateIndexAsync(
                    blockList.ToAsyncEnumerable(), progressTracker,
                    cancellationToken);

                await foreach (var match in _qGramIndexer.GenerateCandidatePairsAsync(
                    invertedIndex, entries, progressTracker, cancellationToken))
                {
                    yield return match;
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await Task.CompletedTask;
    }
}