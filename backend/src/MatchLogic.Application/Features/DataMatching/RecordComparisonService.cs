using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;

namespace MatchLogic.Application.Features.DataMatching;
public class RecordComparisonService: IRecordComparisonService
{
    private readonly ILogger _logger;
    private readonly ITelemetry _telemetry;
    private readonly IComparatorBuilder _comparatorBuilder;
    private readonly int _batchSize;
    private readonly int _maxDegreeOfParallelism;
    private bool _disposed;
    private readonly ObjectPool<Dictionary<string, object>> _dictionaryPool;

    public const string FinalScoreField = "FinalScore";
    public const string WeightedScoreField = "WeightedScore";
    public const string Record1Field = "Record1";
    public const string Record2Field = "Record2";
    public const string PairIdField = "PairId";
    private const string ScoreSuffix = "_Score";
    private const string MetaDataField = "_metadata";
    private const string BlockingKeyField = "BlockingKey";
    private const string WeightSuffix = "_Weight";
    private const double MinScoreThreshold = double.Epsilon; // Use epsilon for floating point comparison
    private long _pairIdCounter;

    
    public RecordComparisonService(
        ILogger<RecordComparisonService> logger,
        ITelemetry telemetry,
        IComparatorBuilder comparatorBuilder,
        IOptions<RecordLinkageOptions> options)
    {
        _logger = logger;
        _telemetry = telemetry;
        _comparatorBuilder = comparatorBuilder;
        _batchSize = options.Value.BatchSize;
        _maxDegreeOfParallelism = options.Value.MaxDegreeOfParallelism;
        _dictionaryPool = ObjectPool.Create(new DictionaryPoolPolicy());
    }

    private class DictionaryPoolPolicy : IPooledObjectPolicy<Dictionary<string, object>>
    {
        public Dictionary<string, object> Create() => new(32); // Pre-allocate with typical size

        public bool Return(Dictionary<string, object> obj)
        {
            obj.Clear();
            return true;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetNextPairId() => Interlocked.Increment(ref _pairIdCounter);

    public async IAsyncEnumerable<IDictionary<string, object>> CompareRecordsAsync(
        IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> candidatePairs,
        IEnumerable<MatchCriteria> criteria,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var operation = _telemetry.MeasureOperation("compare_records");

        var criteriaList = criteria.ToList();//.Where(x => x.MatchingType != MatchingType.Exact).ToList();
        var comparators = CreateComparators(criteriaList);

        // Process candidate pairs in parallel batches
        await foreach (var result in ProcessCandidatePairsAsync(
            candidatePairs,
            comparators,
            criteriaList,
            cancellationToken))
        {
            yield return result;
        }
    }

    private Dictionary<string, IComparator> CreateComparators(IList<MatchCriteria> criteria)
    {
        return criteria.ToDictionary(
            c => c.FieldName,
            c => _comparatorBuilder.WithArgs(c.Arguments).Build(),
            StringComparer.OrdinalIgnoreCase);
    }

    private static void ValidateWeights(IList<MatchCriteria> criteria)
    {
        if (criteria.Any(c => c.Weight < 0 || c.Weight > 1))
        {
            throw new ArgumentException("All weights must be between 0 and 1");
        }

        var totalWeight = criteria.Sum(c => c.Weight);
        if (Math.Abs(totalWeight - 1) > 0.0001) // Allow small floating point difference
        {
            throw new ArgumentException($"Sum of weights must be 1.0, got {totalWeight}");
        }
    }

    private async IAsyncEnumerable<IDictionary<string, object>> ProcessCandidatePairsAsync(
        IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> candidatePairs,
        Dictionary<string, IComparator> comparators,
        IList<MatchCriteria> criteria,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<IDictionary<string, object>>(
            new BoundedChannelOptions(_maxDegreeOfParallelism * _batchSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });

        // Start processing task
        var processingTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in candidatePairs.ChunkAsync(_batchSize, cancellationToken))
                {
                    await Task.WhenAll(batch.Select(pair =>
                        ProcessPairAsync(pair, comparators, criteria, channel.Writer, cancellationToken)));
                }
            }
            finally
            {
                channel.Writer.Complete();
            }
        }, cancellationToken);

        await foreach (var result in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return result;
        }

        await processingTask;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task ProcessPairAsync(
        (IDictionary<string, object>, IDictionary<string, object>) pair,
        Dictionary<string, IComparator> comparators,
        IList<MatchCriteria> criteria,
        ChannelWriter<IDictionary<string, object>> writer,
        CancellationToken cancellationToken)
    {
        var (record1, record2) = pair;
        var matchResult = _dictionaryPool.Get();
        try
        {
            var totalWeightedScore = criteria.Count == 0 ? 1.0 : 0.0;
            var totalScore = criteria.Count == 0 ? 1.0 : 0.0;
            var totalWeight = 0.0;

            foreach (var criterion in criteria)
            {
                var fieldName = criterion.FieldName;
                if (!record1.TryGetValue(fieldName, out var value1) ||
                    !record2.TryGetValue(fieldName, out var value2))
                {
                    return;
                }

                //Handling the case where there are only exact criteria and no fuzzy criteriam in that case we use first criteria to create blocks
                var score = (criterion.MatchingType == MatchingType.Exact && criterion.DataType != CriteriaDataType.Phonetic) ? 1.0 : comparators[fieldName].Compare(
                    value1?.ToString() ?? string.Empty,
                    value2?.ToString() ?? string.Empty);

                if (score <= MinScoreThreshold)
                {
                    return;
                }

                matchResult[$"{fieldName}{ScoreSuffix}"] = score;
                matchResult[$"{fieldName}{WeightSuffix}"] = criterion.Weight;
                totalScore += score;
                totalWeightedScore += score * criterion.Weight;
                totalWeight += criterion.Weight;
            }

            var pairId = GetNextPairId();

            matchResult[PairIdField] = pairId;
            matchResult[Record1Field] = new Dictionary<string, object>(record1);
            matchResult[Record2Field] = new Dictionary<string, object>(record2);
            matchResult[WeightedScoreField] = totalWeightedScore / totalWeight;
            matchResult[FinalScoreField] = totalScore / criteria.Count;
            if (record1.ContainsKey(MetaDataField) && record1[MetaDataField] != null && (record1[MetaDataField] is Dictionary<string, object>))
            {
                var blockingKey = (record1[MetaDataField] as Dictionary<string, object>)[BlockingKeyField].ToString();
                matchResult[BlockingKeyField] = blockingKey;
            }

            await writer.WriteAsync(matchResult, cancellationToken);
            _telemetry.MatchFound();
            matchResult = null;
        }
        finally
        {
            if (matchResult != null)
            {
                _dictionaryPool.Return(matchResult);
            }
        }
    }

    public void ResetPairIdCounter()
    {
        Interlocked.Exchange(ref _pairIdCounter, 0);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await Task.CompletedTask;
    }
}

public interface IRecordComparisonService
{
    IAsyncEnumerable<IDictionary<string, object>> CompareRecordsAsync(
        IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> candidatePairs,
        IEnumerable<MatchCriteria> criteria,
        [EnumeratorCancellation] CancellationToken cancellationToken = default);
    void ResetPairIdCounter();
}