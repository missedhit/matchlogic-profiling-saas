using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;

/// <summary>
/// Synchronous record comparison service with qualified pair merging.
/// Merges candidate pairs with same record identities into single scored pairs.
/// </summary>
public class EnhancedRecordComparisonServiceDME : IEnhancedRecordComparisonService
{
    private readonly ILogger<EnhancedRecordComparisonServiceDME> _logger;
    private readonly ITelemetry _telemetry;
    private readonly IComparatorBuilder _comparatorBuilder;
    private readonly ObjectPool<ScoredMatchPair> _scoredPairPool;
    private readonly int _maxDegreeOfParallelism;
    private readonly double _minScoreThreshold;
    private long _pairIdCounter;
    private bool _disposed;

    public EnhancedRecordComparisonServiceDME(
        ILogger<EnhancedRecordComparisonServiceDME> logger,
        ITelemetry telemetry,
        IComparatorBuilder comparatorBuilder,
        IOptions<RecordLinkageOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _comparatorBuilder = comparatorBuilder ?? throw new ArgumentNullException(nameof(comparatorBuilder));

        var optionsValue = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _maxDegreeOfParallelism = optionsValue.MaxDegreeOfParallelism;
        _minScoreThreshold = optionsValue.MinimumMatchScore ?? 0.0;

        _scoredPairPool = ObjectPool.Create(new ScoredPairPoolPolicy());
    }

    /// <summary>
    /// Main comparison method with qualified pair merging.
    /// Consolidates candidates with same record identities into single pairs.
    /// </summary>
    public Task<(List<ScoredMatchPair>, MatchDefinitionCollection)> CompareAndCollectPairsAsync(
        IAsyncEnumerable<CandidatePair> candidates,
        MatchDefinitionCollection matchDefinitions,
        IDataSourceIndexMapper indexMapper,
        CancellationToken cancellationToken = default)
    {
        // Delegate to the parameterised overload, preserving the configured batch-mode parallelism.
        return CompareAndCollectPairsAsync(candidates, matchDefinitions, indexMapper, -1, cancellationToken);
    }

    public async Task<(List<ScoredMatchPair>, MatchDefinitionCollection)> CompareAndCollectPairsAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            int maxDegreeOfParallelism,
            CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var operation = _telemetry.MeasureOperation("synchronous_compare_records");

        // Pre-build comparators for all definitions
        var comparatorsByDefinition = BuildComparatorsForDefinitions(matchDefinitions);

        // Collect all candidates into array for parallel processing
        var candidateList = new List<CandidatePair>();
        await foreach (var candidate in candidates.WithCancellation(cancellationToken))
        {
            candidateList.Add(candidate);
        }

        // Caller-supplied parallelism overrides the configured default (used by Live Search to
        // cap per-request CPU fan-out; batch callers pass -1 and retain their configured default).
        var effectiveParallelism = maxDegreeOfParallelism > 0 ? maxDegreeOfParallelism : _maxDegreeOfParallelism;

        _logger.LogInformation(
            "Processing {Count} candidates with {Parallelism} parallel workers",
            candidateList.Count, effectiveParallelism);

        // Use concurrent dictionary for automatic pair merging
        // Key: normalized pair identity (DataSource1Id, Row1Number, DataSource2Id, Row2Number)
        var mergedPairs = new ConcurrentDictionary<PairIdentity, ScoredMatchPair>();

        // Process candidates with Parallel.ForEachAsync -- truly async end-to-end so record
        // loads no longer block thread-pool threads via sync-over-async. Peak memory is bounded
        // to roughly effectiveParallelism * 2 records (one pair at a time per worker), which
        // scales the same whether we have 10K candidates (live search) or 10M (batch).
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = effectiveParallelism,
            CancellationToken = cancellationToken
        };

        await Parallel.ForEachAsync(
            candidateList,
            parallelOptions,
            async (candidate, ct) =>
            {
                var scoredPair = await ScoreCandidateAsync(
                    candidate,
                    matchDefinitions,
                    comparatorsByDefinition,
                    indexMapper,
                    ct);

                if (scoredPair != null)
                {
                    var identity = PairIdentity.Create(
                        scoredPair.DataSource1Id, scoredPair.Row1Number,
                        scoredPair.DataSource2Id, scoredPair.Row2Number);

                    mergedPairs.AddOrUpdate(
                        identity,
                        scoredPair,
                        (key, existingPair) =>
                        {
                            MergePairs(existingPair, scoredPair);
                            _scoredPairPool.Return(scoredPair);
                            return existingPair;
                        });

                    _telemetry.MatchFound();
                }
            });

        // Assign unique pair IDs to merged pairs
        var resultPairs = mergedPairs.Values.ToList();
        foreach (var pair in resultPairs)
        {
            pair.PairId = Interlocked.Increment(ref _pairIdCounter);
        }

        _logger.LogInformation(
            "Comparison completed: {ResultCount} unique pairs from {CandidateCount} candidates (merged {MergedCount})",
            resultPairs.Count, candidateList.Count, candidateList.Count - resultPairs.Count);

        return (resultPairs, matchDefinitions);
    }
    /// <summary>
    /// Merges scores and definitions from source pair into target pair.
    /// Keeps the highest score for each definition and updates max score.
    /// </summary>
    private void MergePairs(ScoredMatchPair target, ScoredMatchPair source)
    {
        // Merge match definition indices
        foreach (var defIndex in source.MatchDefinitionIndices)
        {
            if (!target.MatchDefinitionIndices.Contains(defIndex))
            {
                target.MatchDefinitionIndices.Add(defIndex);
            }
        }

        // Merge scores by definition - keep highest weighted score per definition
        foreach (var kvp in source.ScoresByDefinition)
        {
            if (!target.ScoresByDefinition.TryGetValue(kvp.Key, out var existingScoreDetail))
            {
                // New definition, add it
                target.ScoresByDefinition[kvp.Key] = kvp.Value;
            }
            else
            {
                // Definition exists, keep the one with higher weighted score
                if (kvp.Value.WeightedScore > existingScoreDetail.WeightedScore)
                {
                    target.ScoresByDefinition[kvp.Key] = kvp.Value;
                }
            }
        }

        // Update max score
        target.MaxScore = Math.Max(target.MaxScore, source.MaxScore);

        // Merge metadata if present
        foreach (var kvp in source.Metadata)
        {
            if (!target.Metadata.ContainsKey(kvp.Key))
            {
                target.Metadata[kvp.Key] = kvp.Value;
            }
        }
    }

    /// <summary>
    /// Legacy interface method - returns enumerable for compatibility
    /// </summary>
    public async IAsyncEnumerable<ScoredMatchPair> CompareAsync(
        IAsyncEnumerable<CandidatePair> candidates,
        MatchDefinitionCollection matchDefinitions,
        IDataSourceIndexMapper indexMapper,
        CancellationToken cancellationToken = default)
    {
        var (pairs, _) = await CompareAndCollectPairsAsync(
            candidates, matchDefinitions, indexMapper, cancellationToken);

        foreach (var pair in pairs)
        {
            yield return pair;
        }
    }

    /// <summary>
    /// Legacy interface - not used in new implementation
    /// </summary>
    public Task<(IAsyncEnumerable<ScoredMatchPair>, MatchGraph)> CompareWithGraphAsync(
        IAsyncEnumerable<CandidatePair> candidates,
        MatchDefinitionCollection matchDefinitions,
        IDataSourceIndexMapper indexMapper,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException(
            "Use CompareAndCollectPairsAsync then build graph separately");
    }

    /// <summary>
    /// Async single-pass scoring. Async end-to-end so record loads don't block thread-pool
    /// threads (the sync-over-async pattern used previously would starve the pool under heavy
    /// concurrency, which was the main cause of queue build-up under load).
    /// </summary>
    private async Task<ScoredMatchPair> ScoreCandidateAsync(
        CandidatePair candidate,
        MatchDefinitionCollection matchDefinitions,
        Dictionary<Guid, Dictionary<Guid, IComparator>> comparatorsByDefinition,
        IDataSourceIndexMapper indexMapper,
        CancellationToken cancellationToken)
    {
        // FAST PATH: Exact-only (EstimatedSimilarity = 1.0 from GenerateExactOnlyCandidatesAsync)
        if (candidate.EstimatedSimilarity >= 0.99)
        {
            bool allExactOnly = true;

            foreach (var definitionId in candidate.MatchDefinitionIds)
            {
                var definition = matchDefinitions.Definitions.FirstOrDefault(d => d.Id == definitionId);
                if (definition == null) continue;

                foreach (var criterion in definition.Criteria)
                {
                    if (criterion.MatchingType != MatchingType.Exact ||
                        criterion.DataType == CriteriaDataType.Phonetic)
                    {
                        allExactOnly = false;
                        break;
                    }
                }
                if (!allExactOnly) break;
            }

            if (allExactOnly)
            {
                return await CreateExactOnlyScoredPairAsync(candidate, matchDefinitions, indexMapper, cancellationToken);
            }
        }

        // SLOW PATH: Fuzzy - load both records concurrently via the underlying async stores
        // (previously sync-over-async, which blocked thread-pool threads inside Parallel.For).
        var record1Task = candidate.GetRecord1Async();
        var record2Task = candidate.GetRecord2Async();
        await Task.WhenAll(record1Task, record2Task);

        var record1 = record1Task.Result;
        var record2 = record2Task.Result;

        if (record1 == null || record2 == null)
            return null;

        var scoredPair = _scoredPairPool.Get();
        scoredPair.DataSource1Id = candidate.DataSource1Id;
        scoredPair.DataSource2Id = candidate.DataSource2Id;
        scoredPair.Row1Number = candidate.Row1Number;
        scoredPair.Row2Number = candidate.Row2Number;
        scoredPair.Record1 = record1;
        scoredPair.Record2 = record2;

        double maxScore = 0.0;
        var passingDefinitions = new List<int>();

        foreach (var definitionId in candidate.MatchDefinitionIds)
        {
            var definition = matchDefinitions.Definitions
                .FirstOrDefault(d => d.Id == definitionId);

            if (definition == null)
                continue;

            var definitionIndex = definition.UIDefinitionIndex;

            if (!comparatorsByDefinition.TryGetValue(definitionId, out var comparators))
                continue;

            var scoreDetail = ScoreAgainstDefinitionSync(
                record1, record2, definition,
                comparators,
                candidate.DataSource1Id, candidate.DataSource2Id,
                indexMapper);

            if (scoreDetail != null && scoreDetail.FinalScore >= _minScoreThreshold)
            {
                passingDefinitions.Add(definitionIndex);
                maxScore = Math.Max(maxScore, scoreDetail.WeightedScore);
                scoredPair.ScoresByDefinition[definitionIndex] = scoreDetail;
            }
        }

        if (passingDefinitions.Count == 0)
        {
            _scoredPairPool.Return(scoredPair);
            return null;
        }

        scoredPair.MatchDefinitionIndices.AddRange(passingDefinitions);
        scoredPair.MaxScore = maxScore;
        scoredPair.DataSource1Index = indexMapper.GetDataSourceIndex(candidate.DataSource1Id);
        scoredPair.DataSource2Index = indexMapper.GetDataSourceIndex(candidate.DataSource2Id);

        return scoredPair;
    }

    /// <summary>
    /// Scores a record pair against a single definition using the appropriate comparators.
    /// 
    /// Evidence from Legacy:
    /// - RowsMatcher.cs CalculatePairScoresForOneMatchDefinition: Calculates weighted scores
    /// - RowsMatcher.cs GetExactMatchScore (lines 522-553): Binary 1.0/0.0 for exact
    /// - JaroWinklerCombined.cs: Fuzzy similarity calculation
    /// </summary>
    private MatchScoreDetail ScoreAgainstDefinitionSync(
IDictionary<string, object> record1,
IDictionary<string, object> record2,
Domain.Entities.MatchDefinition definition,
Dictionary<Guid, IComparator> comparators,
Guid dataSource1Id,
Guid dataSource2Id,
IDataSourceIndexMapper indexMapper)
    {
        if (definition == null || definition.Criteria == null || definition.Criteria.Count == 0)
            return null;

        var scoreDetail = new MatchScoreDetail();
        double totalScore = 0.0;
        double totalWeightedScore = 0.0;
        double totalWeight = 0.0;
        int criteriaCount = 0;

        foreach (var criterion in definition.Criteria)
        {
            var fieldsDS1 = criterion.FieldMappings?
                .Where(fm => fm.DataSourceId == dataSource1Id)
                .Select(fm => fm.FieldName)
                .Distinct()
                .ToList() ?? new List<string>();

            var fieldsDS2 = criterion.FieldMappings?
                .Where(fm => fm.DataSourceId == dataSource2Id)
                .Select(fm => fm.FieldName)
                .Distinct()
                .ToList() ?? new List<string>();

            List<(string f1, string f2)> fieldPairs;

            if (dataSource1Id == dataSource2Id)
            {
                // Dedupe: if fields mapped once, use same set
                var fields = fieldsDS1?.Count > 0 ? fieldsDS1
                            : fieldsDS2?.Count > 0 ? fieldsDS2
                            : new List<string>();
                if (fields?.Count == 0) return null;
                fieldPairs = fields.Select(f => (f, f)).ToList();
            }
            else
            {
                if (fieldsDS1.Count == 0 || fieldsDS2.Count == 0) return null;
                fieldPairs = (from f1 in fieldsDS1 from f2 in fieldsDS2 select (f1, f2)).ToList();
            }

            double criterionScoreAccum = 0;
            int criterionComparisons = 0;

            // Exact text matching
            if (criterion.MatchingType == MatchingType.Exact && criterion.DataType != CriteriaDataType.Phonetic)
            {
                foreach (var (f1, f2) in fieldPairs)
                {
                    if (!record1.TryGetValue(f1, out var v1) || !record2.TryGetValue(f2, out var v2)) return null;

                    var s1 = (v1?.ToString() ?? "").Trim();
                    var s2 = (v2?.ToString() ?? "").Trim();

                    if (!string.Equals(s1, s2, StringComparison.Ordinal))
                        return null;

                    var fk = $"{f1}_{f2}";
                    scoreDetail.FieldScores[fk] = 1.0;
                    scoreDetail.FieldWeights[fk] = criterion.Weight;
                    criterionScoreAccum += 1.0;
                    criterionComparisons++;
                }

                var criterionScore = criterionComparisons > 0 ? (criterionScoreAccum / criterionComparisons) : 0.0;
                totalScore += criterionScore;
                totalWeightedScore += criterionScore * criterion.Weight;
                totalWeight += criterion.Weight;
                criteriaCount++;
                continue;
            }

            // Fuzzy or phonetic/numeric matching
            if (!comparators.TryGetValue(criterion.Id, out var comparator))
                return null;

            foreach (var (f1, f2) in fieldPairs)
            {
                if (!record1.TryGetValue(f1, out var v1) || !record2.TryGetValue(f2, out var v2)) return null;

                var score = comparator.Compare(v1?.ToString() ?? string.Empty, v2?.ToString() ?? string.Empty);
                if (score <= double.Epsilon || score == 0)
                {
                    return null;
                }

                var fk = $"{f1}_{f2}";
                scoreDetail.FieldScores[fk] = score;
                scoreDetail.FieldWeights[fk] = criterion.Weight;
                criterionScoreAccum += score;
                criterionComparisons++;
            }

            if (criterionComparisons == 0) return null;

            var criterionAvg = criterionScoreAccum / criterionComparisons;
            totalScore += criterionAvg;
            totalWeightedScore += criterionAvg * criterion.Weight;
            totalWeight += criterion.Weight;
            criteriaCount++;
        }

        if (criteriaCount == 0 || totalWeight <= 0) return null;

        scoreDetail.WeightedScore = totalWeightedScore / totalWeight;
        scoreDetail.FinalScore = totalScore / criteriaCount;
        return scoreDetail;
    }

    private async Task<ScoredMatchPair> CreateExactOnlyScoredPairAsync(
            CandidatePair candidate,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            CancellationToken cancellationToken)
    {
        // Load records concurrently via the underlying async stores instead of blocking on
        // .GetAwaiter().GetResult() inside a Parallel.For worker.
        var record1Task = candidate.GetRecord1Async();
        var record2Task = candidate.GetRecord2Async();
        await Task.WhenAll(record1Task, record2Task);

        var record1 = record1Task.Result;
        var record2 = record2Task.Result;

        var scoredPair = _scoredPairPool.Get();

        scoredPair.DataSource1Id = candidate.DataSource1Id;
        scoredPair.DataSource2Id = candidate.DataSource2Id;
        scoredPair.Row1Number = candidate.Row1Number;
        scoredPair.Row2Number = candidate.Row2Number;
        scoredPair.Record1 = record1;
        scoredPair.Record2 = record2;
        scoredPair.MaxScore = 1.0;
        scoredPair.DataSource1Index = indexMapper.GetDataSourceIndex(candidate.DataSource1Id);
        scoredPair.DataSource2Index = indexMapper.GetDataSourceIndex(candidate.DataSource2Id);

        bool isDedup = (candidate.DataSource1Id == candidate.DataSource2Id);

        foreach (var definitionId in candidate.MatchDefinitionIds)
        {
            var definition = matchDefinitions.Definitions.FirstOrDefault(d => d.Id == definitionId);
            if (definition == null) continue;

            var definitionIndex = definition.UIDefinitionIndex;

            var scoreDetail = new MatchScoreDetail
            {
                WeightedScore = 1.0,
                FinalScore = 1.0
            };

            foreach (var criterion in definition.Criteria)
            {
                var fieldsDS1 = criterion.FieldMappings?
                    .Where(fm => fm.DataSourceId == candidate.DataSource1Id)
                    .Select(fm => fm.FieldName)
                    .Distinct()
                    .ToList() ?? new List<string>();

                var fieldsDS2 = criterion.FieldMappings?
                    .Where(fm => fm.DataSourceId == candidate.DataSource2Id)
                    .Select(fm => fm.FieldName)
                    .Distinct()
                    .ToList() ?? new List<string>();

                List<(string f1, string f2)> fieldPairs;

                if (isDedup)
                {
                    var fields = fieldsDS1.Count > 0 ? fieldsDS1
                                : fieldsDS2.Count > 0 ? fieldsDS2
                                : new List<string>();
                    fieldPairs = fields.Select(f => (f, f)).ToList();
                }
                else
                {
                    fieldPairs = (from f1 in fieldsDS1 from f2 in fieldsDS2 select (f1, f2)).ToList();
                }

                foreach (var (f1, f2) in fieldPairs)
                {
                    var fieldKey = $"{f1}_{f2}";
                    scoreDetail.FieldScores[fieldKey] = 1.0;
                    scoreDetail.FieldWeights[fieldKey] = criterion.Weight;
                }
            }

            scoredPair.MatchDefinitionIndices.Add(definitionIndex);
            scoredPair.ScoresByDefinition[definitionIndex] = scoreDetail;
        }

        return scoredPair;
    }
    private Dictionary<Guid, Dictionary<Guid, IComparator>> BuildComparatorsForDefinitions(
        MatchDefinitionCollection matchDefinitions)
    {
        var comparatorsByDefinition = new Dictionary<Guid, Dictionary<Guid, IComparator>>();

        foreach (var definition in matchDefinitions.Definitions)
        {
            var comparators = new Dictionary<Guid, IComparator>();

            foreach (var criterion in definition.Criteria)
            {
                if (criterion.MatchingType == MatchingType.Exact &&
                    criterion.DataType != CriteriaDataType.Phonetic)
                {
                    continue;
                }

                try
                {
                    var comparator = _comparatorBuilder
                        .WithArgs(criterion.Arguments)
                        .Build();

                    comparators[criterion.Id] = comparator;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to build comparator for criterion {CriterionId}",
                        criterion.Id);
                }
            }

            comparatorsByDefinition[definition.Id] = comparators;
        }

        return comparatorsByDefinition;
    }

    public void ResetPairIdCounter()
    {
        Interlocked.Exchange(ref _pairIdCounter, 0);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;
        return ValueTask.CompletedTask;
    }

    public Task<(IAsyncEnumerable<ScoredMatchPair>, MatchGraph, Task)> CompareWithGraphAndProcessingAsync(
        IAsyncEnumerable<CandidatePair> candidates,
        MatchDefinitionCollection matchDefinitions,
        IDataSourceIndexMapper indexMapper,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    // Thread-local state to avoid lock contention
    private class ThreadLocalState
    {
        public Dictionary<Guid, Dictionary<Guid, IComparator>> Comparators { get; set; }
        public IDataSourceIndexMapper IndexMapper { get; set; }
        public double MinThreshold { get; set; }
    }

    private class ScoredPairPoolPolicy : IPooledObjectPolicy<ScoredMatchPair>
    {
        public ScoredMatchPair Create() => new();

        public bool Return(ScoredMatchPair obj)
        {
            obj.PairId = 0;
            obj.DataSource1Id = Guid.Empty;
            obj.DataSource2Id = Guid.Empty;
            obj.DataSource1Index = 0;
            obj.DataSource2Index = 0;
            obj.Row1Number = 0;
            obj.Row2Number = 0;
            obj.Record1 = null;
            obj.Record2 = null;
            obj.MatchDefinitionIndices.Clear();
            obj.ScoresByDefinition.Clear();
            obj.MaxScore = 0;
            obj.Metadata.Clear();
            return true;
        }
    }
}

/// <summary>
/// Normalized pair identity for deduplication.
/// Ensures (A,B) and (B,A) have the same identity.
/// </summary>
internal readonly struct PairIdentity : IEquatable<PairIdentity>
{
    public readonly Guid DataSource1Id;
    public readonly int Row1Number;
    public readonly Guid DataSource2Id;
    public readonly int Row2Number;

    private PairIdentity(Guid ds1, int row1, Guid ds2, int row2)
    {
        DataSource1Id = ds1;
        Row1Number = row1;
        DataSource2Id = ds2;
        Row2Number = row2;
    }

    /// <summary>
    /// Creates a normalized identity where the lexicographically smaller pair comes first.
    /// </summary>
    public static PairIdentity Create(Guid ds1, int row1, Guid ds2, int row2)
    {
        // Normalize: ensure consistent ordering
        var cmp = ds1.CompareTo(ds2);
        if (cmp < 0 || (cmp == 0 && row1 <= row2))
        {
            return new PairIdentity(ds1, row1, ds2, row2);
        }
        else
        {
            return new PairIdentity(ds2, row2, ds1, row1);
        }
    }

    public bool Equals(PairIdentity other)
        => DataSource1Id == other.DataSource1Id &&
           Row1Number == other.Row1Number &&
           DataSource2Id == other.DataSource2Id &&
           Row2Number == other.Row2Number;

    public override bool Equals(object obj)
        => obj is PairIdentity other && Equals(other);

    public override int GetHashCode()
        => HashCode.Combine(DataSource1Id, Row1Number, DataSource2Id, Row2Number);
}