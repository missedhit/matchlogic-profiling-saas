using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class EnhancedRecordComparisonService : IEnhancedRecordComparisonService
    {
        private readonly ILogger<EnhancedRecordComparisonService> _logger;
        private readonly ITelemetry _telemetry;
        private readonly IComparatorBuilder _comparatorBuilder;
        private readonly ObjectPool<Dictionary<string, object>> _dictionaryPool;
        private readonly ObjectPool<ScoredMatchPair> _scoredPairPool;
        private readonly int _batchSize;
        private readonly int _maxDegreeOfParallelism;
        private readonly double _minScoreThreshold;
        private long _pairIdCounter;
        private bool _disposed;

        public EnhancedRecordComparisonService(
            ILogger<EnhancedRecordComparisonService> logger,
            ITelemetry telemetry,
            IComparatorBuilder comparatorBuilder,
            IOptions<RecordLinkageOptions> options)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
            _comparatorBuilder = comparatorBuilder ?? throw new ArgumentNullException(nameof(comparatorBuilder));

            var optionsValue = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _batchSize = optionsValue.BatchSize;
            _maxDegreeOfParallelism = optionsValue.MaxDegreeOfParallelism;
            _minScoreThreshold = optionsValue.MinimumMatchScore ?? 0.0;

            _dictionaryPool = ObjectPool.Create(new DictionaryPoolPolicy());
            _scoredPairPool = ObjectPool.Create(new ScoredPairPoolPolicy());
        }

        public async IAsyncEnumerable<ScoredMatchPair> CompareAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (matchDefinitions == null)
                throw new ArgumentNullException(nameof(matchDefinitions));
            if (indexMapper == null)
                throw new ArgumentNullException(nameof(indexMapper));

            using var operation = _telemetry.MeasureOperation("enhanced_compare_records");

            // Pre-build comparators for all definitions
            var comparatorsByDefinition = BuildComparatorsForDefinitions(matchDefinitions);

            // Create processing channel
            var outputChannel = Channel.CreateBounded<ScoredMatchPair>(
                new BoundedChannelOptions(_maxDegreeOfParallelism * _batchSize)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false
                });

            // Start processing pipeline
            var processingTask = ProcessCandidatesAsync(
                candidates,
                matchDefinitions,
                comparatorsByDefinition,
                indexMapper,
                outputChannel.Writer,
                null,
                cancellationToken);

            // Stream results
            await foreach (var scoredPair in outputChannel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return scoredPair;
            }

            await processingTask;
        }

        public async Task<(IAsyncEnumerable<ScoredMatchPair>, MatchGraph)> CompareWithGraphAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (matchDefinitions == null)
                throw new ArgumentNullException(nameof(matchDefinitions));
            if (indexMapper == null)
                throw new ArgumentNullException(nameof(indexMapper));

            using var operation = _telemetry.MeasureOperation("enhanced_compare_records_with_graph");

            var matchGraph = new MatchGraph(matchDefinitions.ProjectId);

            var comparatorsByDefinition = BuildComparatorsForDefinitions(matchDefinitions);

            var outputChannel = Channel.CreateBounded<ScoredMatchPair>(
                new BoundedChannelOptions(_maxDegreeOfParallelism * _batchSize)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false
                });

            var processingTask = ProcessCandidatesAsync(
                candidates,
                matchDefinitions,
                comparatorsByDefinition,
                indexMapper,
                outputChannel.Writer,
                matchGraph,  // Pass the graph
                cancellationToken);

            async IAsyncEnumerable<ScoredMatchPair> StreamResults()
            {
                await foreach (var scoredPair in outputChannel.Reader.ReadAllAsync(cancellationToken))
                {
                    yield return scoredPair;
                }
                await processingTask;
            }

            return (StreamResults(), matchGraph);
        }

        public async Task<(IAsyncEnumerable<ScoredMatchPair>, MatchGraph, Task)> CompareWithGraphAndProcessingAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (matchDefinitions == null)
                throw new ArgumentNullException(nameof(matchDefinitions));
            if (indexMapper == null)
                throw new ArgumentNullException(nameof(indexMapper));

            using var operation = _telemetry.MeasureOperation("enhanced_compare_records_with_graph");

            var matchGraph = new MatchGraph(matchDefinitions.ProjectId);
            var comparatorsByDefinition = BuildComparatorsForDefinitions(matchDefinitions);

            // Increased channel capacity
            var outputChannel = Channel.CreateBounded<ScoredMatchPair>(
                new BoundedChannelOptions(100000)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false
                });

            var processingTask = ProcessCandidatesAsync(
                candidates,
                matchDefinitions,
                comparatorsByDefinition,
                indexMapper,
                outputChannel.Writer,
                matchGraph,
                cancellationToken);

            async IAsyncEnumerable<ScoredMatchPair> StreamResults()
            {
                await foreach (var scoredPair in outputChannel.Reader.ReadAllAsync(cancellationToken))
                {
                    yield return scoredPair;
                }
            }

            return (StreamResults(), matchGraph, processingTask);
        }

        private async Task ProcessCandidatesAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            Dictionary<Guid, Dictionary<Guid, IComparator>> comparatorsByDefinition,
            IDataSourceIndexMapper indexMapper,
            ChannelWriter<ScoredMatchPair> writer,
            MatchGraph matchGraph = null,
            CancellationToken cancellationToken = default)
        {
            var semaphore = new SemaphoreSlim(_maxDegreeOfParallelism);
            var processingTasks = new List<Task>();

            try
            {
                await foreach (var batch in candidates.ChunkAsync(_batchSize, cancellationToken))
                {
                    await semaphore.WaitAsync(cancellationToken);

                    var batchTask = Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessBatchAsync(
                                batch,
                                matchDefinitions,
                                comparatorsByDefinition,
                                indexMapper,
                                writer,
                                matchGraph,
                                cancellationToken);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, cancellationToken);

                    processingTasks.Add(batchTask);

                    // Clean up completed tasks periodically
                    if (processingTasks.Count >= _maxDegreeOfParallelism * 2)
                    {
                        processingTasks.RemoveAll(t => t.IsCompleted);
                    }
                }

                await Task.WhenAll(processingTasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing candidates");
                throw;
            }
            finally
            {
                writer.Complete();
                semaphore.Dispose();
            }
        }

        private async Task ProcessBatchAsync(
            IList<CandidatePair> batch,
            MatchDefinitionCollection matchDefinitions,
            Dictionary<Guid, Dictionary<Guid, IComparator>> comparatorsByDefinition,
            IDataSourceIndexMapper indexMapper,
            ChannelWriter<ScoredMatchPair> writer,
            MatchGraph matchGraph = null,
            CancellationToken cancellationToken = default)
        {
            var scoringTasks = batch.Select(candidate =>
                ScoreCandidateAsync(candidate, matchDefinitions, comparatorsByDefinition, indexMapper, matchGraph));

            var scoredPairs = await Task.WhenAll(scoringTasks);

            foreach (var scoredPair in scoredPairs.Where(p => p != null))
            {
                await writer.WriteAsync(scoredPair, cancellationToken);
                _telemetry.MatchFound();
            }
        }

        private async Task<ScoredMatchPair> ScoreCandidateAsync(
            CandidatePair candidate,
            MatchDefinitionCollection matchDefinitions,
            Dictionary<Guid, Dictionary<Guid, IComparator>> comparatorsByDefinition,
            IDataSourceIndexMapper indexMapper, MatchGraph matchGraph = null)
        {
            var scoredPair = _scoredPairPool.Get();

            try
            {
                // Get actual records
                var record1 = await candidate.GetRecord1Async();
                var record2 = await candidate.GetRecord2Async();

                if (record1 == null || record2 == null)
                {
                    _logger.LogWarning("Unable to retrieve records for candidate pair");
                    return null;
                }

                var maxScore = 0.0;
                var passingDefinitions = new List<int>();

                // Score against each qualifying definition
                foreach (var definitionId in candidate.MatchDefinitionIds)
                {
                    var definition = matchDefinitions.Definitions.FirstOrDefault(d => d.Id == definitionId);
                    if (definition == null)
                    {
                        _logger.LogWarning("Definition {DefinitionId} not found", definitionId);
                        continue;
                    }

                    if (!comparatorsByDefinition.TryGetValue(definitionId, out var comparators))
                    {
                        _logger.LogWarning("No comparators found for definition {DefinitionId}", definitionId);
                        continue;
                    }

                    var scoreDetail = CalculateScore(
                        record1, record2,
                        candidate.DataSource1Id, candidate.DataSource2Id,
                        definition, comparators);

                    if (scoreDetail != null && scoreDetail.WeightedScore >= _minScoreThreshold)
                    {
                        if (indexMapper.TryGetDefinitionIndex(definitionId, out int defIndex))
                        {
                            passingDefinitions.Add(defIndex);
                            scoredPair.ScoresByDefinition[defIndex] = scoreDetail;
                            maxScore = Math.Max(maxScore, scoreDetail.WeightedScore);
                        }
                    }
                }

                if (!passingDefinitions.Any())
                {
                    _scoredPairPool.Return(scoredPair);
                    scoredPair = null;
                    return null;
                }

                scoredPair.PairId = GetNextPairId();
                scoredPair.DataSource1Id = candidate.DataSource1Id;
                scoredPair.DataSource2Id = candidate.DataSource2Id;
                scoredPair.Row1Number = candidate.Row1Number;
                scoredPair.Row2Number = candidate.Row2Number;
                scoredPair.Record1 = record1;
                scoredPair.Record2 = record2;
                scoredPair.MatchDefinitionIndices = passingDefinitions;
                scoredPair.MaxScore = maxScore;
                scoredPair.Metadata["InitialSimilarity"] = candidate.EstimatedSimilarity;

                // Map to indices
                if (indexMapper.TryGetDataSourceIndex(candidate.DataSource1Id, out int ds1Index))
                    scoredPair.DataSource1Index = ds1Index;
                if (indexMapper.TryGetDataSourceIndex(candidate.DataSource2Id, out int ds2Index))
                    scoredPair.DataSource2Index = ds2Index;

                var result = scoredPair;

                if (result != null && matchGraph != null)
                {
                    var node1 = new RecordKey(candidate.DataSource1Id, candidate.Row1Number);
                    var node2 = new RecordKey(candidate.DataSource2Id, candidate.Row2Number);

                    // Add nodes with record data
                    matchGraph.AddNode(node1, record1);
                    matchGraph.AddNode(node2, record2);

                    // Create edge details
                    var edgeDetails = new MatchEdgeDetails
                    {
                        PairId = result.PairId,
                        MaxScore = result.MaxScore,
                        MatchDefinitionIndices = new List<int>(result.MatchDefinitionIndices),
                        ScoresByDefinition = new Dictionary<int, MatchScoreDetail>(result.ScoresByDefinition),
                        MatchedAt = DateTime.UtcNow
                    };

                    matchGraph.AddEdge(node1, node2, edgeDetails);

                    // Update node metadata
                    if (matchGraph.NodeMetadata.TryGetValue(node1, out var metadata1))
                    {
                        metadata1.DegreeCount = matchGraph.AdjacencyList[node1]?.Count ?? 0;
                        foreach (var defIndex in result.MatchDefinitionIndices)
                            metadata1.ParticipatingDefinitions.Add(defIndex);
                    }

                    if (matchGraph.NodeMetadata.TryGetValue(node2, out var metadata2))
                    {
                        metadata2.DegreeCount = matchGraph.AdjacencyList[node2]?.Count ?? 0;
                        foreach (var defIndex in result.MatchDefinitionIndices)
                            metadata2.ParticipatingDefinitions.Add(defIndex);
                    }
                }

                // Don't return to pool - it will be used
                var finalResult = result;
                scoredPair = null;
                return finalResult;
            }
            finally
            {
                if (scoredPair != null)
                    _scoredPairPool.Return(scoredPair);
            }
        }

        public Task<(List<ScoredMatchPair>, MatchDefinitionCollection)> CompareAndCollectPairsAsync(IAsyncEnumerable<CandidatePair> candidates, MatchDefinitionCollection matchDefinitions, IDataSourceIndexMapper indexMapper, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<(List<ScoredMatchPair>, MatchDefinitionCollection)> CompareAndCollectPairsAsync(IAsyncEnumerable<CandidatePair> candidates, MatchDefinitionCollection matchDefinitions, IDataSourceIndexMapper indexMapper, int maxDegreeOfParallelism, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        private MatchScoreDetail CalculateScore(
            IDictionary<string, object> record1,
            IDictionary<string, object> record2,
            Guid dataSource1Id,
            Guid dataSource2Id,
            MatchLogic.Domain.Entities.MatchDefinition definition,
            Dictionary<Guid, IComparator> comparators)
        {
            var scoreDetail = new MatchScoreDetail();
            double totalWeightedScore = 0;
            double totalScore = 0;
            double totalWeight = 0;
            int criteriaCount = 0;

            foreach (var criterion in definition.Criteria)
            {
                // Get field mappings for each data source
                var field1Mapping = criterion.FieldMappings?.FirstOrDefault(fm => fm.DataSourceId == dataSource1Id);
                var field2Mapping = criterion.FieldMappings?.FirstOrDefault(fm => fm.DataSourceId == dataSource2Id);

                if (field1Mapping == null || field2Mapping == null)
                {
                    _logger.LogDebug("Missing field mapping for criterion {CriteriaId}", criterion.Id);
                    continue;
                }

                // Get field values
                if (!record1.TryGetValue(field1Mapping.FieldName, out var value1) ||
                    !record2.TryGetValue(field2Mapping.FieldName, out var value2))
                {
                    _logger.LogDebug("Missing field values for {Field1}/{Field2}",
                        field1Mapping.FieldName, field2Mapping.FieldName);
                    continue;
                }

                double score = 0;

                // For exact matching (non-phonetic), score is always 1.0 if we got here
                if (criterion.MatchingType == MatchingType.Exact && criterion.DataType != CriteriaDataType.Phonetic)
                {
                    score = 1.0;
                }
                else if (comparators.TryGetValue(criterion.Id, out var comparator))
                {
                    score = comparator.Compare(
                        value1?.ToString() ?? string.Empty,
                        value2?.ToString() ?? string.Empty);
                }
                else
                {
                    _logger.LogWarning("No comparator found for criterion {CriteriaId}", criterion.Id);
                    continue;
                }

                if (score <= double.Epsilon || score == 0)
                    return null;

                // Store field-level scores
                var fieldKey = $"{field1Mapping.FieldName}_{field2Mapping.FieldName}";
                scoreDetail.FieldScores[fieldKey] = score;
                scoreDetail.FieldWeights[fieldKey] = criterion.Weight;

                totalScore += score;
                totalWeightedScore += score * criterion.Weight;
                totalWeight += criterion.Weight;
                criteriaCount++;
            }

            if (criteriaCount == 0 || totalWeight == 0 || criteriaCount != definition.Criteria.Count)
                return null;

            scoreDetail.WeightedScore = totalWeightedScore / totalWeight;
            scoreDetail.FinalScore = totalScore / criteriaCount;

            return scoreDetail;
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
                    // Skip exact non-phonetic criteria (no comparator needed)
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
                        _logger.LogError(ex, "Failed to build comparator for criterion {CriteriaId}", criterion.Id);
                    }
                }

                comparatorsByDefinition[definition.Id] = comparators;
            }

            return comparatorsByDefinition;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetNextPairId() => Interlocked.Increment(ref _pairIdCounter);

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

        private class DictionaryPoolPolicy : IPooledObjectPolicy<Dictionary<string, object>>
        {
            public Dictionary<string, object> Create() => new(32);

            public bool Return(Dictionary<string, object> obj)
            {
                obj.Clear();
                return true;
            }
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
}
