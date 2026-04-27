using MatchLogic.Application.Common;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

// Using alias to avoid confusion with LiveSearchConfiguration typing

namespace MatchLogic.Application.Features.LiveSearch
{
    /// <summary>
    /// Live comparison service that works with CandidatePair objects
    /// CandidatePair objects should be created by LiveCandidateGenerator, not here
    /// This service only orchestrates the comparison strategies
    /// </summary>
    public class LiveComparisonService
    {
        private readonly IEnhancedRecordComparisonService _batchComparisonService;
        private readonly IDataSourceIndexMapper _indexMapper;
        private readonly IGenericRepository<MatchLogic.Domain.Entities.MatchDefinitionCollection, Guid> _matchDefRepo;
        private readonly ILiveSearchMetadataCache _metadataCache;
        private readonly LiveSearchConfiguration _liveSearchConfig;
        private readonly ILogger<LiveComparisonService> _logger;

        public LiveComparisonService(
            IEnhancedRecordComparisonService batchComparisonService,
            IDataSourceIndexMapper indexMapper,
            IGenericRepository<MatchLogic.Domain.Entities.MatchDefinitionCollection, Guid> matchDefRepo,
            ILiveSearchMetadataCache metadataCache,
            IOptions<LiveSearchConfiguration> liveSearchConfig,
            ILogger<LiveComparisonService> logger)
        {
            _batchComparisonService = batchComparisonService ?? throw new ArgumentNullException(nameof(batchComparisonService));
            _indexMapper = indexMapper ?? throw new ArgumentNullException(nameof(indexMapper));
            _matchDefRepo = matchDefRepo ?? throw new ArgumentNullException(nameof(matchDefRepo));
            _metadataCache = metadataCache ?? throw new ArgumentNullException(nameof(metadataCache));
            _liveSearchConfig = liveSearchConfig?.Value ?? throw new ArgumentNullException(nameof(liveSearchConfig));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Compare using different strategies
        /// CandidatePair objects already contain record stores, no need to create them
        /// </summary>
        public async Task<LiveSearchResult> CompareAsync(
            Guid projectId,
            List<CandidatePair> candidatePairs,
            SearchStrategy strategy,
            double minScoreThreshold,
            int maxResults,
            CancellationToken cancellationToken = default)
        {
            if (candidatePairs == null || !candidatePairs.Any())
            {
                return new LiveSearchResult
                {
                    QualifiedPairs = new List<ScoredMatchPair>(),
                    TotalCandidates = 0,
                    TotalMatches = 0
                };
            }

            _logger.LogDebug(
                "Comparing {Count} candidate pairs using {Strategy} strategy",
                candidatePairs.Count,
                strategy);

            // Pull pre-loaded metadata (match defs + data source index map) from the singleton cache
            // so we don't hit Mongo on every request and don't pay the scoped-per-request re-init
            // of DataSourceIndexMapper that used to happen here.
            var meta = await _metadataCache.GetAsync(projectId, cancellationToken);
            var indexMapper = (IDataSourceIndexMapper)new CachedDataSourceIndexMapper(meta);

            return strategy switch
            {
                SearchStrategy.FastBinary => await CompareFastBinaryAsync(
                    projectId,
                    candidatePairs,
                    meta.MatchDefinitions,
                    indexMapper,
                    minScoreThreshold,
                    cancellationToken),

                SearchStrategy.TopN => await CompareTopNAsync(
                    projectId,
                    candidatePairs,
                    meta.MatchDefinitions,
                    indexMapper,
                    minScoreThreshold,
                    maxResults,
                    cancellationToken),

                SearchStrategy.Comprehensive => await CompareComprehensiveAsync(
                    projectId,
                    candidatePairs,
                    meta.MatchDefinitions,
                    indexMapper,
                    minScoreThreshold,
                    cancellationToken),

                _ => await CompareTopNAsync(
                    projectId,
                    candidatePairs,
                    meta.MatchDefinitions,
                    indexMapper,
                    minScoreThreshold,
                    maxResults,
                    cancellationToken)
            };
        }

        #region Strategy Implementations

        /// <summary>
        /// FastBinary: Compare candidates sequentially until first match found
        /// </summary>
        private async Task<LiveSearchResult> CompareFastBinaryAsync(
            Guid projectId,
            List<CandidatePair> candidatePairs,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            double minScoreThreshold,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("FastBinary: Trying up to {Count} candidates", candidatePairs.Count);

            // Sort by estimated similarity from q-gram matching
            var sortedPairs = candidatePairs
                .OrderByDescending(p => p.EstimatedSimilarity)
                .ToList();

            var perRequestParallelism = _liveSearchConfig.QueryMaxDegreeOfParallelism;

            // Try candidates one by one
            foreach (var pair in sortedPairs)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Compare single pair - use the live-search-scoped parallelism so concurrent
                // requests don't oversubscribe the CPU.
                var singlePairList = new List<CandidatePair> { pair };
                var (scoredPairs, _) = await _batchComparisonService.CompareAndCollectPairsAsync(
                    singlePairList.ToAsyncEnumerable(),
                    matchDefinitions,
                    indexMapper,
                    perRequestParallelism,
                    cancellationToken);

                if (scoredPairs.Any() && scoredPairs[0].MaxScore >= minScoreThreshold)
                {
                    _logger.LogInformation("FastBinary: Found match with score {Score:F3}", scoredPairs[0].MaxScore);

                    return new LiveSearchResult
                    {
                        QualifiedPairs = scoredPairs,
                        TotalCandidates = candidatePairs.Count,
                        TotalMatches = 1,
                        Strategy = SearchStrategy.FastBinary
                    };
                }
            }

            _logger.LogDebug("FastBinary: No matches found above threshold {Threshold:F3}", minScoreThreshold);

            return new LiveSearchResult
            {
                QualifiedPairs = new List<ScoredMatchPair>(),
                TotalCandidates = candidatePairs.Count,
                TotalMatches = 0,
                Strategy = SearchStrategy.FastBinary
            };
        }

        /// <summary>
        /// TopN: Compare all candidates, return top N by score
        /// </summary>
        private async Task<LiveSearchResult> CompareTopNAsync(
            Guid projectId,
            List<CandidatePair> candidatePairs,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            double minScoreThreshold,
            int maxResults,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("TopN: Comparing all {Count} candidates", candidatePairs.Count);

            _logger.LogInformation("Match Definition Count = {matchDefinitionCount}",
                matchDefinitions.Definitions.Count);

            _logger.LogInformation("Candidate Pair Count = {candidatePairCount}",
                candidatePairs.Count);

            var perRequestParallelism = _liveSearchConfig.QueryMaxDegreeOfParallelism;

            // Compare all pairs using the live-search-scoped parallelism cap.
            var (scoredPairs, _) = await _batchComparisonService.CompareAndCollectPairsAsync(
                candidatePairs.ToAsyncEnumerable(),
                matchDefinitions,
                indexMapper,
                perRequestParallelism,
                cancellationToken);

            _logger.LogInformation("Scored Pair Count = {scoredPairCount}",
                scoredPairs.Count);

            // Filter and sort
            var topN = scoredPairs
                .Where(p => p.MaxScore >= minScoreThreshold)
                .OrderByDescending(p => p.MaxScore)
                .Take(maxResults)
                .ToList();

            _logger.LogInformation("TopN: Found {Count} matches above threshold {Threshold:F3}",
                topN.Count, minScoreThreshold);

            return new LiveSearchResult
            {
                QualifiedPairs = topN,
                TotalCandidates = candidatePairs.Count,
                TotalMatches = topN.Count,
                Strategy = SearchStrategy.TopN
            };
        }

        /// <summary>
        /// Comprehensive: Compare all candidates, return all matches
        /// </summary>
        private async Task<LiveSearchResult> CompareComprehensiveAsync(
            Guid projectId,
            List<CandidatePair> candidatePairs,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            double minScoreThreshold,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Comprehensive: Full comparison of {Count} candidates", candidatePairs.Count);

            var perRequestParallelism = _liveSearchConfig.QueryMaxDegreeOfParallelism;

            // Compare all pairs using the live-search-scoped parallelism cap.
            var (scoredPairs, _) = await _batchComparisonService.CompareAndCollectPairsAsync(
                candidatePairs.ToAsyncEnumerable(),
                matchDefinitions,
                indexMapper,
                perRequestParallelism,
                cancellationToken);

            // Filter by threshold
            var qualified = scoredPairs
                .Where(p => p.MaxScore >= minScoreThreshold)
                .OrderByDescending(p => p.MaxScore)
                .ToList();

            _logger.LogInformation("Comprehensive: Found {Count} matches above threshold {Threshold:F3}",
                qualified.Count, minScoreThreshold);

            return new LiveSearchResult
            {
                QualifiedPairs = qualified,
                TotalCandidates = candidatePairs.Count,
                TotalMatches = qualified.Count,
                Strategy = SearchStrategy.Comprehensive
            };
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Load match definitions for the candidate pairs
        /// </summary>
        private async Task<MatchDefinitionCollection> LoadMatchDefinitionsAsync(
            Guid projectId,
            List<CandidatePair> candidatePairs,
            CancellationToken cancellationToken)
        {
            // Load from database
            var definitions = (await _matchDefRepo.QueryAsync(
                md => md.ProjectId == projectId,
                Constants.Collections.MatchDefinitionCollection)).First();

            _logger.LogDebug("Loaded {Count} match definitions for project {ProjectId}",
                definitions.Definitions.Count, projectId);

            return definitions;
        }

        #endregion
    }

    /// <summary>
    /// Result of live search comparison
    /// </summary>
    public class LiveSearchResult
    {
        public List<ScoredMatchPair> QualifiedPairs { get; set; }
        public int TotalCandidates { get; set; }
        public int TotalMatches { get; set; }
        public SearchStrategy Strategy { get; set; }
    }

    /// <summary>
    /// Search strategies for live comparison
    /// </summary>
    public enum SearchStrategy
    {
        /// <summary>
        /// Compare until first match found (fastest)
        /// </summary>
        FastBinary,

        /// <summary>
        /// Compare all, return top N (balanced)
        /// </summary>
        TopN,

        /// <summary>
        /// Compare all, return all matches (most thorough)
        /// </summary>
        Comprehensive
    }

    /// <summary>
    /// Extension method to convert IEnumerable to IAsyncEnumerable
    /// </summary>
    public static class AsyncEnumerableExtensions
    {
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
            this IEnumerable<T> source,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var item in source)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }
}