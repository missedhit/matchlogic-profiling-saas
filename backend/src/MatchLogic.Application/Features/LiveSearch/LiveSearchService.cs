using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Interfaces.LiveSearch;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch
{
    public class LiveSearchService
    {
        private readonly LiveCandidateGenerator _candidateGenerator;
        private readonly LiveComparisonService _comparisonService;
        private readonly ILiveCleansingService _cleansingService;
        private readonly ILiveSearchMetadataCache _metadataCache;
        private readonly ILogger<LiveSearchService> _logger;

        public LiveSearchService(
            LiveCandidateGenerator candidateGenerator,
            LiveComparisonService comparisonService,
            ILiveCleansingService cleansingService,
            ILiveSearchMetadataCache metadataCache,
            ILogger<LiveSearchService> logger)
        {
            _candidateGenerator = candidateGenerator ?? throw new ArgumentNullException(nameof(candidateGenerator));
            _comparisonService = comparisonService ?? throw new ArgumentNullException(nameof(comparisonService));
            _cleansingService = cleansingService ?? throw new ArgumentException(nameof(cleansingService));
            _metadataCache = metadataCache ?? throw new ArgumentNullException(nameof(metadataCache));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Search for matches for a single record
        /// Complete end-to-end flow: candidate generation → comparison
        /// </summary>
        public async Task<LiveSearchResponse> SearchAsync(
            LiveSearchRequest request,
            CancellationToken cancellationToken = default)
        {
            var stopwatch = Stopwatch.StartNew();
            var metrics = new SearchMetrics();

            try
            {
                _logger.LogInformation(
                    "Starting live search for project {ProjectId}, data source {DataSourceId}",
                    request.ProjectId,
                    request.SourceDataSourceId);

                IDictionary<string, object> cleansedRecord = null;

                // Phase 1: Cleansing
                if(request.PerformCleansing)
                {
                    await _cleansingService.LoadProjectRulesAsync(request.ProjectId, new List<Guid> { request.SourceDataSourceId });
                    cleansedRecord = _cleansingService.CleanseRecord(request.SourceDataSourceId, request.Record);
                }

                // Phase 2: Candidate Generation — pull match definitions from the singleton cache
                // once per request and hand them down to both candidate generation and comparison
                // so neither layer re-queries MongoDB.
                var candidateStart = stopwatch.Elapsed;

                var metadata = await _metadataCache.GetAsync(request.ProjectId, cancellationToken);

                var candidatePairs = await _candidateGenerator.GenerateCandidatesAsync(
                    request.ProjectId,
                    request.SourceDataSourceId,
                    cleansedRecord ?? request.Record,
                    metadata.MatchDefinitions,
                    new LiveSearchOptions
                    {
                        QGramSize = request.QGramSize,
                        MinSimilarityThreshold = request.MinSimilarityThreshold,
                        MaxCandidates = request.MaxCandidates
                    },
                    cancellationToken);

                metrics.CandidateGenerationDuration = stopwatch.Elapsed - candidateStart;
                metrics.CandidateCount = candidatePairs.Count;

                _logger.LogInformation(
                    "Generated {Count} candidate pairs in {Duration:F2}ms",
                    candidatePairs.Count,
                    metrics.CandidateGenerationDuration.TotalMilliseconds);

                if (!candidatePairs.Any())
                {
                    _logger.LogInformation("No candidates found, returning empty result");
                    return CreateEmptyResult(request, metrics, stopwatch.Elapsed);
                }

                // Phase 2: Comparison
                var comparisonStart = stopwatch.Elapsed;

                var comparisonResult = await _comparisonService.CompareAsync(
                    request.ProjectId,
                    candidatePairs,
                    request.Strategy,
                    request.MinScoreThreshold,
                    request.MaxResults,
                    cancellationToken);

                metrics.ComparisonDuration = stopwatch.Elapsed - comparisonStart;
                metrics.QualifiedMatchCount = comparisonResult.QualifiedPairs.Count;

                _logger.LogInformation(
                    "Comparison completed in {Duration:F2}ms: {Matched}/{Candidates} qualified",
                    metrics.ComparisonDuration.TotalMilliseconds,
                    comparisonResult.QualifiedPairs.Count,
                    candidatePairs.Count);

                // Build final result
                metrics.TotalDuration = stopwatch.Elapsed;

                var result = new LiveSearchResponse
                {
                    ProjectId = request.ProjectId,
                    SourceDataSourceId = request.SourceDataSourceId,
                    QueryRecord = request.Record,
                    Strategy = request.Strategy,
                    CandidateCount = candidatePairs.Count,
                    QualifiedPairs = comparisonResult.QualifiedPairs,
                    Metrics = metrics,
                    SearchedAt = DateTime.UtcNow
                };

                _logger.LogInformation(
                    "Live search completed in {Duration:F2}ms: {Matches} matches found",
                    metrics.TotalDuration.TotalMilliseconds,
                    result.QualifiedPairs.Count);

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during live search");
                throw;
            }
        }

        /// <summary>
        /// Batch search for multiple records
        /// </summary>
        public async Task<List<LiveSearchResponse>> SearchBatchAsync(
            List<LiveSearchRequest> requests,
            CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Starting batch search for {Count} requests", requests.Count);

            var results = new List<LiveSearchResponse>();

            foreach (var request in requests)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var result = await SearchAsync(request, cancellationToken);
                results.Add(result);
            }

            _logger.LogInformation("Batch search completed: {Count} results", results.Count);
            return results;
        }

        private LiveSearchResponse CreateEmptyResult(
            LiveSearchRequest request,
            SearchMetrics metrics,
            TimeSpan totalDuration)
        {
            metrics.TotalDuration = totalDuration;

            return new LiveSearchResponse
            {
                ProjectId = request.ProjectId,
                SourceDataSourceId = request.SourceDataSourceId,
                QueryRecord = request.Record,
                Strategy = request.Strategy,
                CandidateCount = 0,
                QualifiedPairs = new List<ScoredMatchPair>(),
                Metrics = metrics,
                SearchedAt = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// Request for live search
    /// </summary>
    public class LiveSearchRequest
    {
        public Guid ProjectId { get; set; }
        public bool PerformCleansing { get; set; }
        public Guid SourceDataSourceId { get; set; }
        public IDictionary<string, object> Record { get; set; }
        public SearchStrategy Strategy { get; set; } = SearchStrategy.TopN;
        public int QGramSize { get; set; } = 3;
        public double MinSimilarityThreshold { get; set; } = 0.3;
        public int MaxCandidates { get; set; } = 1000;
        public double MinScoreThreshold { get; set; } = 0.7;
        public int MaxResults { get; set; } = 10;
    }

    /// <summary>
    /// Result of live search
    /// </summary>
    public class LiveSearchResponse
    {
        public Guid ProjectId { get; set; }
        public Guid SourceDataSourceId { get; set; }
        public IDictionary<string, object> QueryRecord { get; set; }
        public SearchStrategy Strategy { get; set; }
        public int CandidateCount { get; set; }
        public List<ScoredMatchPair> QualifiedPairs { get; set; }
        public SearchMetrics Metrics { get; set; }
        public DateTime SearchedAt { get; set; }
    }

    /// <summary>
    /// Performance metrics for search
    /// </summary>
    public class SearchMetrics
    {
        public TimeSpan CandidateGenerationDuration { get; set; }
        public TimeSpan ComparisonDuration { get; set; }
        public TimeSpan TotalDuration { get; set; }
        public int CandidateCount { get; set; }
        public int QualifiedMatchCount { get; set; }

        public double CandidateGenerationMs => CandidateGenerationDuration.TotalMilliseconds;
        public double ComparisonMs => ComparisonDuration.TotalMilliseconds;
        public double TotalMs => TotalDuration.TotalMilliseconds;
    }
    
}
