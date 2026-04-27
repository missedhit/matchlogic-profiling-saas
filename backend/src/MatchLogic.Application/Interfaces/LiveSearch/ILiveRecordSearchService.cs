using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Features.LiveSearch;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.LiveSearch
{
    /// <summary>
    /// Main orchestration service for live record search
    /// Coordinates cleansing, indexing, comparison, and grouping (Scoped)
    /// </summary>
    public interface ILiveRecordSearchService
    {
        /// <summary>
        /// Search for matches of a new record against existing indexed data
        /// </summary>
        Task<LiveSearchResult> SearchRecordAsync(
            Guid projectId,
            Guid sourceDataSourceId,
            IDictionary<string, object> newRecord,
            LiveSearchOptions options = null,
            CancellationToken cancellationToken = default);
    }

    public class LiveSearchResult
    {
        public Guid QueryRecordId { get; set; }
        public Guid QueryDataSourceId { get; set; }
        public List<CandidateMatch> Candidates { get; set; } = new();
        public List<QualifiedMatch> QualifiedPairs { get; set; } = new();
        public List<SuggestedGroup> SuggestedGroups { get; set; } = new();
        public LiveSearchMetrics Metrics { get; set; }
    }

    public class CandidateMatch
    {
        public Guid DataSourceId { get; set; }
        public int RowNumber { get; set; }
        public double SimilarityScore { get; set; }
        public IDictionary<string, object> RecordData { get; set; }
        public List<Guid> QualifyingDefinitions { get; set; }
    }

    public class QualifiedMatch
    {
        public long PairId { get; set; }
        public Guid MatchingDataSourceId { get; set; }
        public int MatchingRowNumber { get; set; }
        public IDictionary<string, object> MatchingRecord { get; set; }
        public double MaxScore { get; set; }
        public Dictionary<int, MatchScoreDetail> ScoresByDefinition { get; set; }
        public Dictionary<int, Guid> MatchDefinitionIndices { get; set; }
    }

    

    public class SuggestedGroup
    {
        public Guid GroupId { get; set; }
        public int GroupSize { get; set; }
        public int MatchingRecords { get; set; }
        public double ConfidenceScore { get; set; }
        public Dictionary<string, object> GroupMetadata { get; set; }
    }

    public class LiveSearchMetrics
    {
        public TimeSpan TotalDuration { get; set; }
        public TimeSpan QGramGenerationTime { get; set; }
        public TimeSpan CandidateGenerationTime { get; set; }
        public TimeSpan ComparisonTime { get; set; }
        public TimeSpan GroupingTime { get; set; }
        public TimeSpan CleansingTime { get; set; }
        public int TotalCandidates { get; set; }
        public int QualifiedPairs { get; set; }
        public int QGramsGenerated { get; set; }
    }

    public class LiveSearchOptions
    {
        public SearchStrategy Strategy { get; set; } = SearchStrategy.TopN;
        public bool ApplyCleansing { get; set; } = true;
        public int MaxCandidates { get; set; } = 1000;
        public int MaxResults { get; set; } = 100;
        public double MinScoreThreshold { get; set; } = 0.7;
        public bool IncludeGroupSuggestions { get; set; } = false;
        public bool MergeDuplicatePairs { get; set; } = true;
        public bool IncludeRecordData { get; set; } = true;

        public static LiveSearchOptions Default() => new();
    }
    
}