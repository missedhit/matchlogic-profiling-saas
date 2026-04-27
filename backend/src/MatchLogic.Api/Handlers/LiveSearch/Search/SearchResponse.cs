using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.LiveSearch.Search;

public record SearchResponse
{
    public Dictionary<string, object> InputRecord { get; init; }
    public List<MatchResult> Matches { get; init; }
    public int TotalCandidates { get; init; }
    public int ProcessingTimeMs { get; init; }
}

public record MatchResult
{
    public Guid DataSourceId { get; init; }
    public string DataSourceName { get; init; }
    public int RowNumber { get; init; }
    public Dictionary<string, object> MatchedRecord { get; init; }
    public double OverallScore { get; init; }
    public Dictionary<int, MatchScoreDetail> FieldScores { get; init; }
    public bool IsPotentialMatch { get; init; }
}


