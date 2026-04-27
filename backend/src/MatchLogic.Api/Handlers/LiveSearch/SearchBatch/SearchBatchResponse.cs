using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.LiveSearch.SearchBatch;

public record SearchBatchResponse
{
    public int TotalRecords { get; init; }
    public int ProcessedRecords { get; init; }
    public int FailedRecords { get; init; }
    public int TotalMatches { get; init; }
    public int ProcessingTimeMs { get; init; }
    public List<BatchRecordResult> Results { get; init; }
    public List<BatchError> Errors { get; init; }
    public BatchStatistics Statistics { get; init; }
}

public record BatchRecordResult
{
    public int RecordIndex { get; init; }
    public Dictionary<string, object> InputRecord { get; init; }
    public List<BatchMatchResult> Matches { get; init; }
    public int TotalCandidates { get; init; }
    public int ProcessingTimeMs { get; init; }
    public bool Success { get; init; }
}

public record BatchMatchResult
{
    public Guid DataSourceId { get; init; }
    public string DataSourceName { get; init; }
    public int RowNumber { get; init; }
    public Dictionary<string, object> MatchedRecord { get; init; }
    public double OverallScore { get; init; }
    public Dictionary<int, MatchScoreDetail> FieldScores { get; init; }
    public bool IsPotentialMatch { get; init; }
}

public record BatchError
{
    public int RecordIndex { get; init; }
    public Dictionary<string, object> InputRecord { get; init; }
    public string Error { get; init; }
}

public record BatchStatistics
{
    public int AverageMatchesPerRecord { get; init; }
    public int AverageCandidatesPerRecord { get; init; }
    public int AverageProcessingTimeMs { get; init; }
    public int MaxProcessingTimeMs { get; init; }
    public int MinProcessingTimeMs { get; init; }
}