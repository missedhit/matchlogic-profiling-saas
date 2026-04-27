using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.LiveSearch.SearchBatch;

public record SearchBatchRequest(
    List<Dictionary<string, object>> Records,
    BatchSearchOptions? Options = null
) : IRequest<Result<SearchBatchResponse>>;

public record BatchSearchOptions
{
   // Batch-specific options
    public int MaxBatchSize { get; init; } = 50;
    public int ChunkSize { get; init; } = 10;
    public int MaxDegreeOfParallelism { get; init; } = 4;
    public bool ContinueOnError { get; init; } = true;
}