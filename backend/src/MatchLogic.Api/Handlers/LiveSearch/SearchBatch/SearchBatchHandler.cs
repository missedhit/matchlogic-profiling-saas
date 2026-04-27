using MatchLogic.Application.Features.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.LiveSearch.SearchBatch;

public class SearchBatchHandler(
    LiveSearchService liveSearchService,  // ← From Core - only has SearchAsync() for single record
    IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepo,
    IOptions<LiveSearchConfiguration> liveSearchConfiguration,
    ILogger<SearchBatchHandler> logger)
    : IRequestHandler<SearchBatchRequest, Result<SearchBatchResponse>>
{
    public async Task<Result<SearchBatchResponse>> Handle(
        SearchBatchRequest request,
        CancellationToken cancellationToken)
    {
        var overallStart = DateTime.UtcNow;

        try
        {
            logger.LogInformation(
                "Processing batch search: {RecordCount} records for Project: {ProjectId}",
                request.Records.Count,
                liveSearchConfiguration.Value.ProjectId);

            var options = request.Options ?? new BatchSearchOptions();

            // Store all results
            var allResults = new ConcurrentBag<BatchRecordResult>();
            var allErrors = new ConcurrentBag<BatchError>();

            // Split records into chunks of ChunkSize (default 10)
            var recordsWithIndex = request.Records
                .Select((record, index) => new { Record = record, Index = index })
                .ToList();

            var chunks = recordsWithIndex.Chunk(options.ChunkSize);

            foreach (var chunk in chunks)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                // Process each chunk with MaxDegreeOfParallelism (default 4) in parallel
                var semaphore = new SemaphoreSlim(options.MaxDegreeOfParallelism);
                var chunkTasks = chunk.Select(async item =>
                {
                    await semaphore.WaitAsync(cancellationToken);
                    try
                    {
                        var recordStart = DateTime.UtcNow;

                        // Convert API DTO to Core DTO
                        var coreRequest = new LiveSearchRequest
                        {
                            ProjectId = liveSearchConfiguration.Value.ProjectId,
                            SourceDataSourceId = liveSearchConfiguration.Value.DataSourceId,
                            Record = item.Record,
                            PerformCleansing = liveSearchConfiguration.Value.EnableCleansing,
                            QGramSize = liveSearchConfiguration.Value.QGramSize,
                            MinSimilarityThreshold = 0.3,
                            MaxCandidates = liveSearchConfiguration.Value.MaxCandidates,
                            MinScoreThreshold = liveSearchConfiguration.Value.MinScoreThreshold,
                            MaxResults = liveSearchConfiguration.Value.MaxResults,
                            Strategy = liveSearchConfiguration.Value.SearchStrategy
                        };

                        // Call LiveSearchService.SearchAsync() for single record
                        var coreResponse = await liveSearchService.SearchAsync(
                            coreRequest,
                            cancellationToken);

                        var recordDuration = DateTime.UtcNow - recordStart;

                        // Convert Core DTO to API DTO
                        return new BatchRecordResult
                        {
                            RecordIndex = item.Index,
                            InputRecord = item.Record,
                            Matches = coreResponse.QualifiedPairs.Select(pair => new BatchMatchResult
                            {
                                DataSourceId = pair.DataSource1Id == liveSearchConfiguration.Value.DataSourceId ? pair.DataSource2Id : pair.DataSource1Id,
                                DataSourceName = string.Empty, // Will populate later
                                RowNumber = pair.DataSource1Id == liveSearchConfiguration.Value.DataSourceId ? pair.Row2Number : pair.Row1Number,
                                MatchedRecord = (Dictionary<string, object>)(pair.DataSource1Id == liveSearchConfiguration.Value.DataSourceId ? pair.Record2 : pair.Record1),
                                OverallScore = pair.MaxScore,
                                FieldScores = pair.ScoresByDefinition,
                                IsPotentialMatch = pair.MaxScore >= liveSearchConfiguration.Value.MinScoreThreshold
                            }).ToList(),
                            TotalCandidates = coreResponse.CandidateCount,
                            ProcessingTimeMs = (int)recordDuration.TotalMilliseconds,
                            Success = true
                        };
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex,
                            "Failed to process record at index {Index}",
                            item.Index);

                        if (!options.ContinueOnError)
                            throw;

                        allErrors.Add(new BatchError
                        {
                            RecordIndex = item.Index,
                            InputRecord = item.Record,
                            Error = ex.Message
                        });

                        return new BatchRecordResult
                        {
                            RecordIndex = item.Index,
                            InputRecord = item.Record,
                            Matches = new List<BatchMatchResult>(),
                            TotalCandidates = 0,
                            ProcessingTimeMs = 0,
                            Success = false
                        };
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                });

                // Wait for all tasks in this chunk to complete
                var chunkResults = await Task.WhenAll(chunkTasks);

                foreach (var result in chunkResults)
                {
                    allResults.Add(result);
                }

                logger.LogInformation(
                    "Processed chunk: {Processed}/{Total} records",
                    allResults.Count,
                    request.Records.Count);
            }

            // Get all data source IDs for name lookup
            var dataSourceIds = allResults
                .SelectMany(r => r.Matches.Select(m => m.DataSourceId))
                .Distinct()
                .ToList();

            var dataSources = await dataSourceRepo.QueryAsync(
                ds => dataSourceIds.Contains(ds.Id),
                "DataSources");

            var dataSourceNames = dataSources.ToDictionary(ds => ds.Id, ds => ds.Name);

            // Update data source names in results
            var results = allResults
                .OrderBy(r => r.RecordIndex)
                .Select(r => r with
                {
                    Matches = r.Matches.Select(m => m with
                    {
                        DataSourceName = dataSourceNames.GetValueOrDefault(m.DataSourceId, "Unknown")
                    }).ToList()
                })
                .ToList();

            var overallDuration = DateTime.UtcNow - overallStart;

            // Calculate statistics
            var successfulResults = results.Where(r => r.Success).ToList();
            var statistics = new BatchStatistics
            {
                AverageMatchesPerRecord = successfulResults.Any()
                    ? (int)successfulResults.Average(r => r.Matches.Count)
                    : 0,
                AverageCandidatesPerRecord = successfulResults.Any()
                    ? (int)successfulResults.Average(r => r.TotalCandidates)
                    : 0,
                AverageProcessingTimeMs = successfulResults.Any()
                    ? (int)successfulResults.Average(r => r.ProcessingTimeMs)
                    : 0,
                MaxProcessingTimeMs = successfulResults.Any()
                    ? successfulResults.Max(r => r.ProcessingTimeMs)
                    : 0,
                MinProcessingTimeMs = successfulResults.Any()
                    ? successfulResults.Min(r => r.ProcessingTimeMs)
                    : 0
            };

            var response = new SearchBatchResponse
            {
                TotalRecords = request.Records.Count,
                ProcessedRecords = results.Count(r => r.Success),
                FailedRecords = results.Count(r => !r.Success),
                TotalMatches = results.Sum(r => r.Matches.Count),
                ProcessingTimeMs = (int)overallDuration.TotalMilliseconds,
                Results = results,
                Errors = allErrors.OrderBy(e => e.RecordIndex).ToList(),
                Statistics = statistics
            };

            logger.LogInformation(
                "Batch search completed: {Processed}/{Total} records, {Matches} matches in {Ms}ms",
                response.ProcessedRecords,
                response.TotalRecords,
                response.TotalMatches,
                response.ProcessingTimeMs);

            return Result<SearchBatchResponse>.Success(response);
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Batch search failed for Project: {ProjectId}",
                liveSearchConfiguration.Value.ProjectId);

            return Result<SearchBatchResponse>.Error($"Batch search failed: {ex.Message}");
        }
    }
}