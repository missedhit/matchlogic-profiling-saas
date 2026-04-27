using MatchLogic.Application.Features.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.LiveSearch.Search;

public class SearchHandler(
    LiveSearchService liveSearchService,  // ← From Core DLL - already exists!
    ILiveSearchMetadataCache metadataCache,
    IOptions<LiveSearchConfiguration> liveSearchConfiguration,
    ILogger<SearchHandler> logger)
    : IRequestHandler<SearchRequest, Result<SearchResponse>>
{
    public async Task<Result<SearchResponse>> Handle(
        SearchRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            logger.LogInformation(
                "Processing search request for Project: {ProjectId}, DataSource: {DataSourceId}",
                liveSearchConfiguration.Value.ProjectId,
                liveSearchConfiguration.Value.DataSourceId);

            // Convert API DTO to Core DTO
            var coreRequest = new LiveSearchRequest
            {
                ProjectId = liveSearchConfiguration.Value.ProjectId,
                SourceDataSourceId = liveSearchConfiguration.Value.DataSourceId,
                Record = request.Record,
                PerformCleansing = liveSearchConfiguration.Value.EnableCleansing,
                QGramSize = liveSearchConfiguration.Value.QGramSize,
                MinSimilarityThreshold = 0.3,
                MaxCandidates = liveSearchConfiguration.Value.MaxCandidates,
                MinScoreThreshold = liveSearchConfiguration.Value.MinScoreThreshold,
                MaxResults = liveSearchConfiguration.Value.MaxResults,
                Strategy = liveSearchConfiguration.Value.SearchStrategy
            };

            // Call existing LiveSearchService from Core
            var coreResponse = await liveSearchService.SearchAsync(coreRequest, cancellationToken);

            // Pull data source names from the singleton metadata cache instead of re-querying Mongo
            // on every request.
            var metadata = await metadataCache.GetAsync(
                liveSearchConfiguration.Value.ProjectId, cancellationToken);
            var dataSourceNames = metadata.DataSourceNames;

            // Convert Core DTO to API DTO
            var apiResponse = new SearchResponse
            {
                InputRecord = request.Record,
                Matches = coreResponse.QualifiedPairs.Select(pair => new MatchResult
                {
                    DataSourceId = pair.DataSource2Id,
                    DataSourceName = dataSourceNames.GetValueOrDefault(pair.DataSource2Id, "Unknown"),
                    RowNumber = Convert.ToInt32((pair.Record2["_metadata"] as Dictionary<string, object>)["RowNumber"]),
                    MatchedRecord = (Dictionary<string, object>)(pair.Record2),
                    OverallScore = pair.MaxScore,
                    FieldScores = pair.ScoresByDefinition,
                    IsPotentialMatch = pair.MaxScore >= liveSearchConfiguration.Value.MinScoreThreshold
                }).ToList(),
                TotalCandidates = coreResponse.CandidateCount,
                ProcessingTimeMs = (int)coreResponse.Metrics.TotalMs,
            };

            logger.LogInformation(
                "Search completed: {Matches} matches from {Candidates} candidates in {Ms}ms",
                apiResponse.Matches.Count,
                apiResponse.TotalCandidates,
                apiResponse.ProcessingTimeMs);

            return Result<SearchResponse>.Success(apiResponse);
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Search failed for Project: {ProjectId}, DataSource: {DataSourceId}",
                liveSearchConfiguration.Value.ProjectId,
                liveSearchConfiguration.Value.DataSourceId);

            return Result<SearchResponse>.Error($"Search failed: {ex.Message}");
        }
    }
}