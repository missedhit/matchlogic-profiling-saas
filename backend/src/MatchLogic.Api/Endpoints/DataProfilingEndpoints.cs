using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataProfile.AdvanceDataPreview;
using MatchLogic.Api.Handlers.DataProfile.AdvanceStatisticAnalysis;
using MatchLogic.Api.Handlers.DataProfile.DataPreview;
using MatchLogic.Api.Handlers.DataProfile.GenerateAdvanceProfile;
using MatchLogic.Api.Handlers.DataProfile.GenerateProfile;
using MatchLogic.Api.Handlers.DataProfile.StatisticAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using MatchLogic.Application.Identity;

namespace MatchLogic.Api.Endpoints;

public static class DataProfilingEndpoints
{
    public static string PATH = "DataProfile";

    public static void MapDataProfilingEndpoints(this IEndpointRouteBuilder builder, IOptions<FeatureFlags> featureFlags)
    {
        var group = builder.MapGroup($"api/{PATH}")
        .WithTags("Data Profiling");

        //Generate Data Sources from ID's
        group.MapPost("/Generate", async (GenerateDataProfileRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);
        }).Produces<GenerateDataProfileResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Profiling.Execute);

        // Get Data Source Statistical Analysis Preivew
        group.MapGet("/Analytics", async (Guid dataSourceId,IMediator mediator) =>
        {
            return await mediator.Send(new StatisticAnalysisRequest(dataSourceId));

        }).Produces<List<StatisticAnalysisResponse>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Profiling.View);

        // Get Data Source Column Data Preivew
        group.MapGet("/Data",async (IMediator mediator, Guid DataSourceId,Guid DocumentId) =>
        {            
            return await mediator.Send(new DataPreviewRequest()
            {
                DataSourceId = DataSourceId,
                DocumentId = DocumentId,
            });

        }).Produces<List<DataPreviewResponse>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Profiling.View);

        // If the feature flag for Advance Profiling is enabled, add the following endpoints
        if (featureFlags.Value.AdvanceProfiling)
        {
            // Generate Advanced Data Profile endpoint
            group.MapPost("/GenerateAdvance", async (GenerateAdvanceDataProfileRequest request, IMediator mediator) =>
            {
                return await mediator.Send(request);
            })
            .Produces<GenerateAdvanceDataProfileResponse>(StatusCodes.Status200OK)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status500InternalServerError)
            .RequireAuthorization(AppPermissions.Profiling.Execute);

            // Advance Statistic Analysis endpoint
            group.MapGet("/AdvanceAnalytics", async (Guid dataSourceId, IMediator mediator) =>
            {
                return await mediator.Send(new AdvanceStatisticAnalysisRequest(dataSourceId));
            })
            .Produces<AdvanceStatisticAnalysisResponse>(StatusCodes.Status200OK)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status404NotFound)
            .Produces(StatusCodes.Status500InternalServerError)
            .RequireAuthorization(AppPermissions.Profiling.View);
            // Get Data Source Column Data Preivew
            group.MapGet("/AdvanceData", async (IMediator mediator, Guid DataSourceId, Guid DocumentId) =>
            {
                return await mediator.Send(new AdvanceDataPreviewRequest()
                {
                    DataSourceId = DataSourceId,
                    DocumentId = DocumentId,
                });

            }).Produces<List<AdvanceDataPreviewResponse>>(StatusCodes.Status200OK)
           .Produces(StatusCodes.Status400BadRequest)
           .Produces(StatusCodes.Status500InternalServerError)
           .RequireAuthorization(AppPermissions.Profiling.View);
        }
    }
}
