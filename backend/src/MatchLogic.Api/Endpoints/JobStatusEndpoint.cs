using MatchLogic.Api.Handlers.JobInfo;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using System;
using System.Collections.Generic;
using MatchLogic.Application.Identity;

namespace MatchLogic.Api.Endpoints;

public static class JobStatusEndpoint
{
    public static string PATH = "Run";
    public static void MapJobStatusEndpoints(this IEndpointRouteBuilder builder)
    {

        var group = builder.MapGroup("api/" + PATH).
            WithTags("Run Info");

        //Job Status
        group.MapGet("/Status/{id}", async (Guid id, IMediator mediator) =>
        {
            return await mediator.Send(new JobRunStatusRequest(id));
        })
        .Produces<JobRunStatusResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .AllowAnonymous();

        //Cancel Job
        group.MapPost("/Cancel/{id}", async (Guid id, IMediator mediator) =>
        {
            return await mediator.Send(new CancelJobRunRequest(id));
        })
        .Produces<CancelJobRunResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Projects.Read);

    }
}
