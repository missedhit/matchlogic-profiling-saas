using Azure.Core;
using MatchLogic.Api.Handlers.Cleansing.Get;
using MatchLogic.Api.Handlers.ColumnNotes;
using MatchLogic.Api.Handlers.ColumnNotes.Get;
using MatchLogic.Api.Handlers.ColumnNotes.Update;
using MatchLogic.Domain.Project;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using System;
using MatchLogic.Application.Identity;

namespace MatchLogic.Api.Endpoints;

public static class ColumnNotesEndpoints
{
    public static string PATH = "ColumnNotes";
    public static void MapColumnNotesEndpoints(this IEndpointRouteBuilder builder)
    {
        var group = builder.MapGroup($"api/{PATH}")
        .WithTags("Column Notes");

        group.MapGet("/", async (IMediator mediator, Guid dataSourceId) =>
        {
            var result = await mediator.Send(new GetColumnNotesQuery { DataSourceId = dataSourceId });
            return Result<DataSourceColumnNotesDto>.Success(result);
        })
        .DisableAntiforgery()
        .Produces<Result<DataSourceColumnNotesDto>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Projects.Read);

        group.MapPost("/", async (Guid dataSourceId,
        [FromBody] UpsertColumnNotesRequest request, IMediator mediator) =>
        {
            var result = await mediator.Send(new UpsertColumnNotesCommand
            {
                DataSourceId = dataSourceId,
                Request = request
            });
            return Result<DataSourceColumnNotesDto>.Success(result);

        }).Produces<Result<DataSourceColumnNotesDto>>(StatusCodes.Status200OK)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status500InternalServerError)
            .RequireAuthorization(AppPermissions.Projects.Read);
    }
}
