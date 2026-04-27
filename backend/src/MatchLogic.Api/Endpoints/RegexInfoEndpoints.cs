using MatchLogic.Api.Handlers.RegexInfo.Create;
using MatchLogic.Api.Handlers.RegexInfo.List;
using MatchLogic.Api.Handlers.RegexInfo.Update;
using MatchLogic.Api.Handlers.RegexInfo.Delete;
using MatchLogic.Domain.Regex;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using System;
using MatchLogic.Api.Handlers.RegexInfo.ResetSystemDefaults;
using MatchLogic.Application.Identity;

namespace MatchLogic.Api.Endpoints;

public static class RegexInfoEndpoints
{
    public const string PATH = "regex-patterns";

    public static void MapRegexInfoEndpoints(this IEndpointRouteBuilder builder)
    {
        var group = builder.MapGroup($"api/{PATH}")
            .WithTags("Regex Patterns");

        // Create regex pattern
        group.MapPost("/", async (IMediator mediator, CreateRegexInfoRequest request) =>
        {
            return await mediator.Send(request);
        }).Produces<Result<CreateRegexInfoResponse>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Dictionary.Manage);

        // Get all regex patterns
        group.MapGet("/", async (IMediator mediator) =>
        {
            return await mediator.Send(new ListRegexInfoRequest());
        }).Produces<Result<ListRegexInfoResponse>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status404NotFound)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Dictionary.View);

        // Update regex pattern
        group.MapPut("/", async (IMediator mediator, UpdateRegexInfoRequest request) =>
        {
            return await mediator.Send(request);
        }).Produces<Result<RegexInfo>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status404NotFound)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Dictionary.Manage);


        // Reset Regex to Default
        group.MapPost("/Reset", async (IMediator mediator) =>
        {
            return await mediator.Send(new ResetRegexRequest());
        }).Produces<Result<CreateRegexInfoResponse>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Dictionary.Manage);

        // Delete regex pattern
        group.MapDelete("/{id}", async (IMediator mediator, Guid id) =>
        {
            return await mediator.Send(new DeleteRegexInfoRequest(id));
        }).Produces<Result<bool>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status404NotFound)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Dictionary.Manage);
    }
}
