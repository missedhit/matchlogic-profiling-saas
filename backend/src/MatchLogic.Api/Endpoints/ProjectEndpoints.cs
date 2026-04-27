using Azure.Core;
using MatchLogic.Api.Handlers.Project.ById;
using MatchLogic.Api.Handlers.Project.Create;
using MatchLogic.Api.Handlers.Project.Delete;
using MatchLogic.Api.Handlers.Project.List;
using MatchLogic.Api.Handlers.Project.Update;
using MatchLogic.Application.Identity;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using System;

namespace MatchLogic.Api.Endpoints;

public static class ProjectEndpoints
{
    public static readonly string PATH = "projects";

    public static void MapProjectEndpoints(this IEndpointRouteBuilder builder)
    {
        var group = builder.MapGroup("api/" + PATH).
            WithTags("Projects");
        
        //Create project
        group.MapPost("/", async (IMediator mediator, CreateProjectRequest request) =>
        {
            return await mediator.Send(request);
        }).Produces<CreateProjectResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Projects.Create);

        //List all projects
        group.MapGet("/", async (IMediator mediator) =>
        {
            return await mediator.Send(new ProjectListRequest());
        }).Produces<ProjectListResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Projects.Read);

        //Update project
        group.MapPut("/", async (IMediator mediator, UpdateProjectRequest request) =>
        {
            return await mediator.Send(request);
        }).Produces<UpdateProjectResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Projects.Update);

        //Delete project
        group.MapDelete("/{id}", async (IMediator mediator, Guid id) =>
        {
            return await mediator.Send(new DeleteProjectRequest(id));
        }).Produces<DeleteProjectResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Projects.Delete);

        //Get project by id
        group.MapGet("/{id}", async (IMediator mediator, Guid id) =>
        {
            return await mediator.Send(new GetProjectRequest(id));
        }).Produces<GetProjectResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.Projects.Read);

    }
}
