using MatchLogic.Application.Features.Upload;
using MediatR;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using System.Reflection;

namespace MatchLogic.Api.Endpoints;

public static class ApiVersionEndpoints
{
    public static void MapApiVersionEndpoints(this IEndpointRouteBuilder builder)
    {
        var group = builder.MapGroup("api/version")
        .WithTags("Api version");

        group.MapGet("/", () =>
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version;
            string versionString = version != null ? version.ToString() : "1.0.0";
            var result = new Ardalis.Result.Result<string>(versionString);
            return result;
        })
        .DisableAntiforgery()        
        .Produces<string>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError);

    }
}
