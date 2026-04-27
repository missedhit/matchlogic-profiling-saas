using Ardalis.Result.AspNetCore;
using MatchLogic.Api.Handlers.HealthCheck.Echo;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using System.Threading.Tasks;

namespace MatchLogic.Api.Endpoints;

public static class HealthCheckEndpoints
{
    public static void MapHealthCheckEndpoints(this IEndpointRouteBuilder builder)
    {
        var group = builder.MapGroup("api/HealthCheck")
            .WithTags("HealthCheck");

        group.MapGet("/", async () =>
        {
            var result = await Task.FromResult("OK");
            return result;
        });

        group.MapGet("{text}", async (IMediator mediator, string text) =>
        {
            var result = await mediator.Send(new HealthCheckEchoRequest { Text = text });
            return result.ToMinimalApiResult();
        });
    }
}
