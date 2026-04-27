using MatchLogic.Application.Common.Behaviors;
using Microsoft.Extensions.DependencyInjection;

namespace MatchLogic.Api.Configurations;

public static class MediatRSetup
{
    public static IServiceCollection AddMediatRSetup(this IServiceCollection services)
    {
        services.AddMediatR((config) =>
        {
            config.RegisterServicesFromAssemblyContaining(typeof(MatchLogic.Application.IAssemblyMarker));
            config.RegisterServicesFromAssemblyContaining(typeof(MatchLogic.Api.IAssemblyMarker));
            config.AddOpenBehavior(typeof(ValidationResultPipelineBehavior<,>));
        });
        


        return services;
    }
}