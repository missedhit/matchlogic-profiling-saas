using MatchLogic.Application.Interfaces.Project;
using FluentValidation;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace MatchLogic.Api.Configurations;

public static class ValidationSetup
{
    public static void AddValidationSetup(this WebApplicationBuilder builder)
    {
        builder.Services.AddValidatorsFromAssemblyContaining<Application.IAssemblyMarker>();
        builder.Services.AddValidatorsFromAssemblyContaining<IAssemblyMarker>();
        
    }
}
