using MatchLogic.Api.Auth;
using MatchLogic.Api.Common;
using MatchLogic.Api.Configurations;
using MatchLogic.Api.Endpoints;
using MatchLogic.Application.Auth;
using MatchLogic.Application.Identity;
using MatchLogic.Domain.Auth.Interfaces;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mapster;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http.Json;
using MatchLogic.Infrastructure;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using Microsoft.Extensions.Options;
using Serilog;
using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Api.Middleware;
using MatchLogic.Application.Common;

// Profiling SaaS Startup — slimmed from main-product Startup during saas-extract.
// Removed: Desktop/Keycloak dual-mode auth, RBAC policies, LiveSearch rate limiter,
// License middleware, Hangfire dashboard, all non-profiling endpoint registrations.
public class Startup
{
    public IConfiguration Configuration { get; }

    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddCognitoJwtAuth(Configuration);
        services.AddAuthorization(options =>
        {
            // SaaS has a single user role (customer). Endpoints carried over from the main
            // product still call .RequireAuthorization("projects.read") etc; register every
            // permission name as "must be authenticated" so those calls succeed with a valid
            // Cognito token. Real role splits land in M5+ if/when an admin surface ships.
            foreach (var perm in AppPermissions.All())
            {
                options.AddPolicy(perm, p => p.RequireAuthenticatedUser());
            }
        });
        services.AddSwaggerSetup();

        services.AddCompressionSetup();
        services.AddHttpContextAccessor();
        services.AddScoped<ICurrentUser, CurrentUser>();

        services.AddApplicationSetup(configuration: this.Configuration);
        services.AddMediatRSetup();
        services.AddTransient<IProjectService, ProjectService>();

        var config = new TypeAdapterConfig();
        MapsterConfiguration.RegisterMappings(config);
        services.AddSingleton(config);

        services.Configure<JsonOptions>(options =>
        {
            options.SerializerOptions.NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals;
            options.SerializerOptions.Converters.Add(new MultiDimensionalArrayConverter<double>());
            options.SerializerOptions.Converters.Add(new MultiDimensionalArrayConverter<int>());
        });
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        app.UseSerilogRequestLogging();
        app.UseResponseCompression();

        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        app.UseCors("AllowAngularApp");
        app.UseStaticFiles();
        app.UseRouting();

        app.UseAuthentication();
        app.UseAuthorization();

        app.UseSwaggerSetup();
        app.UseHsts();
        app.UseMiddleware<ExceptionHandlingMiddleware>();

        using (var scope = app.ApplicationServices.CreateScope())
        {
            var migrationService = scope.ServiceProvider.GetRequiredService<IMigrationService>();
            var needMigration = migrationService.NeedsInitialization().Result;
            if (needMigration)
            {
                migrationService.InitializeDatabase().Wait();
            }
        }

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapHealthCheckEndpoints();
            endpoints.MapApiVersionEndpoints();
            endpoints.MapColumnNotesEndpoints();
            endpoints.MapProjectEndpoints();
            endpoints.MapFileDataImportEndpoints();
            endpoints.MapJobStatusEndpoints();
            endpoints.MapRegexInfoEndpoints();
            endpoints.MapDataProfilingEndpoints(app.ApplicationServices.GetRequiredService<IOptions<FeatureFlags>>());
        });
    }
}
