using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MatchLogic.Api.Common;
using MatchLogic.Api.Configurations;
using MatchLogic.Application.Common;
using Microsoft.Extensions.Configuration;
using Serilog;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;

// Profiling SaaS entry point — slimmed from main-product Program during saas-extract.
// Removed: Desktop mode (mutex + named pipe + browser auto-open + IIS hosting),
// AppModeSettings dual-mode branching. Single-path Kestrel-on-Fargate posture.
public class Program
{
    public static async Task Main(string[] args)
    {
        StoragePaths.Initialize();
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = args,
            WebRootPath = "wwwroot"
        });

        builder.Host.UseSerilog((context, configuration) =>
            configuration.ReadFrom.Configuration(context.Configuration));

        builder.Services.Configure<FeatureFlags>(builder.Configuration.GetSection("FeatureFlags"));
        builder.AddValidationSetup();

        // TODO (M4): clamp request body size to 50 MB once edge limits are in place.
        builder.Services.Configure<FormOptions>(options =>
        {
            options.MultipartBodyLengthLimit = long.MaxValue;
        });
        builder.Services.Configure<KestrelServerOptions>(options =>
        {
            options.Limits.MaxRequestBodySize = long.MaxValue;
        });

        builder.Services.AddCors(options =>
        {
            options.AddPolicy("AllowAngularApp", b =>
            {
                b.AllowAnyOrigin()
                 .AllowAnyMethod()
                 .AllowAnyHeader()
                 .WithExposedHeaders("Content-Disposition");
            });
        });

        var startup = new Startup(builder.Configuration);
        startup.ConfigureServices(builder.Services);
        builder.AddOpenTemeletrySetup();

        var app = builder.Build();
        startup.Configure(app, app.Environment);

        await app.RunAsync();
    }
}
