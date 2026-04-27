

using MatchLogic.Infrastructure;
using FluentAssertions.Common;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;


namespace MatchLogic.Api.IntegrationTests;
public class WebAPIFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        string _dbPath = Path.GetTempFileName();
        string _dbJobPath = Path.GetTempFileName();

        builder.ConfigureAppConfiguration((context, config) =>
        {
            var settings = new Dictionary<string, string>
            {
                { "Security:MasterKey", "ZAP59dT7sqFv9AdPw0bPrJpKaAE2DHtB" }
            };

            config.AddInMemoryCollection(settings);
        });

        builder.ConfigureServices(srv =>
        {
            srv.AddApplicationSetup(_dbPath, _dbJobPath);
            srv.AddLogging(builder => builder.AddConsole());

        });
        /*builder.ConfigureTestServices(services =>
        {
            services.AddApplicationSetup(_dbPath);
            services.AddLogging(builder => builder.AddConsole());
        });*/
        builder.UseEnvironment("Development");
    }

}