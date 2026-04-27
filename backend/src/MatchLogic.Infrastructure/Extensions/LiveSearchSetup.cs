using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.LiveSearch;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Infrastructure.Configuration;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure
{
    /// <summary>
    /// Extension methods for Live Search dependency injection
    /// </summary>
    public static class LiveSearchSetup
    {
        /// <summary>
        /// Add Live Search services (only when Mode = LiveSearch)
        /// </summary>
        public static IServiceCollection AddLiveSearchServices(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            // Get operation config
            var operationConfig = configuration.GetSection("Application:Operation")
                .Get<ApplicationOperationConfig>() ?? new ApplicationOperationConfig();

            // Only register if in LiveSearch mode
            // TODO: Conditional registeration not working due to mediatr handler registerations.
            /*if (operationConfig.Mode != OperationMode.LiveSearch)
            {
                return services;
            }*/

            Console.WriteLine("Registering Live Search services...");

            // Configure Live Search options
            services.Configure<LiveSearchConfiguration>(
                configuration.GetSection("Application:LiveSearch"));

            // Register ProductionQGramIndexerDME as Singleton (shared by all Live Search operations)
            // This is the core indexer that both batch and live search use
            services.AddSingleton<ProductionQGramIndexerDME>();

            // Register Live Search Persistence Layer
            services.AddSingleton<IIndexPersistenceService, IndexPersistenceService>();
            services.AddSingleton<IQGramIndexManager, QGramIndexManager>();
            services.AddSingleton<IRecordStoreManager, RecordStoreManager>();

            // Singleton cache of static per-project metadata (match definitions, data sources,
            // index maps) used on the Live Search hot path. Loaded once per project on first use.
            services.AddSingleton<ILiveSearchMetadataCache, LiveSearchMetadataCache>();

            // Register Live Search Core Services
            services.AddSingleton<LiveSearchIndexBuilder>();
            services.AddScoped<IndexNodeInitializer>();
            services.AddScoped<LiveSearchService>();
            services.AddScoped<LiveCandidateGenerator>();
            services.AddScoped<LiveComparisonService>();

            // Register Live Search Supporting Services
            services.AddScoped<ILiveCleansingService, LiveCleansingService>();

            // Note: IEnhancedRecordComparisonService is registered in shared services
            // as it's used by both batch and live search

            Console.WriteLine("✅ Live Search services registered successfully");

            return services;
        }

        /// <summary>
        /// Initialize Live Search node at startup (only in LiveSearch mode)
        /// Call this in your Program.cs/Startup after building the service provider
        /// </summary>
        public static async Task InitializeLiveSearchNodeAsync(
            this IServiceProvider serviceProvider,
            IConfiguration configuration)
        {
            // Get operation config
            var operationConfig = configuration.GetSection("Application:Operation")
                .Get<ApplicationOperationConfig>() ?? new ApplicationOperationConfig();

            // Only initialize if in LiveSearch mode
            if (operationConfig.Mode != OperationMode.LiveSearch)
            {
                return;
            }

            var logger = serviceProvider.GetRequiredService<ILogger<IndexNodeInitializer>>();
            logger.LogInformation("Initializing Live Search node...");

            try
            {
                var initializer = serviceProvider.GetRequiredService<IndexNodeInitializer>();
                await initializer.InitializeAsync();

                logger.LogInformation("✅ Live Search node initialized successfully");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "❌ Failed to initialize Live Search node");
                throw;
            }
        }
    }
}