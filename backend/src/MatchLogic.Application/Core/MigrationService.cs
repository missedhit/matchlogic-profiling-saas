using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Application.Core;

public class MigrationService : IMigrationService
{
    private readonly ILogger<MigrationService> _logger;
    private readonly IEnumerable<IDataSeedProvider<IEntity>> _seedProviders;
    private readonly IDataStore _dataStore;

    public MigrationService(
        ILogger<MigrationService> logger,
        IEnumerable<IDataSeedProvider<IEntity>> seedProviders,
        IDataStore dataStore)
    {
        _logger = logger;
        _seedProviders = seedProviders;
        //_repository = repository;
        _dataStore = dataStore;
    }

    /// <summary>
    /// Initializes the database with seed data from all registered providers
    /// </summary>
    public async Task InitializeDatabase()
    {
        _logger.LogInformation("Starting database initialization");

        foreach (var provider in _seedProviders)
        {
            var collectionName = provider.GetCollectionName();
            if (!await provider.IsCollectionEmptyAsync(_dataStore))
            {
                _logger.LogInformation("Collection {CollectionName} is not empty. Skipping seeding.", collectionName);
                continue;
            }

            var seedData = provider.GetSeedData();

            _logger.LogInformation($"Seeding collection: {collectionName} with {seedData.Count()} items");

            // Insert all seed data for the collection
            await provider.SeedDataAsync(_dataStore);
        }

        _logger.LogInformation("Database initialization completed successfully");
    }

    /// <summary>
    /// Checks if the database needs to be initialized by looking for seed data
    /// </summary>
    public async Task<bool> NeedsInitialization()
    {
        // We'll check if any of our collections are empty
        foreach (var provider in _seedProviders)
        {
            var collectionName = provider.GetCollectionName();
            if (await provider.IsCollectionEmptyAsync(_dataStore))
            {
                _logger.LogInformation("Collection {CollectionName} is empty. Database initialization needed.", collectionName);
                return true;
            }
        }

        _logger.LogInformation("Database already initialized, skipping initialization");
        return false;
    }
}
