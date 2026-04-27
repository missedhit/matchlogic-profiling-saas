using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System;
using System.Collections.Generic;

namespace MatchLogic.Infrastructure.Persistence.MongoDB;

/// <summary>
/// Extension methods for registering MongoDB services
/// </summary>
public static class MongoDbServiceCollectionExtensions
{
    /// <summary>
    /// Adds MongoDB data store services to the service collection
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configuration">Configuration containing MongoDB settings</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddMongoDbDataStore(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        // Register BSON conventions
        MongoDbBsonConverter.RegisterConventions();

        // FIXED: Use AddOptions with Bind pattern for proper configuration binding
        services.AddOptions<MongoDbOptions>()
            .Bind(configuration.GetSection(MongoDbOptions.SectionName));

        services.AddOptions<MongoDbProgressOptions>()
            .Bind(configuration.GetSection(MongoDbProgressOptions.SectionName));

        // Register MongoDB client as singleton (thread-safe, connection pooling built-in)
        services.AddSingleton<IMongoClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<MongoDbOptions>>().Value;
            var settings = MongoClientSettings.FromConnectionString(options.ConnectionString);

            // Apply high-throughput settings
            settings.MaxConnectionPoolSize = options.MaxConnectionPoolSize;
            settings.MinConnectionPoolSize = options.MinConnectionPoolSize;
            settings.WaitQueueTimeout = TimeSpan.FromSeconds(options.WaitQueueTimeoutSeconds);
            settings.ConnectTimeout = TimeSpan.FromSeconds(options.ConnectTimeoutSeconds);
            settings.SocketTimeout = TimeSpan.FromSeconds(options.SocketTimeoutSeconds);
            settings.ServerSelectionTimeout = TimeSpan.FromSeconds(options.ServerSelectionTimeoutSeconds);
            settings.RetryWrites = options.RetryWrites;
            settings.RetryReads = true;

            // Set write concern
            settings.WriteConcern = options.WriteConcern switch
            {
                WriteConcernLevel.Unacknowledged => WriteConcern.Unacknowledged,
                WriteConcernLevel.Acknowledged => WriteConcern.Acknowledged,
                WriteConcernLevel.Majority => WriteConcern.WMajority,
                WriteConcernLevel.Journaled => WriteConcern.Acknowledged.With(journal: true),
                _ => WriteConcern.Acknowledged
            };

            return new MongoClient(settings);
        });

        // Register database instance
        services.AddSingleton<IMongoDatabase>(sp =>
        {
            var client = sp.GetRequiredService<IMongoClient>();
            var options = sp.GetRequiredService<IOptions<MongoDbOptions>>().Value;
            return client.GetDatabase(options.DatabaseName);
        });

        // Register the store factory
        services.AddSingleton<Func<StoreType, IDataStore>>(sp =>
        {
            var stores = new Dictionary<StoreType, Lazy<IDataStore>>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var mainOptions = sp.GetRequiredService<IOptions<MongoDbOptions>>();
            var progressOptions = sp.GetRequiredService<IOptions<MongoDbProgressOptions>>();

            // Main MongoDB store
            stores[StoreType.MongoDB] = new Lazy<IDataStore>(() =>
                new MongoDbDataStore(mainOptions, loggerFactory.CreateLogger<MongoDbDataStore>()));

            // Progress MongoDB store (can use same or different database)
            stores[StoreType.ProgressMongoDB] = new Lazy<IDataStore>(() =>
            {
                var progressOpts = new MongoDbOptions
                {
                    ConnectionString = !string.IsNullOrEmpty(progressOptions.Value.ConnectionString)
                        ? progressOptions.Value.ConnectionString
                        : mainOptions.Value.ConnectionString,
                    DatabaseName = progressOptions.Value.DatabaseName,
                    MaxConnectionPoolSize = progressOptions.Value.MaxConnectionPoolSize,
                    MinConnectionPoolSize = progressOptions.Value.MinConnectionPoolSize,
                    BulkInsertBatchSize = progressOptions.Value.BulkInsertBatchSize,
                    CollectionPrefix = progressOptions.Value.CollectionPrefix,
                    ConnectTimeoutSeconds = mainOptions.Value.ConnectTimeoutSeconds,
                    SocketTimeoutSeconds = mainOptions.Value.SocketTimeoutSeconds,
                    ServerSelectionTimeoutSeconds = mainOptions.Value.ServerSelectionTimeoutSeconds,
                    WaitQueueTimeoutSeconds = mainOptions.Value.WaitQueueTimeoutSeconds,
                    WriteConcern = mainOptions.Value.WriteConcern,
                    UseUnorderedBulkWrites = mainOptions.Value.UseUnorderedBulkWrites,
                    RetryWrites = mainOptions.Value.RetryWrites,
                    AutoCreateIndexes = mainOptions.Value.AutoCreateIndexes
                };
                return new MongoDbDataStore(
                    Options.Create(progressOpts),
                    loggerFactory.CreateLogger<MongoDbDataStore>());
            });

            // Fallback to InMemory store
            stores[StoreType.InMemory] = new Lazy<IDataStore>(() =>
                new InMemoryStore(loggerFactory.CreateLogger<InMemoryStore>()));

            // Map LiteDb to MongoDB for backwards compatibility
            stores[StoreType.LiteDb] = stores[StoreType.MongoDB];
            stores[StoreType.ProgressLiteDb] = stores[StoreType.ProgressMongoDB];

            return storeType =>
            {
                if (!stores.ContainsKey(storeType))
                    throw new ArgumentException($"Invalid store type: {storeType}");

                return stores[storeType].Value;
            };
        });

        // Register default IDataStore pointing to MongoDB
        services.AddSingleton<IDataStore>(sp =>
        {
            var storeFactory = sp.GetRequiredService<Func<StoreType, IDataStore>>();
            return storeFactory(StoreType.MongoDB);
        });

        // Register support services
        services.AddSingleton<MongoDbTransactionManager>();
        services.AddSingleton<MongoDbIndexManager>(sp =>
        {
            var database = sp.GetRequiredService<IMongoDatabase>();
            var logger = sp.GetRequiredService<ILogger<MongoDbIndexManager>>();
            return new MongoDbIndexManager(database, logger);
        });        

        return services;
    }

    /// <summary>
    /// Adds MongoDB data store with custom options action
    /// </summary>
    public static IServiceCollection AddMongoDbDataStore(
        this IServiceCollection services,
        Action<MongoDbOptions> configureOptions)
    {
        MongoDbBsonConverter.RegisterConventions();
        // FIXED: Use Configure with Action<T> properly
        services.Configure(configureOptions);

        // Register MongoDB client
        services.AddSingleton<IMongoClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<MongoDbOptions>>().Value;
            var settings = MongoClientSettings.FromConnectionString(options.ConnectionString);

            settings.MaxConnectionPoolSize = options.MaxConnectionPoolSize;
            settings.MinConnectionPoolSize = options.MinConnectionPoolSize;
            settings.WaitQueueTimeout = TimeSpan.FromSeconds(options.WaitQueueTimeoutSeconds);
            settings.ConnectTimeout = TimeSpan.FromSeconds(options.ConnectTimeoutSeconds);
            settings.SocketTimeout = TimeSpan.FromSeconds(options.SocketTimeoutSeconds);
            settings.ServerSelectionTimeout = TimeSpan.FromSeconds(options.ServerSelectionTimeoutSeconds);
            settings.RetryWrites = options.RetryWrites;

            return new MongoClient(settings);
        });

        services.AddSingleton<IMongoDatabase>(sp =>
        {
            var client = sp.GetRequiredService<IMongoClient>();
            var options = sp.GetRequiredService<IOptions<MongoDbOptions>>().Value;
            return client.GetDatabase(options.DatabaseName);
        });

        services.AddSingleton<IDataStore, MongoDbDataStore>();
        services.AddSingleton<MongoDbTransactionManager>();        

        return services;
    }

    /// <summary>
    /// Adds MongoDB health checks
    /// </summary>
    public static IHealthChecksBuilder AddMongoDbHealthCheck(
        this IHealthChecksBuilder builder,
        string name = "mongodb",
        HealthStatus failureStatus = HealthStatus.Unhealthy,
        IEnumerable<string> tags = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new MongoDbHealthCheck(
                sp.GetRequiredService<IMongoClient>(),
                sp.GetRequiredService<IOptions<MongoDbOptions>>(),
                sp.GetRequiredService<ILogger<MongoDbHealthCheck>>()),
            failureStatus,
            tags));
    }

    /// <summary>
    /// Adds MongoDB with both main and progress databases
    /// This replaces the LiteDB dual-database setup
    /// </summary>
    public static IServiceCollection AddMongoDbWithProgressDatabase(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        return services.AddMongoDbDataStore(configuration);
    }
}