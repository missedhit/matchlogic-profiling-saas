using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence.MongoDB;

/// <summary>
/// Provides transaction support for MongoDB operations
/// Requires MongoDB 4.0+ with replica set or sharded cluster
/// </summary>
public class MongoDbTransactionManager : IDisposable
{
    private readonly IMongoClient _client;
    private readonly ILogger<MongoDbTransactionManager> _logger;
    private readonly MongoDbOptions _options;
    private bool _disposed;

    public MongoDbTransactionManager(
        IMongoClient client,
        IOptions<MongoDbOptions> options,
        ILogger<MongoDbTransactionManager> logger)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Executes an action within a transaction
    /// </summary>
    public async Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<IClientSessionHandle, CancellationToken, Task<TResult>> action,
        TransactionOptions transactionOptions = null,
        CancellationToken cancellationToken = default)
    {
        using var session = await _client.StartSessionAsync(cancellationToken: cancellationToken);

        transactionOptions ??= new TransactionOptions(
            readConcern: ReadConcern.Majority,
            writeConcern: WriteConcern.WMajority,
            readPreference: ReadPreference.Primary,
            maxCommitTime: TimeSpan.FromSeconds(60));

        try
        {
            TResult result = default;

            await session.WithTransactionAsync(
                async (s, ct) =>
                {
                    result = await action(s, ct);
                    return result;
                },
                transactionOptions,
                cancellationToken);

            return result;
        }
        catch (MongoException ex)
        {
            _logger.LogError(ex, "Transaction failed");
            throw;
        }
    }

    /// <summary>
    /// Executes an action within a transaction (void return)
    /// </summary>
    public async Task ExecuteInTransactionAsync(
        Func<IClientSessionHandle, CancellationToken, Task> action,
        TransactionOptions transactionOptions = null,
        CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(
            async (session, ct) =>
            {
                await action(session, ct);
                return true;
            },
            transactionOptions,
            cancellationToken);
    }

    /// <summary>
    /// Bulk insert with transaction support
    /// </summary>
    public async Task BulkInsertWithTransactionAsync<T>(
        IMongoCollection<T> collection,
        IEnumerable<T> documents,
        CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(async (session, ct) =>
        {
            await collection.InsertManyAsync(
                session,
                documents,
                new InsertManyOptions { IsOrdered = false },
                ct);
        }, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Checks if the MongoDB deployment supports transactions
    /// </summary>
    public async Task<bool> SupportsTransactionsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var session = await _client.StartSessionAsync(cancellationToken: cancellationToken);

            // Try to start a transaction - this will fail on standalone servers
            session.StartTransaction();
            await session.AbortTransactionAsync(cancellationToken);

            return true;
        }
        catch (NotSupportedException)
        {
            _logger.LogWarning("MongoDB deployment does not support transactions. " +
                "Ensure you're using a replica set or sharded cluster for transaction support.");
            return false;
        }
        catch (MongoException ex) when (ex.Message.Contains("transaction"))
        {
            _logger.LogWarning("MongoDB deployment does not support transactions: {Message}", ex.Message);
            return false;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
    }
}

/// <summary>
/// Health check for MongoDB connectivity
/// </summary>
public class MongoDbHealthCheck : IHealthCheck
{
    private readonly IMongoClient _client;
    private readonly MongoDbOptions _options;
    private readonly ILogger<MongoDbHealthCheck> _logger;

    public MongoDbHealthCheck(
        IMongoClient client,
        IOptions<MongoDbOptions> options,
        ILogger<MongoDbHealthCheck> logger)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var database = _client.GetDatabase(_options.DatabaseName);

            // Ping the database
            await database.RunCommandAsync<BsonDocument>(
                new BsonDocument("ping", 1),
                cancellationToken: cancellationToken);

            // Get server status for additional info
            var serverStatus = await database.RunCommandAsync<BsonDocument>(
                new BsonDocument("serverStatus", 1),
                cancellationToken: cancellationToken);

            var data = new Dictionary<string, object>
            {
                ["database"] = _options.DatabaseName,
                ["version"] = serverStatus.GetValue("version", "unknown").AsString,
                ["uptime"] = serverStatus.GetValue("uptime", 0).ToDouble()
            };

            return HealthCheckResult.Healthy("MongoDB is healthy", data);
        }
        catch (MongoException ex)
        {
            _logger.LogError(ex, "MongoDB health check failed");
            return HealthCheckResult.Unhealthy(
                "MongoDB is unhealthy",
                ex,
                new Dictionary<string, object>
                {
                    ["error"] = ex.Message,
                    ["database"] = _options.DatabaseName
                });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error during MongoDB health check");
            return HealthCheckResult.Unhealthy("MongoDB health check failed", ex);
        }
    }
}

/// <summary>
/// Manages MongoDB indexes for optimal query performance
/// </summary>
public class MongoDbIndexManager
{
    private readonly IMongoDatabase _database;
    private readonly ILogger<MongoDbIndexManager> _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public MongoDbIndexManager(
        IMongoDatabase database,
        ILogger<MongoDbIndexManager> logger)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Ensures all required indexes exist for a collection
    /// </summary>
    public async Task EnsureIndexesAsync<T>(
        string collectionName,
        IEnumerable<CreateIndexModel<T>> indexModels,
        CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            var collection = _database.GetCollection<T>(collectionName);
            await collection.Indexes.CreateManyAsync(indexModels, cancellationToken);
            _logger.LogInformation("Ensured indexes for collection: {Collection}", collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create indexes for collection: {Collection}", collectionName);
            throw;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Creates a compound index for sorting and filtering
    /// </summary>
    public async Task CreateSortFilterIndexAsync(
        string collectionName,
        string[] sortFields,
        string[] filterFields = null,
        CancellationToken cancellationToken = default)
    {
        var collection = _database.GetCollection<BsonDocument>(collectionName);
        var indexKeysBuilder = Builders<BsonDocument>.IndexKeys;

        var keys = new List<IndexKeysDefinition<BsonDocument>>();

        // Add filter fields first (equality conditions)
        if (filterFields != null)
        {
            foreach (var field in filterFields)
            {
                keys.Add(indexKeysBuilder.Ascending(field));
            }
        }

        // Add sort fields
        foreach (var field in sortFields)
        {
            keys.Add(indexKeysBuilder.Descending(field));
        }

        var combinedKeys = indexKeysBuilder.Combine(keys);
        var indexModel = new CreateIndexModel<BsonDocument>(
            combinedKeys,
            new CreateIndexOptions
            {
                Background = true,
                Name = $"idx_{string.Join("_", sortFields)}"
            });

        await collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogInformation(
            "Created compound index on {Collection} for fields: {Fields}",
            collectionName, string.Join(", ", sortFields));
    }

    /// <summary>
    /// Creates a text index for full-text search
    /// </summary>
    public async Task CreateTextIndexAsync(
        string collectionName,
        string[] textFields,
        CancellationToken cancellationToken = default)
    {
        var collection = _database.GetCollection<BsonDocument>(collectionName);
        var indexKeysBuilder = Builders<BsonDocument>.IndexKeys;

        var keys = textFields.Select(f => indexKeysBuilder.Text(f)).ToList();
        var combinedKeys = indexKeysBuilder.Combine(keys);

        var indexModel = new CreateIndexModel<BsonDocument>(
            combinedKeys,
            new CreateIndexOptions
            {
                Background = true,
                Name = "idx_text_search"
            });

        await collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogInformation(
            "Created text index on {Collection} for fields: {Fields}",
            collectionName, string.Join(", ", textFields));
    }

    /// <summary>
    /// Lists all indexes on a collection
    /// </summary>
    public async Task<List<BsonDocument>> ListIndexesAsync(
        string collectionName,
        CancellationToken cancellationToken = default)
    {
        var collection = _database.GetCollection<BsonDocument>(collectionName);
        var indexes = new List<BsonDocument>();

        using var cursor = await collection.Indexes.ListAsync(cancellationToken);
        while (await cursor.MoveNextAsync(cancellationToken))
        {
            indexes.AddRange(cursor.Current);
        }

        return indexes;
    }

    /// <summary>
    /// Drops an index by name
    /// </summary>
    public async Task DropIndexAsync(
        string collectionName,
        string indexName,
        CancellationToken cancellationToken = default)
    {
        var collection = _database.GetCollection<BsonDocument>(collectionName);
        await collection.Indexes.DropOneAsync(indexName, cancellationToken);
        _logger.LogInformation("Dropped index {Index} from {Collection}", indexName, collectionName);
    }
}


/// <summary>
/// Utility class for MongoDB collection name handling
/// MongoDB collection names have fewer restrictions than LiteDB, but we maintain compatibility
/// </summary>
public static class MongoCollectionNameHelper
{
    /// <summary>
    /// Converts a Guid to a valid MongoDB collection name
    /// MongoDB allows most characters, but we sanitize for consistency
    /// </summary>
    public static string ToValidCollectionName(Guid guid)
    {
        // MongoDB collection names:
        // - Cannot contain null character
        // - Cannot start with "system."
        // - Cannot contain $ (except in special cases)
        // - Maximum 120 bytes (we use 100 for safety)

        return $"Job_{guid:N}";
    }

    /// <summary>
    /// Validates a MongoDB collection name
    /// </summary>
    public static bool IsValidCollectionName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return false;

        if (name.Length > 100)
            return false;

        if (name.Contains('\0'))
            return false;

        if (name.StartsWith("system."))
            return false;

        // Allow alphanumeric, underscore, and hyphen
        return Regex.IsMatch(name, @"^[a-zA-Z0-9_-]+$");
    }

    /// <summary>
    /// Sanitizes a collection name to be MongoDB-compatible
    /// </summary>
    public static string SanitizeCollectionName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Collection name cannot be empty", nameof(name));

        // Replace invalid characters with underscore
        var sanitized = Regex.Replace(name, @"[^a-zA-Z0-9_-]", "_");

        // Remove leading/trailing underscores
        sanitized = sanitized.Trim('_');

        // Ensure it doesn't start with system.
        if (sanitized.StartsWith("system"))
            sanitized = "col_" + sanitized;

        // Truncate if too long
        if (sanitized.Length > 100)
            sanitized = sanitized.Substring(0, 100);

        return sanitized;
    }
}

/// <summary>
/// Extension methods for MongoDB operations
/// </summary>
public static class MongoDbExtensions
{
    /// <summary>
    /// Converts a dictionary to a consistent format for comparison
    /// </summary>
    public static string ToConsistentKey(this IDictionary<string, object> dict)
    {
        if (dict == null) return string.Empty;

        var sortedPairs = dict
            .OrderBy(kvp => kvp.Key)
            .Select(kvp => $"{kvp.Key}:{kvp.Value}");

        return string.Join("|", sortedPairs);
    }
}

/// <summary>
/// Performance metrics collector for MongoDB operations
/// </summary>
public class MongoDbMetrics
{
    private long _insertCount;
    private long _queryCount;
    private long _updateCount;
    private long _deleteCount;
    private long _totalInsertTimeMs;
    private long _totalQueryTimeMs;

    public void RecordInsert(int count, long elapsedMs)
    {
        Interlocked.Add(ref _insertCount, count);
        Interlocked.Add(ref _totalInsertTimeMs, elapsedMs);
    }

    public void RecordQuery(long elapsedMs)
    {
        Interlocked.Increment(ref _queryCount);
        Interlocked.Add(ref _totalQueryTimeMs, elapsedMs);
    }

    public void RecordUpdate()
    {
        Interlocked.Increment(ref _updateCount);
    }

    public void RecordDelete()
    {
        Interlocked.Increment(ref _deleteCount);
    }

    public MongoDbMetricsSnapshot GetSnapshot()
    {
        return new MongoDbMetricsSnapshot
        {
            InsertCount = Interlocked.Read(ref _insertCount),
            QueryCount = Interlocked.Read(ref _queryCount),
            UpdateCount = Interlocked.Read(ref _updateCount),
            DeleteCount = Interlocked.Read(ref _deleteCount),
            AverageInsertTimeMs = _insertCount > 0
                ? (double)_totalInsertTimeMs / _insertCount
                : 0,
            AverageQueryTimeMs = _queryCount > 0
                ? (double)_totalQueryTimeMs / _queryCount
                : 0
        };
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _insertCount, 0);
        Interlocked.Exchange(ref _queryCount, 0);
        Interlocked.Exchange(ref _updateCount, 0);
        Interlocked.Exchange(ref _deleteCount, 0);
        Interlocked.Exchange(ref _totalInsertTimeMs, 0);
        Interlocked.Exchange(ref _totalQueryTimeMs, 0);
    }
}

public class MongoDbMetricsSnapshot
{
    public long InsertCount { get; set; }
    public long QueryCount { get; set; }
    public long UpdateCount { get; set; }
    public long DeleteCount { get; set; }
    public double AverageInsertTimeMs { get; set; }
    public double AverageQueryTimeMs { get; set; }

    public override string ToString()
    {
        return $"Inserts: {InsertCount} (avg {AverageInsertTimeMs:F2}ms), " +
               $"Queries: {QueryCount} (avg {AverageQueryTimeMs:F2}ms), " +
               $"Updates: {UpdateCount}, Deletes: {DeleteCount}";
    }
}






