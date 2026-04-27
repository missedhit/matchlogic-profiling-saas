using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence.MongoDB;

/// <summary>
/// MongoDB implementation of IDataStore
/// Optimized for high-throughput scenarios (1M+ records, 6.5M+ pairs)
/// Thread-safe and SAAS-ready
/// </summary>
public partial class MongoDbDataStore : IDataStore
{
    private readonly IMongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly MongoDbOptions _options;
    private readonly ILogger<MongoDbDataStore> _logger;
    private readonly SemaphoreSlim _indexCreationLock = new(1, 1);
    private readonly HashSet<string> _indexedCollections = new();
    private bool _disposed;

    // Common sort fields that should be indexed
    private static readonly HashSet<string> CommonSortFields = new()
    {
        "FinalScore",
        "WeightedScore",
        "CreatedAt",
        "UpdatedAt"
    };

    // Field name constants
    private const string ID_FIELD = "_id";
    private const string ENTITY_ID_FIELD = "Id";
    private const string GROUP_HASH = "GroupHash";
    private const string RECORD1 = "Record1";
    private const string RECORD2 = "Record2";
    private const string RECORDS = "Records";
    private const string FIELD_SCORES = "FieldScores";
    private const string METADATA = "_metadata";

    public MongoDbDataStore(
        IOptions<MongoDbOptions> options,
        ILogger<MongoDbDataStore> logger)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Register conventions
        MongoDbBsonConverter.RegisterConventions();

        // Build client settings
        var clientSettings = BuildClientSettings();

        _client = new MongoClient(clientSettings);
        _database = _client.GetDatabase(_options.DatabaseName);

        _logger.LogInformation(
            "MongoDB DataStore initialized. Database: {Database}, MaxPoolSize: {PoolSize}",
            _options.DatabaseName,
            _options.MaxConnectionPoolSize);
    }

    /// <summary>
    /// Constructor for dependency injection with pre-configured client
    /// Useful for testing and advanced scenarios
    /// </summary>
    public MongoDbDataStore(
        IMongoClient client,
        IMongoDatabase database,
        MongoDbOptions options,
        ILogger<MongoDbDataStore> logger)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        MongoDbBsonConverter.RegisterConventions();
    }

    #region Private Helpers

    private MongoClientSettings BuildClientSettings()
    {
        var settings = MongoClientSettings.FromConnectionString(_options.ConnectionString);

        // Connection pool settings for high throughput
        settings.MaxConnectionPoolSize = _options.MaxConnectionPoolSize;
        settings.MinConnectionPoolSize = _options.MinConnectionPoolSize;
        settings.WaitQueueTimeout = TimeSpan.FromSeconds(_options.WaitQueueTimeoutSeconds);

        // Timeout settings
        settings.ConnectTimeout = TimeSpan.FromSeconds(_options.ConnectTimeoutSeconds);
        settings.SocketTimeout = TimeSpan.FromSeconds(_options.SocketTimeoutSeconds);
        settings.ServerSelectionTimeout = TimeSpan.FromSeconds(_options.ServerSelectionTimeoutSeconds);

        // Write concern
        settings.WriteConcern = _options.WriteConcern switch
        {
            WriteConcernLevel.Unacknowledged => WriteConcern.Unacknowledged,
            WriteConcernLevel.Acknowledged => WriteConcern.Acknowledged,
            WriteConcernLevel.Majority => WriteConcern.WMajority,
            WriteConcernLevel.Journaled => WriteConcern.Acknowledged.With(journal: true),
            _ => WriteConcern.Acknowledged
        };

        // Read preference
        settings.ReadPreference = _options.ReadPreference switch
        {
            ReadPreferenceLevel.Primary => ReadPreference.Primary,
            ReadPreferenceLevel.PrimaryPreferred => ReadPreference.PrimaryPreferred,
            ReadPreferenceLevel.Secondary => ReadPreference.Secondary,
            ReadPreferenceLevel.SecondaryPreferred => ReadPreference.SecondaryPreferred,
            ReadPreferenceLevel.Nearest => ReadPreference.Nearest,
            _ => ReadPreference.Primary
        };

        // Retry writes
        settings.RetryWrites = _options.RetryWrites;
        settings.RetryReads = true;

        // Command logging for debugging
        if (_options.EnableCommandLogging)
        {
            settings.ClusterConfigurator = builder =>
            {
                builder.Subscribe<CommandStartedEvent>(e =>
                    _logger.LogDebug("MongoDB Command: {CommandName} - {Command}",
                        e.CommandName, e.Command.ToJson()));
            };
        }

        return settings;
    }

    private string GetCollectionName(string baseName)
    {
        if (string.IsNullOrEmpty(_options.CollectionPrefix))
            return baseName;

        return $"{_options.CollectionPrefix}{baseName}";
    }

    private string GetJobCollectionName(Guid jobId)
    {
        return GetCollectionName(GuidCollectionNameConverter.ToValidCollectionName(jobId));
    }

    private IMongoCollection<BsonDocument> GetCollection(string collectionName)
    {
        return _database.GetCollection<BsonDocument>(GetCollectionName(collectionName));
    }

    private IMongoCollection<T> GetTypedCollection<T>(string collectionName)
    {
        return _database.GetCollection<T>(GetCollectionName(collectionName));
    }

    private async Task EnsureIndexesAsync(string collectionName, CancellationToken cancellationToken = default)
    {
        if (!_options.AutoCreateIndexes)
            return;

        var fullName = GetCollectionName(collectionName);

        if (_indexedCollections.Contains(fullName))
            return;

        await _indexCreationLock.WaitAsync(cancellationToken);
        try
        {
            if (_indexedCollections.Contains(fullName))
                return;

            var collection = _database.GetCollection<BsonDocument>(fullName);

            // Create index on Id field (our Guid ID)
            var idIndexModel = new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending(ENTITY_ID_FIELD),
                new CreateIndexOptions { Background = true, Name = "idx_entity_id" });

            // Create indexes on common sort fields
            var indexModels = CommonSortFields
                .Select(field => new CreateIndexModel<BsonDocument>(
                    Builders<BsonDocument>.IndexKeys.Descending(field),
                    new CreateIndexOptions { Background = true, Sparse = true, Name = $"idx_{field.ToLower()}" }))
                .ToList();

            indexModels.Add(idIndexModel);

            await collection.Indexes.CreateManyAsync(indexModels, cancellationToken);

            _indexedCollections.Add(fullName);
            _logger.LogDebug("Created indexes for collection: {Collection}", fullName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create indexes for collection: {Collection}", fullName);
        }
        finally
        {
            _indexCreationLock.Release();
        }
    }

    private WriteConcern GetBulkWriteConcern()
    {
        return _options.WriteConcern switch
        {
            WriteConcernLevel.Unacknowledged => WriteConcern.Unacknowledged,
            WriteConcernLevel.Acknowledged => WriteConcern.Acknowledged,
            WriteConcernLevel.Majority => WriteConcern.WMajority.With(wTimeout: TimeSpan.FromSeconds(30)),
            WriteConcernLevel.Journaled => WriteConcern.Acknowledged.With(journal: true),
            _ => WriteConcern.Acknowledged
        };
    }

    #endregion

    #region Job Initialization

    public async Task<Guid> InitializeJobAsync(string collectionName = "")
    {
        try
        {
            var jobId = Guid.NewGuid();
            var targetCollection = string.IsNullOrEmpty(collectionName)
                ? GetJobCollectionName(jobId)
                : GetCollectionName(collectionName);

            // Create collection explicitly (optional but ensures it exists)
            try
            {
                await _database.CreateCollectionAsync(targetCollection);
            }
            catch (MongoCommandException ex) when (ex.CodeName == "NamespaceExists")
            {
                // Collection already exists, that's fine
            }

            // Ensure basic indexes
            await EnsureIndexesAsync(targetCollection);

            _logger.LogInformation("Initialized new import job: {JobId}, Collection: {Collection}",
                jobId, targetCollection);

            return jobId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize new import job");
            throw;
        }
    }

    public async Task<bool> DeleteCollection(string collectionName)
    {
        try
        {
            var fullName = GetCollectionName(collectionName);
            await _database.DropCollectionAsync(fullName);
            _indexedCollections.Remove(fullName);

            _logger.LogInformation("Deleted collection: {Collection}", fullName);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting collection: {Collection}", collectionName);
            throw;
        }
    }

    #endregion

    #region Batch Insert Operations

    public async Task InsertBatchAsync(Guid jobId, IEnumerable<IDictionary<string, object>> batch, string collectionName = "")
    {
        var sw = Stopwatch.StartNew();
        var targetCollectionName = string.IsNullOrEmpty(collectionName)
            ? GetJobCollectionName(jobId)
            : GetCollectionName(collectionName);

        try
        {
            var collection = _database.GetCollection<BsonDocument>(targetCollectionName)
                .WithWriteConcern(GetBulkWriteConcern());

            var bsonDocs = batch.Select(item =>
            {
                var doc = new BsonDocument();
                foreach (var kvp in item)
                {
                    if (kvp.Key != "_id")
                    {
                        doc[kvp.Key] = MongoDbBsonConverter.ConvertToBsonValue(kvp.Value);
                    }
                }
                return doc;
            }).ToList();

            if (bsonDocs.Count == 0)
            {
                _logger.LogWarning("Empty batch provided for job {JobId}", jobId);
                return;
            }

            await InsertBatchInternalAsync(collection, bsonDocs);

            sw.Stop();
            _logger.LogInformation(
                "Inserted batch of {Count} records into job {JobId} in {ElapsedMs}ms ({Rate}/sec)",
                bsonDocs.Count, jobId, sw.ElapsedMilliseconds,
                bsonDocs.Count * 1000 / Math.Max(1, sw.ElapsedMilliseconds));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting batch into job {JobId}", jobId);
            throw;
        }
    }

    public async Task InsertBatchAsync(string collectionName, IEnumerable<IDictionary<string, object>> batch)
    {
        var sw = Stopwatch.StartNew();
        var fullCollectionName = GetCollectionName(collectionName);

        try
        {
            var collection = _database.GetCollection<BsonDocument>(fullCollectionName)
                .WithWriteConcern(GetBulkWriteConcern());

            var bsonDocs = MongoDbBsonConverter.ConvertBatch(batch);

            if (bsonDocs.Count == 0)
            {
                _logger.LogWarning("Empty batch provided for collection {Collection}", collectionName);
                return;
            }

            await InsertBatchInternalAsync(collection, bsonDocs);

            sw.Stop();
            _logger.LogInformation(
                "Inserted batch of {Count} records into {Collection} in {ElapsedMs}ms ({Rate}/sec)",
                bsonDocs.Count, collectionName, sw.ElapsedMilliseconds,
                bsonDocs.Count * 1000 / Math.Max(1, sw.ElapsedMilliseconds));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting batch into {Collection}", collectionName);
            throw;
        }
    }

    /// <summary>
    /// Internal batch insert with optimized chunking and parallel writes
    /// </summary>
    private async Task InsertBatchInternalAsync(
        IMongoCollection<BsonDocument> collection,
        List<BsonDocument> documents)
    {
        var batchSize = _options.BulkInsertBatchSize;
        var totalDocs = documents.Count;

        if (totalDocs <= batchSize)
        {
            // Single batch - direct insert
            var insertOptions = new InsertManyOptions
            {
                IsOrdered = !_options.UseUnorderedBulkWrites,
                BypassDocumentValidation = true
            };

            await collection.InsertManyAsync(documents, insertOptions);
            return;
        }

        // Multiple batches - process in parallel chunks
        var batches = new List<List<BsonDocument>>();
        for (int i = 0; i < totalDocs; i += batchSize)
        {
            batches.Add(documents.Skip(i).Take(batchSize).ToList());
        }

        var insertOptions2 = new InsertManyOptions
        {
            IsOrdered = !_options.UseUnorderedBulkWrites,
            BypassDocumentValidation = true
        };

        // Process batches with controlled parallelism
        var semaphore = new SemaphoreSlim(Math.Min(4, Environment.ProcessorCount));
        var tasks = batches.Select(async batch =>
        {
            await semaphore.WaitAsync();
            try
            {
                await collection.InsertManyAsync(batch, insertOptions2);
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    public async Task InsertProbabilisticBatchAsync(string collectionName, IEnumerable<MatchResult> batch)
    {
        var sw = Stopwatch.StartNew();
        var fullCollectionName = GetCollectionName(collectionName);

        try
        {
            var collection = _database.GetCollection<BsonDocument>(fullCollectionName)
                .WithWriteConcern(GetBulkWriteConcern());

            var bsonDocs = batch.Select(MongoDbBsonConverter.SerializeToDocument).ToList();

            if (bsonDocs.Count == 0)
            {
                _logger.LogWarning("Empty probabilistic batch provided for {Collection}", collectionName);
                return;
            }

            await InsertBatchInternalAsync(collection, bsonDocs);

            sw.Stop();
            _logger.LogInformation(
                "Inserted probabilistic batch of {Count} records into {Collection} in {ElapsedMs}ms",
                bsonDocs.Count, collectionName, sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting probabilistic batch into {Collection}", collectionName);
            throw;
        }
    }

    #endregion

    #region Query Operations

    public async Task<IEnumerable<IDictionary<string, object>>> GetJobDataAsync(Guid jobId)
    {
        try
        {
            var collection = _database.GetCollection<BsonDocument>(GetJobCollectionName(jobId));

            var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty)
                .ToListAsync();

            var result = documents.Select(MongoDbBsonConverter.ConvertBsonDocumentToDictionary).ToList();

            _logger.LogInformation("Retrieved {Count} records from job {JobId}", result.Count, jobId);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving data for job {JobId}", jobId);
            throw;
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobDataAsync(
        Guid jobId, int pageNumber, int pageSize)
    {
        try
        {
            var collection = _database.GetCollection<BsonDocument>(GetJobCollectionName(jobId));

            var totalCount = (int)await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);

            var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty)
                .Skip((pageNumber - 1) * pageSize)
                .Limit(pageSize)
                .ToListAsync();

            var data = documents.Select(MongoDbBsonConverter.ConvertBsonDocumentToDictionary).ToList();

            _logger.LogInformation("Fetched page {Page} with {Count} records from job {JobId}",
                pageNumber, data.Count, jobId);

            return (data, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching paged data for job {JobId}", jobId);
            throw;
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedDataAsync(
        string collectionName, int pageNumber, int pageSize)
    {
        try
        {
            var collection = GetCollection(collectionName);

            var totalCount = (int)await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);

            var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty)
                .Skip((pageNumber - 1) * pageSize)
                .Limit(pageSize)
                .ToListAsync();

            var data = documents.Select(MongoDbBsonConverter.ConvertBsonDocumentToDictionary).ToList();

            _logger.LogInformation("Fetched page {Page} with {Count} records from {Collection}",
                pageNumber, data.Count, collectionName);

            return (data, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching paged data from {Collection}", collectionName);
            throw;
        }
    }

    #endregion

    #region Streaming Operations - FIXED: No yield in try-catch

    public async IAsyncEnumerable<IDictionary<string, object>> StreamDataAsync(
        string collectionName,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncCursor<BsonDocument> cursor = null;

        try
        {
            var collection = GetCollection(collectionName);
            cursor = await collection.Find(FilterDefinition<BsonDocument>.Empty)
                .ToCursorAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error initializing stream for {Collection}", collectionName);
            throw;
        }

        // Yield outside try-catch
        using (cursor)
        {
            while (await cursor.MoveNextAsync(cancellationToken))
            {
                foreach (var document in cursor.Current)
                {
                    if (cancellationToken.IsCancellationRequested)
                        yield break;

                    yield return MongoDbBsonConverter.ConvertBsonDocumentToDictionary(document);
                }
            }
        }
    }

    public async IAsyncEnumerable<IDictionary<string, object>> StreamJobDataAsync(
        Guid jobId,
        IStepProgressTracker progressTracker,
        string collectionName = "",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var targetCollection = string.IsNullOrEmpty(collectionName)
            ? GetJobCollectionName(jobId)
            : GetCollectionName(collectionName);

        var collection = _database.GetCollection<BsonDocument>(targetCollection);
        IAsyncCursor<BsonDocument> cursor = null;
        long count = 0;

        try
        {
            // Get count for progress tracking
            count = await collection.CountDocumentsAsync(
                FilterDefinition<BsonDocument>.Empty,
                cancellationToken: cancellationToken);

            await progressTracker.StartStepAsync((int)count, cancellationToken);

            cursor = await collection.Find(FilterDefinition<BsonDocument>.Empty)
                .ToCursorAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error initializing stream for job {JobId}", jobId);
            throw;
        }

        // Yield outside try-catch
        using (cursor)
        {
            while (await cursor.MoveNextAsync(cancellationToken))
            {
                foreach (var document in cursor.Current)
                {
                    if (cancellationToken.IsCancellationRequested)
                        yield break;

                    yield return MongoDbBsonConverter.ConvertBsonDocumentToDictionary(document);
                }
            }
        }

        // Complete progress after streaming
        try
        {
            await progressTracker.CompleteStepAsync(null);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error completing progress for job {JobId}", jobId);
        }
    }

    public async IAsyncEnumerable<IDictionary<string, object>> GetStreamFromTempCollection(
        string tempCollectionName,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IAsyncCursor<BsonDocument> cursor = null;

        try
        {
            var collection = GetCollection(tempCollectionName);
            cursor = await collection.Find(FilterDefinition<BsonDocument>.Empty)
                .ToCursorAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error initializing stream for temp collection {Collection}", tempCollectionName);
            throw;
        }

        // Yield outside try-catch
        using (cursor)
        {
            while (await cursor.MoveNextAsync(cancellationToken))
            {
                foreach (var document in cursor.Current)
                {
                    if (cancellationToken.IsCancellationRequested)
                        yield break;

                    yield return MongoDbBsonConverter.ConvertBsonDocumentToDictionary(document);
                }
            }
        }
    }

    #endregion

    #region Typed Entity Operations

    public async Task<T> GetByIdAsync<T, TKey>(TKey id, string collectionName)
    {
        try
        {
            var collection = GetTypedCollection<T>(collectionName);

            FilterDefinition<T> filter;
            if (id is Guid guidId)
            {
                filter = Builders<T>.Filter.Eq(ENTITY_ID_FIELD, guidId);
            }
            else
            {
                filter = Builders<T>.Filter.Eq(ENTITY_ID_FIELD, id);
            }

            return await collection.Find(filter).FirstOrDefaultAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting entity by ID {Id} from {Collection}", id, collectionName);
            throw;
        }
    }

    public async Task<List<T>> GetAllAsync<T>(string collectionName)
    {
        try
        {
            var collection = GetTypedCollection<T>(collectionName);
            return await collection.Find(FilterDefinition<T>.Empty).ToListAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting all entities from {Collection}", collectionName);
            throw;
        }
    }

    public async Task<List<T>> QueryAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        try
        {
            var collection = GetTypedCollection<T>(collectionName);
            return await collection.Find(predicate).ToListAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error querying entities from {Collection}", collectionName);
            throw;
        }
    }

    public async Task InsertAsync<T>(T entity, string collectionName)
    {
        try
        {
            var collection = GetTypedCollection<T>(collectionName);
            await collection.InsertOneAsync(entity);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting entity into {Collection}", collectionName);
            throw;
        }
    }

    public async Task BulkInsertAsync<T>(IEnumerable<T> entities, string collectionName)
    {
        try
        {
            var collection = GetTypedCollection<T>(collectionName);
            var entityList = entities.ToList();

            if (entityList.Count == 0)
                return;

            var options = new InsertManyOptions
            {
                IsOrdered = !_options.UseUnorderedBulkWrites,
                BypassDocumentValidation = true
            };

            await collection.InsertManyAsync(entityList, options);

            _logger.LogInformation("Bulk inserted {Count} entities into {Collection}",
                entityList.Count, collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error bulk inserting entities into {Collection}", collectionName);
            throw;
        }
    }

    public async Task UpdateAsync<T>(T entity, string collectionName)
    {
        try
        {
            var collection = GetTypedCollection<T>(collectionName);

            // Get the Id property value
            var idProperty = typeof(T).GetProperty("Id");
            if (idProperty == null)
                throw new InvalidOperationException("Entity must have an Id property");

            var idValue = idProperty.GetValue(entity);
            var filter = Builders<T>.Filter.Eq(ENTITY_ID_FIELD, idValue);

            await collection.ReplaceOneAsync(filter, entity, new ReplaceOptions { IsUpsert = false });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating entity in {Collection}", collectionName);
            throw;
        }
    }

    public async Task DeleteAsync<TKey>(TKey id, string collectionName)
    {
        try
        {
            var collection = _database.GetCollection<BsonDocument>(GetCollectionName(collectionName));

            FilterDefinition<BsonDocument> filter;
            if (id is Guid guidId)
            {
                filter = Builders<BsonDocument>.Filter.Eq("_id", guidId);
            }
            else
            {
                filter = Builders<BsonDocument>.Filter.Eq("_id", id);
            }

            await collection.DeleteOneAsync(filter);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting entity {Id} from {Collection}", id, collectionName);
            throw;
        }
    }

    public async Task<int> DeleteAllAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        try
        {
            var collection = GetTypedCollection<T>(collectionName);
            var result = await collection.DeleteManyAsync(predicate);
            return (int)result.DeletedCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting entities from {Collection}", collectionName);
            throw;
        }
    }

    public async Task UpdateAsync(IDictionary<string, object> entity, string collectionName)
    {
        var collection = GetCollection(collectionName);

        if (!entity.TryGetValue("_id", out var idValue))
            throw new InvalidOperationException("_id is required for update");

        var bson = MongoDbBsonConverter.ConvertToBsonDocument(entity);
        var filter = Builders<BsonDocument>.Filter.Eq("_id", BsonValue.Create(idValue));

        var result = await collection.ReplaceOneAsync(filter, bson, new ReplaceOptions { IsUpsert = false });

        if (result.MatchedCount == 0)
            throw new InvalidOperationException("Update failed: document not found");
    }

    public async Task BulkUpdateAsync(IEnumerable<IDictionary<string, object>> entities, string collectionName)
    {
        var collection = GetCollection(collectionName);
        var writes = new List<WriteModel<BsonDocument>>();

        foreach (var entity in entities)
        {
            if (!entity.TryGetValue("_id", out var id))
                continue;

            var doc = MongoDbBsonConverter.ConvertToBsonDocument(entity);
            var filter = Builders<BsonDocument>.Filter.Eq("_id", BsonValue.Create(id));

            writes.Add(new ReplaceOneModel<BsonDocument>(filter, doc));
        }

        if (writes.Count == 0) return;

        await collection.BulkWriteAsync(writes, new BulkWriteOptions
        {
            IsOrdered = false
        });
    }


    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)>
        GetPagedWithSmartFilteringAndProjectionAsync(
        string collectionName,
        int pageNumber,
        int pageSize,
        string filterText = null,
        string sortColumn = null,
        bool ascending = true,
        string filters = "")
    {
        var collection = _database.GetCollection<BsonDocument>(collectionName);

        var pipeline = new List<BsonDocument>();

        /* -------------------------------------------------
         * 1. TEXT SEARCH (filterText)
         * ------------------------------------------------- */
        if (!string.IsNullOrWhiteSpace(filterText))
        {
            var orConditions = new BsonArray();

            // Top-level fields
            orConditions.Add(new BsonDocument("GroupHash",
                new BsonDocument("$regex", filterText).Add("$options", "i")));

            // Record1 / Record2 scalar fields
            foreach (var record in new[] { "Record1", "Record2" })
            {
                orConditions.Add(new BsonDocument(record,
                    new BsonDocument("$elemMatch",
                        new BsonDocument("$regex", filterText).Add("$options", "i"))));
            }

            // Records[*].*
            orConditions.Add(new BsonDocument("Records",
                new BsonDocument("$elemMatch",
                    new BsonDocument("$regex", filterText).Add("$options", "i"))));

            // Metadata.RowNumber (numeric)
            if (int.TryParse(filterText, out var rowNumber))
            {
                orConditions.Add(new BsonDocument("Metadata.RowNumber", rowNumber));
                orConditions.Add(new BsonDocument("Record1.Metadata.RowNumber", rowNumber));
                orConditions.Add(new BsonDocument("Record2.Metadata.RowNumber", rowNumber));
                orConditions.Add(new BsonDocument("Records.Metadata.RowNumber", rowNumber));
            }

            pipeline.Add(new BsonDocument("$match",
                new BsonDocument("$or", orConditions)));
        }

        /* -------------------------------------------------
         * 2. SMART FILTERS (filters)
         * ------------------------------------------------- */
        var arrayProjections = new Dictionary<string, BsonDocument>();

        if (!string.IsNullOrWhiteSpace(filters))
        {
            var andConditions = new BsonArray();

            foreach (var pair in filters.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                ParseFilter(pair, out var path, out var op, out var value);

                if (path.Contains("[*]") || path.Contains("Records"))
                {
                    // Array condition → $elemMatch
                    var (arrayField, innerField) = SplitArrayPath(path);

                    var condition = BuildMongoCondition(innerField, op, value);

                    andConditions.Add(new BsonDocument(arrayField,
                        new BsonDocument("$elemMatch", condition)));

                    // Projection: filter array items
                    arrayProjections[arrayField] =
                        new BsonDocument("$filter", new BsonDocument
                        {
                        { "input", $"${arrayField}" },
                        { "as", "item" },
                        { "cond", BuildMongoCondition($"$$item.{innerField}", op, value) }
                        });
                }
                else
                {
                    // Scalar condition
                    andConditions.Add(BuildMongoCondition(path, op, value));
                }
            }

            if (andConditions.Count > 0)
            {
                pipeline.Add(new BsonDocument("$match",
                    new BsonDocument("$and", andConditions)));
            }
        }

        /* -------------------------------------------------
         * 3. PROJECTION (array-filtered)
         * ------------------------------------------------- */
        if (arrayProjections.Any())
        {
            var projection = new BsonDocument
        {
            { "_id", 1 },
            { "GroupId", 1 },
            { "GroupHash", 1 },
            { "Metadata", 1 }
        };

            foreach (var kvp in arrayProjections)
                projection[kvp.Key] = kvp.Value;

            pipeline.Add(new BsonDocument("$project", projection));
        }

        /* -------------------------------------------------
         * 4. SORTING
         * ------------------------------------------------- */
        if (!string.IsNullOrWhiteSpace(sortColumn))
        {
            pipeline.Add(new BsonDocument("$sort",
                new BsonDocument(sortColumn, ascending ? 1 : -1)));
        }

        /* -------------------------------------------------
         * 5. TOTAL COUNT (before paging)
         * ------------------------------------------------- */
        var countPipeline = pipeline
            .Concat(new[] { new BsonDocument("$count", "count") })
            .ToList();

        var countCursor = await collection.AggregateAsync<BsonDocument>(countPipeline);

        var countDoc = await countCursor.FirstOrDefaultAsync();
        var totalCount = countDoc != null && countDoc.Contains("count")
            ? countDoc["count"].AsInt32
            : 0;

        /* -------------------------------------------------
         * 6. PAGINATION
         * ------------------------------------------------- */
        pipeline.Add(new BsonDocument("$skip", (pageNumber - 1) * pageSize));
        pipeline.Add(new BsonDocument("$limit", pageSize));

        var docs = await collection.Aggregate<BsonDocument>(pipeline).ToListAsync();
        var data = docs.Select(MongoDbBsonConverter.ConvertBsonDocumentToDictionary);

        return (data, totalCount);
    }

    private static void ParseFilter(string pair, out string key, out string op, out string value)
    {
        if (pair.Contains(">=")) { var p = pair.Split(">="); key = p[0]; op = ">="; value = p[1]; }
        else if (pair.Contains("<=")) { var p = pair.Split("<="); key = p[0]; op = "<="; value = p[1]; }
        else if (pair.Contains(">")) { var p = pair.Split(">"); key = p[0]; op = ">"; value = p[1]; }
        else if (pair.Contains("<")) { var p = pair.Split("<"); key = p[0]; op = "<"; value = p[1]; }
        else if (pair.Contains("~")) { var p = pair.Split("~"); key = p[0]; op = "contains"; value = p[1]; }
        else { var p = pair.Split("="); key = p[0]; op = "="; value = p[1]; }

        key = key.Trim();
        value = value.Trim();
    }

    private static (string arrayField, string innerField) SplitArrayPath(string path)
    {
        var parts = path.Replace("$.", "").Split('.');
        return (parts[0], parts.Last());
    }

    private static BsonDocument BuildMongoCondition(string field, string op, string value)
    {
        return op switch
        {
            "=" => new BsonDocument(field, ParseValue(value)),
            "contains" => new BsonDocument(field,
                new BsonDocument("$regex", value).Add("$options", "i")),
            ">" => new BsonDocument(field, new BsonDocument("$gt", ParseValue(value))),
            "<" => new BsonDocument(field, new BsonDocument("$lt", ParseValue(value))),
            ">=" => new BsonDocument(field, new BsonDocument("$gte", ParseValue(value))),
            "<=" => new BsonDocument(field, new BsonDocument("$lte", ParseValue(value))),
            _ => throw new NotSupportedException($"Operator {op}")
        };
    }

    private static BsonValue ParseValue(string value)
    {
        if (int.TryParse(value, out var i)) return i;
        if (double.TryParse(value, out var d)) return d;
        return value;
    }


    public async Task<bool> UpdateByFieldAsync<TField>(
    IDictionary<string, object> data,
    string collectionName,
    string fieldName,
    TField fieldValue)
    {
        var collection = GetCollection(collectionName);
        var filter = Builders<BsonDocument>.Filter.Eq(fieldName, BsonValue.Create(fieldValue));
        var doc = MongoDbBsonConverter.ConvertToBsonDocument(data);

        var result = await collection.ReplaceOneAsync(filter, doc);
        return result.ModifiedCount == 1;
    }

    public async Task<bool> RenameCollection(string oldName, string newName)
    {
        try
        {
            await _database.RenameCollectionAsync(
                GetCollectionName(oldName),
                GetCollectionName(newName));
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> CollectionExistsAsync(string collectionName)
    {
        var filter = new BsonDocument("name", GetCollectionName(collectionName));
        var collections = await _database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
        return await collections.AnyAsync();
    }
    #endregion

    #region Bulk Upsert Methods

    /// <summary>
    /// Bulk upsert entities by Id field
    /// Uses UpdateOne with $set to avoid _id immutability issues
    /// </summary>
    public async Task BulkUpsertAsync<T>(IEnumerable<T> entities, string collectionName)
    {
        int BatchSize = _options.BulkInsertBatchSize;
        try
        {
            var collection = _database.GetCollection<T>(collectionName);
            var entityList = entities.ToList();

            if (!entityList.Any())
            {
                _logger.LogWarning("No entities to upsert in {CollectionName}", collectionName);
                return;
            }

            int totalUpserted = 0;
            int totalInserted = 0;
            int totalModified = 0;

            for (int i = 0; i < entityList.Count; i += BatchSize)
            {
                var batch = entityList.Skip(i).Take(BatchSize).ToList();
                var bulkOps = new List<WriteModel<T>>();

                foreach (var entity in batch)
                {
                    var id = GetEntityId(entity);

                    // If no Id, generate one
                    if (id == null || id.Equals(Guid.Empty))
                    {
                        id = Guid.NewGuid();
                        SetEntityId(entity, id);
                    }

                    // Build filter on _id
                    var filter = Builders<T>.Filter.Eq("_id", id);

                    // Build update definition using existing converter
                    var update = BuildUpdateDefinitionUsingConverter(entity);

                    var updateOne = new UpdateOneModel<T>(filter, update)
                    {
                        IsUpsert = true
                    };

                    bulkOps.Add(updateOne);
                }

                var result = await collection.BulkWriteAsync(bulkOps,
                    new BulkWriteOptions { IsOrdered = false });

                totalUpserted += bulkOps.Count;
                totalInserted += result.Upserts.Count;
                totalModified += (int)result.ModifiedCount;

                _logger.LogDebug(
                    "Bulk upserted batch {BatchNumber}: Upserted={Upserted}, Modified={Modified}",
                    i / BatchSize + 1,
                    result.Upserts.Count,
                    result.ModifiedCount);
            }

            _logger.LogInformation(
                "Completed bulk upsert of {Total} records into {CollectionName}: {Inserted} inserted, {Modified} modified",
                totalUpserted, collectionName, totalInserted, totalModified);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during bulk upsert in {CollectionName}", collectionName);
            throw;
        }
    }

    /// <summary>
    /// Bulk upsert with custom match criteria (e.g., DataSourceId + FieldName)
    /// RECOMMENDED APPROACH - Uses UpdateOne with $set operations
    /// </summary>
    public async Task BulkUpsertByFieldsAsync<T>(
        IEnumerable<T> entities,
        string collectionName,
        Expression<Func<T, object>>[] matchFields)
    {
        int BatchSize = _options.BulkInsertBatchSize;
        try
        {
            var collection = _database.GetCollection<T>(collectionName);
            var entityList = entities.ToList();

            if (!entityList.Any())
            {
                _logger.LogWarning("No entities to upsert in {CollectionName}", collectionName);
                return;
            }

            // Create compound index on match fields for better performance
            await CreateCompoundIndexAsync(collection, matchFields);

            int totalUpserted = 0;
            int totalInserted = 0;
            int totalModified = 0;

            for (int i = 0; i < entityList.Count; i += BatchSize)
            {
                var batch = entityList.Skip(i).Take(BatchSize).ToList();
                var bulkOps = new List<WriteModel<T>>();

                foreach (var entity in batch)
                {
                    // Ensure entity has an Id
                    var id = GetEntityId(entity);
                    if (id == null || id.Equals(Guid.Empty))
                    {
                        id = Guid.NewGuid();
                        SetEntityId(entity, id);
                    }

                    // Build filter from match fields
                    var filter = BuildFilterFromMatchFields(entity, matchFields);

                    // Build update definition with SetOnInsert for Id
                    var update = BuildUpdateDefinitionWithSetOnInsertUsingConverter(entity);

                    var updateOne = new UpdateOneModel<T>(filter, update)
                    {
                        IsUpsert = true
                    };

                    bulkOps.Add(updateOne);
                }

                var result = await collection.BulkWriteAsync(bulkOps,
                    new BulkWriteOptions { IsOrdered = false });

                totalUpserted += bulkOps.Count;
                totalInserted += result.Upserts.Count;
                totalModified += (int)result.ModifiedCount;

                _logger.LogDebug(
                    "Bulk upserted batch {BatchNumber}: Upserted={Upserted}, Modified={Modified}",
                    i / BatchSize + 1,
                    result.Upserts.Count,
                    result.ModifiedCount);
            }

            _logger.LogInformation(
                "Completed bulk upsert by fields of {Total} records into {CollectionName}: {Inserted} inserted, {Modified} modified",
                totalUpserted, collectionName, totalInserted, totalModified);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during bulk upsert by fields in {CollectionName}", collectionName);
            throw;
        }
    }


    #endregion

    #region Helper Methods

    /// <summary>
    /// Build update definition using existing MongoDbBsonConverter
    /// This ensures consistency with insert operations
    /// </summary>
    private UpdateDefinition<T> BuildUpdateDefinitionUsingConverter<T>(T entity)
    {
        var updateBuilder = Builders<T>.Update;
        var updates = new List<UpdateDefinition<T>>();

        // Get all properties except Id
        var properties = typeof(T).GetProperties()
            .Where(p => !IsIdProperty(p.Name) && p.CanRead);

        foreach (var prop in properties)
        {
            try
            {
                var value = prop.GetValue(entity);
                
                var bsonValue = MongoDbBsonConverter.ConvertToBsonValue(value);

                updates.Add(updateBuilder.Set(prop.Name, bsonValue));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to set property {PropertyName} during update", prop.Name);
                // Continue with other properties
            }
        }

        if (updates.Count == 0)
        {
            // Fallback: at least set _id if no other properties
            var entityId = GetEntityId(entity);
            return updateBuilder.Set("_id", new BsonBinaryData((Guid)entityId, GuidRepresentation.Standard));
        }

        return updateBuilder.Combine(updates);
    }

    // <summary>
    /// Build update definition with SetOnInsert for _id
    /// Uses existing MongoDbBsonConverter for type conversion
    /// </summary>
    private UpdateDefinition<T> BuildUpdateDefinitionWithSetOnInsertUsingConverter<T>(T entity)
    {
        var updateBuilder = Builders<T>.Update;
        var updates = new List<UpdateDefinition<T>>();

        // Get all properties except Id
        var properties = typeof(T).GetProperties()
            .Where(p => !IsIdProperty(p.Name) && p.CanRead);

        foreach (var prop in properties)
        {
            try
            {
                var value = prop.GetValue(entity);
                
                var bsonValue = MongoDbBsonConverter.ConvertToBsonValue(value);

                updates.Add(updateBuilder.Set(prop.Name, bsonValue));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to set property {PropertyName} during update", prop.Name);
                // Continue with other properties
            }
        }

        // Add SetOnInsert for _id
        var entityId = GetEntityId(entity);
        updates.Add(updateBuilder.SetOnInsert("_id",
            new BsonBinaryData((Guid)entityId, GuidRepresentation.Standard)));

        if (updates.Count == 1) // Only SetOnInsert
        {
            return updates[0];
        }

        // Combine all update operations
        return updateBuilder.Combine(updates);
    }

    /// <summary>
    /// Build filter from match fields
    /// </summary>
    private FilterDefinition<T> BuildFilterFromMatchFields<T>(
        T entity,
        Expression<Func<T, object>>[] matchFields)
    {
        var filterBuilder = Builders<T>.Filter;
        var filters = new List<FilterDefinition<T>>();

        foreach (var field in matchFields)
        {
            var value = field.Compile()(entity);

            // Convert value using the same converter for consistency
            if (value is Guid guidValue)
            {
                var fieldName = GetFieldName(field);
                // Use BsonBinaryData for Guid comparison
                filters.Add(filterBuilder.Eq(fieldName,
                    new BsonBinaryData(guidValue, GuidRepresentation.Standard)));
            }
            else
            {
                filters.Add(filterBuilder.Eq(field, value));
            }
        }

        return filters.Count == 1
            ? filters[0]
            : filterBuilder.And(filters);
    }


    /// <summary>
    /// Create compound index on match fields
    /// </summary>
    private async Task CreateCompoundIndexAsync<T>(
        IMongoCollection<T> collection,
        Expression<Func<T, object>>[] matchFields)
    {
        if (matchFields == null || matchFields.Length == 0)
            return;

        try
        {
            var indexKeys = Builders<T>.IndexKeys;
            var indexParts = new List<IndexKeysDefinition<T>>();

            foreach (var field in matchFields)
            {
                indexParts.Add(indexKeys.Ascending(field));
            }

            var compoundIndex = indexKeys.Combine(indexParts);

            var indexOptions = new CreateIndexOptions
            {
                Background = true,
                Name = $"idx_upsert_{string.Join("_", matchFields.Select(GetFieldName))}"
            };

            await collection.Indexes.CreateOneAsync(
                new CreateIndexModel<T>(compoundIndex, indexOptions));

            _logger.LogInformation("Created compound index on {Fields}",
                string.Join(", ", matchFields.Select(GetFieldName)));
        }
        catch (MongoCommandException ex) when (ex.Code == 85 || ex.Code == 86)
        {
            // Index already exists or index options conflict - ignore
            _logger.LogDebug("Index already exists, continuing");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create compound index, continuing without it");
        }
    }


    /// <summary>
    /// Get field name from expression
    /// </summary>
    private string GetFieldName<T>(Expression<Func<T, object>> expression)
    {
        if (expression.Body is MemberExpression memberExpression)
        {
            return memberExpression.Member.Name;
        }

        if (expression.Body is UnaryExpression unaryExpression &&
            unaryExpression.Operand is MemberExpression operand)
        {
            return operand.Member.Name;
        }

        return "Unknown";
    }

    /// <summary>
    /// Check if property name is an Id property
    /// </summary>
    private bool IsIdProperty(string propertyName)
    {
        return propertyName == "Id" ||
               propertyName == "ID" ||
               propertyName == "id" ||
               propertyName == "_id";
    }

    /// <summary>
    /// Get entity ID value
    /// </summary>
    private static object GetEntityId<T>(T entity)
    {
        var idProperty = typeof(T).GetProperty("Id") ??
                        typeof(T).GetProperty("ID") ??
                        typeof(T).GetProperty("id");

        if (idProperty == null)
        {
            return null;
        }

        return idProperty.GetValue(entity);
    }

    /// <summary>
    /// Set entity ID value
    /// </summary>
    private static void SetEntityId<T>(T entity, object id)
    {
        var idProperty = typeof(T).GetProperty("Id") ??
                        typeof(T).GetProperty("ID") ??
                        typeof(T).GetProperty("id");

        if (idProperty != null && idProperty.CanWrite)
        {
            idProperty.SetValue(entity, id);
        }
    }

    #endregion

    #region IDisposable

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _indexCreationLock?.Dispose();

        _logger.LogInformation("Disposed MongoDB DataStore");
    }    

    #endregion
}