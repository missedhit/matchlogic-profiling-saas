using MatchLogic.Infrastructure.Persistence.MongoDB;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Unit tests for MongoDbDataStore
/// Run these tests to verify the implementation before integration
/// </summary>
public class MongoDbDataStoreTests : IDisposable
{
    private readonly MongoDbDataStore _store;
    private readonly IMongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly Mock<ILogger<MongoDbDataStore>> _loggerMock;
    private readonly string _testDatabaseName = $"TestDb_{Guid.NewGuid():N}";

    public MongoDbDataStoreTests()
    {
        // Use local MongoDB for testing
        // Make sure MongoDB is running on localhost:27017
        var options = Options.Create(new MongoDbOptions
        {
            ConnectionString = "mongodb://admin:secret@localhost:27017",
            DatabaseName = _testDatabaseName,
            BulkInsertBatchSize = 1000,
            UseUnorderedBulkWrites = true
        });

        _loggerMock = new Mock<ILogger<MongoDbDataStore>>();

        _client = new MongoClient(options.Value.ConnectionString);
        _database = _client.GetDatabase(_testDatabaseName);

        _store = new MongoDbDataStore(options, _loggerMock.Object);
    }

    public void Dispose()
    {
        // Clean up test database
        _client.DropDatabase(_testDatabaseName);
        _store.Dispose();
    }

    #region Initialization Tests

    [Fact]
    public async Task InitializeJobAsync_ShouldReturnNewGuid()
    {
        // Act
        var jobId = await _store.InitializeJobAsync();

        // Assert
        Assert.NotEqual(Guid.Empty, jobId);
    }

    [Fact]
    public async Task InitializeJobAsync_WithCollectionName_ShouldCreateCollection()
    {
        // Arrange
        var collectionName = "TestCollection";

        // Act
        var jobId = await _store.InitializeJobAsync(collectionName);

        // Assert
        var collections = await _database.ListCollectionNamesAsync();
        var collectionList = await collections.ToListAsync();
        Assert.Contains(collectionName, collectionList);
    }

    #endregion

    #region Insert Tests

    [Fact]
    public async Task InsertBatchAsync_ShouldInsertDocuments()
    {
        // Arrange
        var collectionName = "InsertTest";
        var batch = CreateTestBatch(100);

        // Act
        await _store.InsertBatchAsync(collectionName, batch);

        // Assert
        var (data, count) = await _store.GetPagedDataAsync(collectionName, 1, 100);
        Assert.Equal(100, count);
    }

    [Fact]
    public async Task InsertBatchAsync_LargeBatch_ShouldHandleChunking()
    {
        // Arrange
        var collectionName = "LargeBatchTest";
        var batch = CreateTestBatch(5000); // Larger than default batch size

        // Act
        await _store.InsertBatchAsync(collectionName, batch);

        // Assert
        var (data, count) = await _store.GetPagedDataAsync(collectionName, 1, 10);
        Assert.Equal(5000, count);
    }

    [Fact]
    public async Task InsertBatchAsync_WithJobId_ShouldInsertToJobCollection()
    {
        // Arrange
        var jobId = await _store.InitializeJobAsync();
        //var batch = CreateTestBatch(50);

        //// Act
        //await _store.InsertBatchAsync(jobId, batch);

        // Assert
        var (data, count) = await _store.GetPagedJobDataAsync(jobId, 1, 50);
        Assert.Equal(50, count);
    }

    #endregion

    #region Query Tests

    [Fact]
    public async Task GetPagedDataAsync_ShouldReturnCorrectPage()
    {
        // Arrange
        var collectionName = "PagingTest";
        var batch = CreateTestBatch(100);
        await _store.InsertBatchAsync(collectionName, batch);

        // Act
        var (page1, total1) = await _store.GetPagedDataAsync(collectionName, 1, 10);
        var (page2, total2) = await _store.GetPagedDataAsync(collectionName, 2, 10);

        // Assert
        Assert.Equal(100, total1);
        Assert.Equal(10, page1.Count());
        Assert.Equal(10, page2.Count());
    }

    [Fact]
    public async Task GetJobDataAsync_ShouldReturnAllData()
    {
        // Arrange
        var jobId = await _store.InitializeJobAsync();
        var batch = CreateTestBatch(50);
        await _store.InsertBatchAsync(jobId, batch);

        // Act
        var data = await _store.GetJobDataAsync(jobId);

        // Assert
        Assert.Equal(50, data.Count());
    }

    #endregion

    #region Streaming Tests

    [Fact]
    public async Task StreamDataAsync_ShouldYieldAllDocuments()
    {
        // Arrange
        var collectionName = "StreamTest";
        var batch = CreateTestBatch(100);
        await _store.InsertBatchAsync(collectionName, batch);

        // Act
        var count = 0;
        await foreach (var item in _store.StreamDataAsync(collectionName))
        {
            count++;
        }

        // Assert
        Assert.Equal(100, count);
    }

    [Fact]
    public async Task StreamDataAsync_WithCancellation_ShouldStop()
    {
        // Arrange
        var collectionName = "CancelStreamTest";
        var batch = CreateTestBatch(1000);
        await _store.InsertBatchAsync(collectionName, batch);
        var cts = new CancellationTokenSource();

        // Act
        var count = 0;
        await foreach (var item in _store.StreamDataAsync(collectionName, cts.Token))
        {
            count++;
            if (count >= 50)
            {
                cts.Cancel();
            }
        }

        // Assert
        Assert.True(count < 1000);
    }

    #endregion

    #region Entity Operations Tests

    [Fact]
    public async Task InsertAsync_ShouldInsertEntity()
    {
        // Arrange
        var collectionName = "EntityTest";
        var entity = new TestEntity { Id = Guid.NewGuid(), Name = "Test", Value = 42 };

        // Act
        await _store.InsertAsync(entity, collectionName);

        // Assert
        var retrieved = await _store.GetByIdAsync<TestEntity, Guid>(entity.Id, collectionName);
        Assert.NotNull(retrieved);
        Assert.Equal(entity.Name, retrieved.Name);
    }

    [Fact]
    public async Task UpdateAsync_ShouldUpdateEntity()
    {
        // Arrange
        var collectionName = "UpdateTest";
        var entity = new TestEntity { Id = Guid.NewGuid(), Name = "Original", Value = 1 };
        await _store.InsertAsync(entity, collectionName);

        // Act
        entity.Name = "Updated";
        entity.Value = 2;
        await _store.UpdateAsync(entity, collectionName);

        // Assert
        var retrieved = await _store.GetByIdAsync<TestEntity, Guid>(entity.Id, collectionName);
        Assert.Equal("Updated", retrieved.Name);
        Assert.Equal(2, retrieved.Value);
    }

    [Fact]
    public async Task DeleteAsync_ShouldRemoveEntity()
    {
        // Arrange
        var collectionName = "DeleteTest";
        var entity = new TestEntity { Id = Guid.NewGuid(), Name = "ToDelete", Value = 0 };
        await _store.InsertAsync(entity, collectionName);

        // Act
        await _store.DeleteAsync(entity.Id, collectionName);

        // Assert
        var retrieved = await _store.GetByIdAsync<TestEntity, Guid>(entity.Id, collectionName);
        Assert.Null(retrieved);
    }

    [Fact]
    public async Task QueryAsync_ShouldFilterEntities()
    {
        // Arrange
        var collectionName = "QueryTest";
        await _store.InsertAsync(new TestEntity { Id = Guid.NewGuid(), Name = "A", Value = 10 }, collectionName);
        await _store.InsertAsync(new TestEntity { Id = Guid.NewGuid(), Name = "B", Value = 20 }, collectionName);
        await _store.InsertAsync(new TestEntity { Id = Guid.NewGuid(), Name = "C", Value = 30 }, collectionName);

        // Act
        var results = await _store.QueryAsync<TestEntity>(x => x.Value > 15, collectionName);

        // Assert
        Assert.Equal(2, results.Count);
    }

    #endregion

    #region Collection Management Tests

    [Fact]
    public async Task DeleteCollection_ShouldRemoveCollection()
    {
        // Arrange
        var collectionName = "ToDeleteCollection";
        await _store.InsertBatchAsync(collectionName, CreateTestBatch(10));

        // Act
        var result = await _store.DeleteCollection(collectionName);

        // Assert
        Assert.True(result);
        var (_, count) = await _store.GetPagedDataAsync(collectionName, 1, 10);
        Assert.Equal(0, count);
    }

    #endregion

    #region BSON Converter Tests

    [Fact]
    public void ConvertToBsonDocument_ShouldHandleNestedDictionaries()
    {
        // Arrange
        var dict = new Dictionary<string, object>
        {
            ["Name"] = "Test",
            ["Nested"] = new Dictionary<string, object>
            {
                ["Level2"] = "Value"
            }
        };

        // Act
        var bsonDoc = MongoDbBsonConverter.ConvertToBsonDocument(dict);

        // Assert
        Assert.True(bsonDoc.Contains("Nested"));
        Assert.True(bsonDoc["Nested"].AsBsonDocument.Contains("Level2"));
    }

    [Fact]
    public void ConvertToBsonDocument_ShouldHandleArrays()
    {
        // Arrange
        var dict = new Dictionary<string, object>
        {
            ["Items"] = new[] { 1, 2, 3 }
        };

        // Act
        var bsonDoc = MongoDbBsonConverter.ConvertToBsonDocument(dict);

        // Assert
        Assert.True(bsonDoc["Items"].IsBsonArray);
        Assert.Equal(3, bsonDoc["Items"].AsBsonArray.Count);
    }

    [Fact]
    public void ConvertBsonDocumentToDictionary_ShouldRoundTrip()
    {
        // Arrange
        var original = new Dictionary<string, object>
        {
            ["String"] = "test",
            ["Int"] = 42,
            ["Double"] = 3.14,
            ["Bool"] = true,
            ["Guid"] = Guid.NewGuid()
        };

        // Act
        var bsonDoc = MongoDbBsonConverter.ConvertToBsonDocument(original);
        var result = MongoDbBsonConverter.ConvertBsonDocumentToDictionary(bsonDoc);

        // Assert
        Assert.Equal(original["String"], result["String"]);
        Assert.Equal(original["Int"], result["Int"]);
        Assert.Equal(original["Bool"], result["Bool"]);
    }

    #endregion

    #region Helper Methods

    private List<IDictionary<string, object>> CreateTestBatch(int count)
    {
        return Enumerable.Range(1, count)
            .Select(i => new Dictionary<string, object>
            {
                ["Id"] = Guid.NewGuid(),
                ["Index"] = i,
                ["Name"] = $"Record_{i}",
                ["Value"] = i * 10,
                ["CreatedAt"] = DateTime.UtcNow
            } as IDictionary<string, object>)
            .ToList();
    }

    #endregion
}

/// <summary>
/// Test entity for unit tests
/// </summary>
public class TestEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public int Value { get; set; }
}

/// <summary>
/// Performance benchmark tests
/// Run these to validate throughput requirements
/// </summary>
public class MongoDbPerformanceTests : IDisposable
{
    private readonly MongoDbDataStore _store;
    private readonly string _testDatabaseName = $"PerfTest_{Guid.NewGuid():N}";
    private readonly IMongoClient _client;

    public MongoDbPerformanceTests()
    {
        var options = Options.Create(new MongoDbOptions
        {
            ConnectionString = "mongodb://admin:secret@localhost:27017",
            DatabaseName = _testDatabaseName,
            BulkInsertBatchSize = 25000,
            UseUnorderedBulkWrites = true,
            MaxConnectionPoolSize = 100
        });

        var loggerMock = new Mock<ILogger<MongoDbDataStore>>();
        _client = new MongoClient(options.Value.ConnectionString);
        _store = new MongoDbDataStore(options, loggerMock.Object);
    }
    public class StepJobTestEntity
    {
        public Guid Id { get; set; }
        public Guid RunId { get; set; }
        public Dictionary<string, object> Configuration { get; set; } = new();
    }
    [Fact]
    public async Task InsertAsync_EntityWithDictionaryContainingGuid_ShouldWork()
    {
        // Arrange - This mimics your StepJob entity
        var collectionName = "DictGuidTest";
        var entity = new StepJobTestEntity
        {
            Id = Guid.NewGuid(),
            RunId = Guid.NewGuid(),
            Configuration = new Dictionary<string, object>
            {
                ["DataSourceId"] = Guid.NewGuid(),  // <-- THIS IS THE PROBLEM
                ["SomeName"] = "TestValue",
                ["SomeNumber"] = 42
            }
        };

        // Act
        await _store.InsertAsync(entity, collectionName);

        // Assert
        var retrieved = await _store.GetByIdAsync<StepJobTestEntity, Guid>(entity.Id, collectionName);
        Assert.NotNull(retrieved);
        Assert.Equal(entity.Configuration["DataSourceId"], retrieved.Configuration["DataSourceId"]);
    }
    public void Dispose()
    {
        _client.DropDatabase(_testDatabaseName);
        _store.Dispose();
    }

    [Fact(Skip = "Performance test - run manually")]
    public async Task BulkInsert_100K_ShouldCompleteInReasonableTime()
    {
        // Arrange
        var collectionName = "BulkPerfTest";
        var batch = Enumerable.Range(1, 100000)
            .Select(i => new Dictionary<string, object>
            {
                ["Id"] = Guid.NewGuid(),
                ["Index"] = i,
                ["Name"] = $"Record_{i}",
                ["Data"] = new string('X', 100)
            } as IDictionary<string, object>)
            .ToList();

        // Act
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await _store.InsertBatchAsync(collectionName, batch);
        sw.Stop();

        // Assert
        // Should complete in less than 10 seconds for 100K records
        Assert.True(sw.ElapsedMilliseconds < 10000,
            $"Bulk insert took {sw.ElapsedMilliseconds}ms, expected < 10000ms");

        // Verify count
        var (_, count) = await _store.GetPagedDataAsync(collectionName, 1, 1);
        Assert.Equal(100000, count);
    }

    [Fact(Skip = "Performance test - run manually")]
    public async Task BulkInsert_1M_ShouldCompleteInReasonableTime()
    {
        // Arrange
        var collectionName = "MillionRecordTest";
        var batchSize = 100000;
        var totalRecords = 1000000;

        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Act - insert in 10 batches of 100K
        for (int i = 0; i < totalRecords / batchSize; i++)
        {
            var batch = Enumerable.Range(i * batchSize, batchSize)
                .Select(j => new Dictionary<string, object>
                {
                    ["Id"] = Guid.NewGuid(),
                    ["Index"] = j,
                    ["Name"] = $"Record_{j}"
                } as IDictionary<string, object>)
                .ToList();

            await _store.InsertBatchAsync(collectionName, batch);
        }

        sw.Stop();

        // Assert
        // Should complete in less than 60 seconds for 1M records
        Assert.True(sw.ElapsedMilliseconds < 60000,
            $"1M insert took {sw.ElapsedMilliseconds}ms, expected < 60000ms");

        var (_, count) = await _store.GetPagedDataAsync(collectionName, 1, 1);
        Assert.Equal(totalRecords, count);
    }
}