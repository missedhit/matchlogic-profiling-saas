using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class ExactMatchBlockingStrategyTests
{
    private readonly ILogger<ExactMatchBlockingStrategy> _mockLogger;

    public ExactMatchBlockingStrategyTests()
    {
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _mockLogger = loggerFactory.CreateLogger<ExactMatchBlockingStrategy>();
    }

    [Fact]
    public async Task BlockRecordsAsync_GroupsRecordsByBlockingFields()
    {
        // Arrange
        var blockingStrategy = new ExactMatchBlockingStrategy(_mockLogger);
        var records = new[]
        {
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Doe" } },
            new Dictionary<string, object> { { "FirstName", "Jane" }, { "LastName", "Doe" } },
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } }
        }.ToAsyncEnumerable();
        var blockingFields = new[] { "LastName" };

        // Act
        var result = await (await blockingStrategy.BlockRecordsAsync(records, blockingFields)).ToListAsync();

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains(result, group => group.Key == "Doe" && group.Count() == 2);
        Assert.Contains(result, group => group.Key == "Smith" && group.Count() == 1);
    }

    [Fact]
    public async Task BlockRecordsAsync_GeneratesEmptyGroupsIfNoMatchingRecords()
    {
        // Arrange
        var blockingStrategy = new ExactMatchBlockingStrategy(_mockLogger);
        var records = new List<Dictionary<string, object>>().ToAsyncEnumerable();
        var blockingFields = new[] { "LastName" };

        // Act
        var result = await (await blockingStrategy.BlockRecordsAsync(records, blockingFields)).ToListAsync();

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task BlockRecordsAsync_CancellationToken_ThrowsTaskCanceledException()
    {
        // Arrange
        var blockingStrategy = new ExactMatchBlockingStrategy(_mockLogger);
        var records = new[]
        {
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Doe" } }
        }.ToAsyncEnumerable();
        var blockingFields = new[] { "LastName" };
        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await (await blockingStrategy.BlockRecordsAsync(records, blockingFields, cancellationTokenSource.Token)).ToListAsync());
    }

    [Fact]
    public async Task BlockRecordsAsync_ThrowsObjectDisposedException_WhenDisposed()
    {
        // Arrange
        var blockingStrategy = new ExactMatchBlockingStrategy(_mockLogger);
        await blockingStrategy.DisposeAsync();
        var records = new[]
        {
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Doe" } }
        }.ToAsyncEnumerable();
        var blockingFields = new[] { "LastName" };

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await (await blockingStrategy.BlockRecordsAsync(records, blockingFields)).ToListAsync());
    }

    [Fact]
    public async Task DisposeAsync_SetsDisposedFlag()
    {
        // Arrange
        var blockingStrategy = new ExactMatchBlockingStrategy(_mockLogger);

        // Act
        await blockingStrategy.DisposeAsync();

        // Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
           await( await blockingStrategy.BlockRecordsAsync(AsyncEnumerable.Empty<IDictionary<string, object>>(), new[] { "LastName" })).ToListAsync());
    }

    [Fact]
    public void GenerateBlockingKey_CreatesCorrectKey()
    {
        // Arrange
        var blockingStrategy = new ExactMatchBlockingStrategy(_mockLogger);
        var record = new Dictionary<string, object>
        {
            { "FirstName", "John" },
            { "LastName", "Doe" },
            { "Age", 30 }
        };
        var blockingFields = new[] { "FirstName", "LastName", "NonExistentField" };

        // Act (accessing the private method using reflection)
        var method = typeof(ExactMatchBlockingStrategy).GetMethod("GenerateBlockingKey", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var result = (string)method.Invoke(blockingStrategy, new object[] { record, blockingFields });

        // Assert
        Assert.Equal("John|Doe|", result);
    }

    [Fact]
    public async Task BlockRecordsAsync_ShouldGroupUnderEmptyKey_WhenBlockingFieldIsMissing()
    {
        // Arrange: Define records with fields not matching the specified blocking fields
        var records = new[]
        {
        new Dictionary<string, object> { { "FirstName", "Alice" } },
        new Dictionary<string, object> { { "FirstName", "Bob" } }
    }.ToAsyncEnumerable();

        // Use a non-existent field as the blocking field
        var blockingFields = new[] { "NonExistentField" };

        // Initialize the ExactMatchBlockingStrategy with a mocked logger
        var strategy = new ExactMatchBlockingStrategy(_mockLogger);

        // Act: Call BlockRecordsAsync with the missing blocking field
        var result = await (await strategy.BlockRecordsAsync(records, blockingFields)).ToListAsync();

        // Assert: Verify that all records are grouped under a single empty key
        Assert.Single(result);  // Only one group should be created for missing fields
        Assert.Equal("", result.First().Key);  // The key should be an empty string
        Assert.Equal(2, result.First().Count());  // All records should be in this group        
    }
}
