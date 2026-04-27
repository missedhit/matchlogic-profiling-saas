using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class ParallelBlockingStrategyTests
{
    private readonly Mock<ILogger<ParallelBlockingStrategy>> _mockLogger;
    private readonly Mock<ITelemetry> _mockTelemetry;
    private readonly ParallelBlockingStrategy _strategy;
    private readonly RecordLinkageOptions _options;

    public ParallelBlockingStrategyTests()
    {
        _mockLogger = new Mock<ILogger<ParallelBlockingStrategy>>();

        _mockTelemetry = new Mock<ITelemetry>();
        _options = new RecordLinkageOptions
        {
            BatchSize = 2,
            MaxDegreeOfParallelism = 2,
            BufferSize = 5
        };

        _strategy = new ParallelBlockingStrategy(_mockLogger.Object, Options.Create(_options), _mockTelemetry.Object);
    }

    [Fact]
    public async Task BlockRecordsAsync_ShouldGroupRecordsByBlockingFields()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Doe" } },
            new Dictionary<string, object> { { "FirstName", "Jane" }, { "LastName", "Doe" } },
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } }
        }.ToAsyncEnumerable();

        var blockingFields = new[] { "LastName" };

        // Act
        var result = await (await _strategy.BlockRecordsAsync(records, blockingFields)).ToListAsync();

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains(result, group => group.Key == "Doe" && group.Count() == 2);
        Assert.Contains(result, group => group.Key == "Smith" && group.Count() == 1);
    }

    //[Fact]
    //public async Task BlockRecordsAsync_ShouldLogError_WhenRecordFailsToSendToPipeline()
    //{
    //    // Arrange
    //    var records = new[]
    //    {
    //    new Dictionary<string, object> { { "FirstName", "John" } }
    //}.ToAsyncEnumerable();  // Provide at least one record to be processed

    //    var blockingFields = new[] { "NonExistentField" }; // Use a field that doesn't exist in the records to force a failure

    //    _mockLogger.Setup(logger => logger.Log(
    //        LogLevel.Error,
    //        It.IsAny<EventId>(),
    //        It.IsAny<It.IsAnyType>(),
    //        It.IsAny<Exception>(),
    //        It.IsAny<Func<It.IsAnyType, Exception, string>>()));

    //    // Act & Assert
    //    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
    //        await (await _strategy.BlockRecordsAsync(records, blockingFields)).ToListAsync());

    //    _mockLogger.Verify(logger => logger.Log(
    //        LogLevel.Error,
    //        It.IsAny<EventId>(),
    //        It.IsAny<It.IsAnyType>(),
    //        It.IsAny<Exception>(),
    //        It.IsAny<Func<It.IsAnyType, Exception, string>>()),
    //        Times.AtLeastOnce);
    //}

    [Fact]
    public async Task BlockRecordsAsync_ShouldGroupUnderEmptyKey_WhenBlockingFieldIsMissing()
    {
        // Arrange
        var records = new[]
        {
        new Dictionary<string, object> { { "FirstName", "John" } }
    }.ToAsyncEnumerable();  // Provide at least one record to be processed

        var blockingFields = new[] { "NonExistentField" }; // Use a field that doesn't exist in the records to force a failure

        _mockLogger.Setup(logger => logger.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception, string>>()));

        // Act & Assert
       var result =
            await (await _strategy.BlockRecordsAsync(records, blockingFields)).ToListAsync();

        Assert.Single(result);  // Only one group should be created for missing fields
        Assert.Equal("", result.First().Key);  // The key should be an empty string
        Assert.Equal(1, result.First().Count());  // All records should be in this group  
    }

    [Fact]
    public async Task BlockRecordsAsync_ShouldThrowObjectDisposedException_WhenDisposed()
    {
        // Arrange
        await _strategy.DisposeAsync();
        var records = AsyncEnumerable.Empty<IDictionary<string, object>>();
        var blockingFields = new[] { "LastName" };

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await _strategy.BlockRecordsAsync(records, blockingFields));
    }

    [Fact]
    public async Task BlockRecordsAsync_ShouldThrowArgumentNullException_WhenRecordsAreNull()
    {
        // Arrange
        var blockingFields = new[] { "LastName" };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await _strategy.BlockRecordsAsync(null, blockingFields));
    }

    [Fact]
    public async Task BlockRecordsAsync_ShouldThrowArgumentNullException_WhenBlockingFieldsAreNull()
    {
        // Arrange
        var records = AsyncEnumerable.Empty<IDictionary<string, object>>();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await _strategy.BlockRecordsAsync(records, null));
    }

    [Fact]
    public async Task BlockRecordsAsync_ShouldThrowArgumentException_WhenNoBlockingFieldsProvided()
    {
        // Arrange
        var records = AsyncEnumerable.Empty<IDictionary<string, object>>();
        var blockingFields = Enumerable.Empty<string>();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _strategy.BlockRecordsAsync(records, blockingFields));
    }

    [Fact]
    public async Task GenerateBlockingKey_ShouldReturnCorrectKey()
    {
        // Arrange
        var record = new Dictionary<string, object>
        {
            { "FirstName", "John" },
            { "LastName", "Doe" }
        };
        var blockingFields = new[] { "FirstName", "LastName" };

        // Act
        var result = typeof(ParallelBlockingStrategy)
            .GetMethod("GenerateBlockingKey", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            .Invoke(_strategy, new object[] { record, blockingFields });

        // Assert
        Assert.Equal("John|Doe", result);
    }

    [Fact]
    public async Task DisposeAsync_ShouldDisposeOnce()
    {
        // Arrange & Act
        await _strategy.DisposeAsync();

        // Act & Assert
        await _strategy.DisposeAsync(); // Ensure no exception or re-disposing logic
    }
    [Fact]
    public async Task BlockRecordsAsync_ShouldProcessMultipleBatches_WhenRecordsExceedBatchSize()
    {
        // Arrange
        int batchSize = 2; // Small batch size for testing
        int recordCount = 5; // Number of records to force multiple batches
        var blockingFields = new[] { "Field1" };

        var records = new List<IDictionary<string, object>>
    {
        new Dictionary<string, object> { { "Field1", "A" } },
        new Dictionary<string, object> { { "Field1", "A" } },
        new Dictionary<string, object> { { "Field1", "B" } },
        new Dictionary<string, object> { { "Field1", "B" } },
        new Dictionary<string, object> { { "Field1", "C" } }
    }.ToAsyncEnumerable();

        // Mock the options to set batch size and max degree of parallelism
        var options = Options.Create(new RecordLinkageOptions
        {
            BatchSize = batchSize,
            MaxDegreeOfParallelism = 2,
            BufferSize = 5
        });

        // Initialize the strategy with mocked dependencies
        var strategy = new ParallelBlockingStrategy(_mockLogger.Object, options, _mockTelemetry.Object);

        // Act
        var result = await (await strategy.BlockRecordsAsync(records, blockingFields)).ToListAsync();

        // Assert
        Assert.Equal(3, result.Count); // Expecting 3 distinct groups for keys "A", "B", and "C"

        var groupA = result.FirstOrDefault(g => g.Key == "A");
        var groupB = result.FirstOrDefault(g => g.Key == "B");
        var groupC = result.FirstOrDefault(g => g.Key == "C");

        Assert.NotNull(groupA);
        Assert.NotNull(groupB);
        Assert.NotNull(groupC);

        Assert.Equal(2, groupA.Count()); // Group "A" should contain 2 records
        Assert.Equal(2, groupB.Count()); // Group "B" should contain 2 records
        Assert.Single(groupC);           // Group "C" should contain 1 record
   
        // Verify log information about batches processed
        _mockLogger.Verify(
            logger => logger.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                null,
                (Func<It.IsAnyType, Exception, string>)It.IsAny<object>()),
            Times.AtLeastOnce);
    }

}
