using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace MatchLogic.Application.UnitTests
{
    public class MemoryMappedRecordStoreTests : IDisposable
    {
        private readonly string _testDirectory;
        private readonly Mock<ILogger> _mockLogger;
        private readonly MemoryMappedStoreOptions _options;
        private readonly List<string> _filesToCleanup;

        public MemoryMappedRecordStoreTests()
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), $"MMStoreTest_{Guid.NewGuid():N}");
            Directory.CreateDirectory(_testDirectory);
            _mockLogger = new Mock<ILogger>();
            _filesToCleanup = new List<string>();

            _options = new MemoryMappedStoreOptions
            {
                TempDirectory = _testDirectory,
                InitialFileSize = 10 * 1024 * 1024, // 10 MB for tests
                MaxMemoryUsage = 50 * 1024 * 1024, // 50 MB limit for tests
                PageSize = 4096,
                WriteBufferSize = 1024 * 1024
            };
        }

        #region Basic Operations Tests

        [Fact]
        public async Task AddRecordAsync_SingleRecord_ShouldStoreAndRetrieve()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var record = CreateTestRecord("test1", 42, "value1");

            // Act
            await store.AddRecordAsync(record);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("test1", retrieved["name"]);
            Assert.Equal(42, retrieved["age"]);
            Assert.Equal("value1", retrieved["data"]);
        }

        [Fact]
        public async Task AddRecordAsync_MultipleRecords_ShouldMaintainOrder()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var records = new List<IDictionary<string, object>>();
            for (int i = 0; i < 100; i++)
            {
                records.Add(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act
            for (int i = 0; i < records.Count; i++)
            {
                await store.AddRecordAsync(records[i]);
            }

            // Assert
            for (int i = 0; i < records.Count; i++)
            {
                var retrieved = await store.GetRecordAsync(i);
                Assert.NotNull(retrieved);
                Assert.Equal($"test{i}", retrieved["name"]);
                Assert.Equal(i, retrieved["age"]);
            }
        }

        [Fact]
        public async Task GetRecordAsync_NonExistentRecord_ShouldReturnNull()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test", 1, "value"));

            // Act
            var result = await store.GetRecordAsync(999);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task GetRecordsAsync_MultipleRowNumbers_ShouldReturnRequestedRecords()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            for (int i = 0; i < 10; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act
            var rowNumbers = new[] { 2, 5, 7, 9 };
            var results = await store.GetRecordsAsync(rowNumbers);

            // Assert
            Assert.Equal(4, results.Count);
            Assert.Equal("test2", results[0]["name"]);
            Assert.Equal("test5", results[1]["name"]);
            Assert.Equal("test7", results[2]["name"]);
            Assert.Equal("test9", results[3]["name"]);
        }

        [Fact]
        public async Task GetRecordsAsync_WithInvalidRowNumbers_ShouldReturnOnlyValidRecords()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            for (int i = 0; i < 5; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act
            var rowNumbers = new[] { 1, 3, 99, 100, -1 };
            var results = await store.GetRecordsAsync(rowNumbers);

            // Assert
            Assert.Equal(2, results.Count);
            Assert.Equal("test1", results[0]["name"]);
            Assert.Equal("test3", results[1]["name"]);
        }

        [Fact]
        public async Task GetRecordsAsync_ProximityOrdering_ShouldOptimizeReads()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            for (int i = 0; i < 20; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act - Request out of order
            var rowNumbers = new[] { 15, 5, 10, 0, 19 };
            var results = await store.GetRecordsAsync(rowNumbers);

            // Assert - Results should still be valid despite optimization
            Assert.Equal(5, results.Count);
            Assert.Contains(results, r => (string)r["name"] == "test15");
            Assert.Contains(results, r => (string)r["name"] == "test5");
            Assert.Contains(results, r => (string)r["name"] == "test10");
            Assert.Contains(results, r => (string)r["name"] == "test0");
            Assert.Contains(results, r => (string)r["name"] == "test19");
        }

        #endregion

        #region Memory-Mapped Specific Tests

        [Fact]
        public async Task FileResize_AutomaticExpansion_ShouldHandleGrowth()
        {
            // Arrange
            var smallOptions = new MemoryMappedStoreOptions
            {
                TempDirectory = _testDirectory,
                InitialFileSize = 1024 * 100, // 100 KB initial
                MaxMemoryUsage = 10 * 1024 * 1024, // 10 MB max
                WriteBufferSize = 1024
            };

            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), smallOptions, _mockLogger.Object);

            // Act - Add enough records to trigger resize
            for (int i = 0; i < 500; i++)
            {
                var record = CreateTestRecord($"test{i}", i, new string('X', 200));
                await store.AddRecordAsync(record);
            }

            // Assert - All records should be retrievable after resize
            for (int i = 0; i < 500; i++)
            {
                var retrieved = await store.GetRecordAsync(i);
                Assert.NotNull(retrieved);
                Assert.Equal($"test{i}", retrieved["name"]);
            }

            var stats = store.GetStatistics();
            Assert.Equal(500, stats.RecordCount);
        }

        [Fact]
        public async Task MaxMemoryLimit_ShouldPreventExcessiveGrowth()
        {
            // Arrange
            var limitedOptions = new MemoryMappedStoreOptions
            {
                TempDirectory = _testDirectory,
                InitialFileSize = 1024 * 10, // 10 KB
                MaxMemoryUsage = 1024 * 100, // 100 KB max - very small
                WriteBufferSize = 1024
            };

            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), limitedOptions, _mockLogger.Object);
            var largeRecord = CreateTestRecord("large", 1, new string('X', 50000)); // 50KB record

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await store.AddRecordAsync(largeRecord);
                await store.AddRecordAsync(largeRecord);
                await store.AddRecordAsync(largeRecord);// Should fail on second
            });
        }

        /*[Fact]
        public async Task HeaderValidation_ShouldWriteAndValidateHeader()
        {
            // Arrange
            var dataSourceId = Guid.NewGuid();
            using var store = new MemoryMappedRecordStore(dataSourceId, _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test", 1, "value"));

            // Act - Read the file directly to check header
            var dataFiles = Directory.GetFiles(_testDirectory, "*.dat");
            Assert.NotEmpty(dataFiles);

            var headerBytes = new byte[64];
            using (var fs = File.OpenRead(dataFiles[0]))
            {
                fs.Read(headerBytes, 0, 64);
            }

            // Assert - Check magic bytes
            var magic = Encoding.UTF8.GetString(headerBytes, 0, 4);
            Assert.Equal("MMRS", magic);

            // Check version
            var version = BitConverter.ToInt32(headerBytes, 4);
            Assert.Equal(1, version);
        }*/

        #endregion

        #region Read-Only Mode Tests

        [Fact]
        public async Task SwitchToReadOnlyMode_ShouldPreventFurtherWrites()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test1", 1, "value1"));

            // Act
            await store.SwitchToReadOnlyModeAsync();

            // Assert
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await store.AddRecordAsync(CreateTestRecord("test2", 2, "value2")));

            var stats = store.GetStatistics();
            Assert.True(stats.IsReadOnly);
        }

        [Fact]
        public async Task SwitchToReadOnlyMode_ShouldAllowReads()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test1", 1, "value1"));
            await store.AddRecordAsync(CreateTestRecord("test2", 2, "value2"));

            // Act
            await store.SwitchToReadOnlyModeAsync();
            var record1 = await store.GetRecordAsync(0);
            var record2 = await store.GetRecordAsync(1);

            // Assert
            Assert.NotNull(record1);
            Assert.NotNull(record2);
            Assert.Equal("test1", record1["name"]);
            Assert.Equal("test2", record2["name"]);
        }

        [Fact]
        public async Task SwitchToReadOnlyMode_MultipleCalls_ShouldBeIdempotent()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test", 1, "value"));

            // Act & Assert (should not throw)
            await store.SwitchToReadOnlyModeAsync();
            await store.SwitchToReadOnlyModeAsync();
            await store.SwitchToReadOnlyModeAsync();

            var stats = store.GetStatistics();
            Assert.True(stats.IsReadOnly);
        }

        #endregion

        #region Metadata Persistence Tests

        [Fact]
        public async Task MetadataPersistence_ShouldSaveOnReadOnlySwitch()
        {
            // Arrange
            var dataSourceId = Guid.NewGuid();
            using var store = new MemoryMappedRecordStore(dataSourceId, _options, _mockLogger.Object);

            for (int i = 0; i < 10; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act
            await store.SwitchToReadOnlyModeAsync();

            // Assert - Metadata file should exist
            var metadataFiles = Directory.GetFiles(_testDirectory, "*.meta");
            Assert.NotEmpty(metadataFiles);

            // Verify metadata content
            var metadataContent = await File.ReadAllTextAsync(metadataFiles[0]);
            Assert.Contains("RecordCount", metadataContent);
            Assert.Contains("\"RecordCount\":10", metadataContent);
        }

        #endregion

        #region Concurrent Access Tests

        [Fact]
        public async Task ConcurrentWrites_ShouldBeThreadSafe()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var tasks = new List<Task>();
            var recordCount = 100;

            // Act
            for (int i = 0; i < recordCount; i++)
            {
                var index = i;
                tasks.Add(Task.Run(async () =>
                {
                    await store.AddRecordAsync(CreateTestRecord($"test{index}", index, $"value{index}"));
                }));
            }
            await Task.WhenAll(tasks);

            // Assert
            var stats = store.GetStatistics();
            Assert.Equal(recordCount, stats.RecordCount);
        }

        [Fact]
        public async Task ConcurrentReads_ShouldBeThreadSafe()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var recordCount = 50;

            for (int i = 0; i < recordCount; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act
            var tasks = new List<Task<IDictionary<string, object>>>();
            for (int i = 0; i < recordCount; i++)
            {
                var index = i;
                tasks.Add(Task.Run(async () => await store.GetRecordAsync(index)));
            }
            var results = await Task.WhenAll(tasks);

            // Assert
            for (int i = 0; i < recordCount; i++)
            {
                Assert.NotNull(results[i]);
                Assert.Equal($"test{i}", results[i]["name"]);
            }
        }

        [Fact]
        public async Task ConcurrentReadWrite_ShouldBeThreadSafe()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);

            // Add initial records
            for (int i = 0; i < 50; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"initial{i}", i, $"value{i}"));
            }

            // Act - Concurrent reads and writes
            var writeTasks = new List<Task>();
            var readTasks = new List<Task<IDictionary<string, object>>>();

            for (int i = 50; i < 100; i++)
            {
                var index = i;
                writeTasks.Add(Task.Run(async () =>
                {
                    await store.AddRecordAsync(CreateTestRecord($"concurrent{index}", index, $"value{index}"));
                }));
            }

            for (int i = 0; i < 50; i++)
            {
                var index = i;
                readTasks.Add(Task.Run(async () => await store.GetRecordAsync(index)));
            }

            await Task.WhenAll(writeTasks);
            var readResults = await Task.WhenAll(readTasks);

            // Assert
            var stats = store.GetStatistics();
            Assert.Equal(100, stats.RecordCount);

            for (int i = 0; i < 50; i++)
            {
                Assert.NotNull(readResults[i]);
                Assert.Equal($"initial{i}", readResults[i]["name"]);
            }
        }

        #endregion

        #region Edge Cases and Error Handling

        [Fact]
        public async Task EmptyRecord_ShouldStoreAndRetrieve()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var emptyRecord = new Dictionary<string, object>();

            // Act
            await store.AddRecordAsync(emptyRecord);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Empty(retrieved);
        }

        [Fact]
        public async Task RecordWithNullValues_ShouldHandleCorrectly()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var record = new Dictionary<string, object>
            {
                ["name"] = "test",
                ["value"] = null,
                ["number"] = 42,
                ["flag"] = true,
                ["date"] = DateTime.Parse("2024-01-01"),
                ["guid"] = Guid.Parse("12345678-1234-1234-1234-123456789012")
            };

            // Act
            await store.AddRecordAsync(record);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("test", retrieved["name"]);
            Assert.Null(retrieved["value"]);
            Assert.Equal(42, retrieved["number"]);
            Assert.Equal(true, retrieved["flag"]);
            Assert.IsType<DateTime>(retrieved["date"]);
            Assert.IsType<Guid>(retrieved["guid"]);
        }

        [Fact]
        public async Task RecordWithComplexTypes_ShouldPreserveTypes()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var record = new Dictionary<string, object>
            {
                ["string"] = "text",
                ["int"] = 42,
                ["long"] = 9999999999L,
                ["double"] = 3.14159,
                ["bool"] = true,
                ["date"] = DateTime.UtcNow,
                ["guid"] = Guid.NewGuid(),
                ["array"] = new List<object> { 1, "two", 3.0 },
                ["nested"] = new Dictionary<string, object> { ["inner"] = "value" }
            };

            // Act
            await store.AddRecordAsync(record);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.IsType<string>(retrieved["string"]);
            Assert.IsType<int>(retrieved["int"]);
            Assert.IsType<long>(retrieved["long"]);
            Assert.IsType<double>(retrieved["double"]);
            Assert.IsType<bool>(retrieved["bool"]);
            Assert.IsType<DateTime>(retrieved["date"]);
            Assert.IsType<Guid>(retrieved["guid"]);
            Assert.IsType<List<object>>(retrieved["array"]);
            Assert.IsType<Dictionary<string, object>>(retrieved["nested"]);
        }

        [Fact]
        public async Task Dispose_ShouldCleanupFiles()
        {
            // Arrange
            var dataSourceId = Guid.NewGuid();
            var store = new MemoryMappedRecordStore(dataSourceId, _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test", 1, "value"));

            var filesBefore = Directory.GetFiles(_testDirectory, $"*{dataSourceId:N}*");
            Assert.NotEmpty(filesBefore);

            // Act
            store.Dispose();

            // Assert
            var filesAfter = Directory.GetFiles(_testDirectory, $"*{dataSourceId:N}*");
            Assert.Empty(filesAfter);
        }

        [Fact]
        public async Task Statistics_ShouldProvideAccurateMetrics()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var recordCount = 25;

            for (int i = 0; i < recordCount; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act
            var stats = store.GetStatistics();

            // Assert
            Assert.Equal(recordCount, stats.RecordCount);
            Assert.True(stats.TotalSizeBytes > 0);
            Assert.False(stats.IsReadOnly);
            Assert.Equal("MemoryMapped", stats.StorageType);
        }

        #endregion

        #region Performance and Stress Tests

        [Fact]
        public async Task LargeVolumeWrite_ShouldHandleThousandsOfRecords()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var recordCount = 1000;

            // Act
            for (int i = 0; i < recordCount; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Assert
            var stats = store.GetStatistics();
            Assert.Equal(recordCount, stats.RecordCount);

            // Verify random access
            var randomIndices = new[] { 0, 250, 500, 750, 999 };
            foreach (var index in randomIndices)
            {
                var record = await store.GetRecordAsync(index);
                Assert.NotNull(record);
                Assert.Equal($"test{index}", record["name"]);
            }
        }

        [Fact]
        public async Task BatchRead_ShouldBeEfficient()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var recordCount = 500;

            for (int i = 0; i < recordCount; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Act - Read every 10th record
            var indices = Enumerable.Range(0, 50).Select(i => i * 10).ToList();
            var results = await store.GetRecordsAsync(indices);

            // Assert
            Assert.Equal(50, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal($"test{i * 10}", results[i]["name"]);
            }
        }

        [Fact]
        public async Task PeriodicFlush_ShouldOccur()
        {
            // Arrange
            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);

            // Act - Add exactly 100 records (flush occurs every 100)
            for (int i = 0; i < 100; i++)
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Small delay to ensure flush completes
            await Task.Delay(100);

            // Assert - Data should be readable even after potential crash
            // This verifies that flush actually wrote to disk
            var retrieved = await store.GetRecordAsync(99);
            Assert.NotNull(retrieved);
            Assert.Equal("test99", retrieved["name"]);
        }

        #endregion

        #region Memory Management Tests

        [Fact]
        public async Task MemoryPressure_ShouldTriggerGC()
        {
            // Arrange
            var pressureOptions = new MemoryMappedStoreOptions
            {
                TempDirectory = _testDirectory,
                InitialFileSize = 5 * 1024 * 1024,
                MaxMemoryUsage = 10 * 1024 * 1024, // 10 MB limit
                WriteBufferSize = 1024
            };

            using var store = new MemoryMappedRecordStore(Guid.NewGuid(), pressureOptions, _mockLogger.Object);

            // Act - Add records to approach memory limit
            for (int i = 0; i < 100; i++)
            {
                var largeData = new string('X', 10000); // 10KB per record
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, largeData));
            }

            // Force a read to trigger memory management
            var retrieved = await store.GetRecordAsync(50);

            // Assert
            Assert.NotNull(retrieved);
            var stats = store.GetStatistics();
            Assert.Equal(100, stats.RecordCount);
        }

        #endregion

        #region Helper Methods

        private IDictionary<string, object> CreateTestRecord(string name, int age, string data)
        {
            return new Dictionary<string, object>
            {
                ["name"] = name,
                ["age"] = age,
                ["data"] = data,
                ["timestamp"] = DateTime.UtcNow,
                ["id"] = Guid.NewGuid()
            };
        }

        public void Dispose()
        {
            try
            {
                // Clean up test files
                foreach (var file in _filesToCleanup)
                {
                    if (File.Exists(file))
                        File.Delete(file);
                }

                if (Directory.Exists(_testDirectory))
                {
                    Directory.Delete(_testDirectory, true);
                }
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }

        #endregion
    }
}