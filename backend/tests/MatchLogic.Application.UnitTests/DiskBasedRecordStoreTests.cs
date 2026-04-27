using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests
{
    public class DiskBasedRecordStoreTests : IDisposable
    {
        private readonly string _testDirectory;
        private readonly Mock<ILogger<DiskBasedRecordStore>> _mockLogger;
        private readonly QGramIndexerOptions _options;
        private readonly List<string> _filesToCleanup;

        public DiskBasedRecordStoreTests()
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), $"DiskStoreTest_{Guid.NewGuid():N}");
            Directory.CreateDirectory(_testDirectory);
            _mockLogger = new Mock<ILogger<DiskBasedRecordStore>>();
            _filesToCleanup = new List<string>();

            _options = new QGramIndexerOptions
            {
                TempDirectory = _testDirectory,
                DiskBufferSize = 4096,
                EnableCompression = true,
                MaxRecordSize = 1_000_000,
                IndexSaveFrequency = 10
            };
        }

        #region Basic Operations Tests

        [Fact]
        public async Task AddRecordAsync_SingleRecord_ShouldStoreAndRetrieve()
        {
            // Arrange
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var record = CreateTestRecord("test1", 42, "value1");

            // Act
            await store.AddRecordAsync(record);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("test1", retrieved["name"]);
            Assert.Equal(42L, JsonSerializer.Deserialize<JsonElement>(retrieved["age"].ToString()).GetInt64());
            Assert.Equal("value1", retrieved["data"]);
        }

        [Fact]
        public async Task AddRecordAsync_MultipleRecords_ShouldMaintainOrder()
        {
            // Arrange
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            }
        }

        [Fact]
        public async Task GetRecordAsync_NonExistentRecord_ShouldReturnNull()
        {
            // Arrange
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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

        #endregion

        #region Compression Tests

        [Fact]
        public async Task Compression_SmallRecord_ShouldNotCompress()
        {
            // Arrange
            var options = new QGramIndexerOptions
            {
                TempDirectory = _testDirectory,
                EnableCompression = true,
                DiskBufferSize = 4096,
                MaxRecordSize = 1_000_000
            };

            using var store = new DiskBasedRecordStore(Guid.NewGuid(), options, _mockLogger.Object);
            var smallRecord = CreateTestRecord("small", 1, "tiny"); // < 1024 bytes

            // Act
            await store.AddRecordAsync(smallRecord);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("small", retrieved["name"]);

            // Verify file size indicates no compression (compressed would have gzip overhead)
            var stats = store.GetStatistics();
            Assert.True(stats.TotalSizeBytes < 500); // Small uncompressed record
        }

        [Fact]
        public async Task Compression_LargeRecord_ShouldCompress()
        {
            // Arrange
            var options = new QGramIndexerOptions
            {
                TempDirectory = _testDirectory,
                EnableCompression = true,
                DiskBufferSize = 4096,
                MaxRecordSize = 10_000_000
            };

            using var store = new DiskBasedRecordStore(Guid.NewGuid(), options, _mockLogger.Object);

            // Create a large record with repetitive data (compresses well)
            var largeData = string.Join("", Enumerable.Repeat("ABCDEFGHIJ", 500)); // 5000 chars
            var largeRecord = CreateTestRecord("large", 1, largeData);

            // Act
            await store.AddRecordAsync(largeRecord);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("large", retrieved["name"]);
            Assert.Equal(largeData, retrieved["data"]);

            // Verify compression occurred
            var stats = store.GetStatistics();
            Assert.Equal("DiskCompressed", stats.StorageType);
        }

        [Fact]
        public async Task Compression_DisabledCompression_ShouldNotCompress()
        {
            // Arrange
            var options = new QGramIndexerOptions
            {
                TempDirectory = _testDirectory,
                EnableCompression = false,
                DiskBufferSize = 4096,
                MaxRecordSize = 10_000_000
            };

            using var store = new DiskBasedRecordStore(Guid.NewGuid(), options, _mockLogger.Object);
            var largeData = string.Join("", Enumerable.Repeat("ABCDEFGHIJ", 500));
            var record = CreateTestRecord("test", 1, largeData);

            // Act
            await store.AddRecordAsync(record);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal(largeData, retrieved["data"]);

            var stats = store.GetStatistics();
            Assert.Equal("Disk", stats.StorageType);
        }

        #endregion

        #region Read-Only Mode Tests

        [Fact]
        public async Task SwitchToReadOnlyMode_ShouldPreventFurtherWrites()
        {
            // Arrange
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test", 1, "value"));

            // Act & Assert (should not throw)
            await store.SwitchToReadOnlyModeAsync();
            await store.SwitchToReadOnlyModeAsync();
            await store.SwitchToReadOnlyModeAsync();

            var stats = store.GetStatistics();
            Assert.True(stats.IsReadOnly);
        }

        #endregion

        #region Index Persistence Tests

        [Fact]
        public async Task IndexPersistence_ShouldSaveIndexPeriodically()
        {
            // Arrange
            var options = new QGramIndexerOptions
            {
                TempDirectory = _testDirectory,
                IndexSaveFrequency = 5,
                DiskBufferSize = 4096,
                MaxRecordSize = 1_000_000
            };

            var dataSourceId = Guid.NewGuid();
            using var store = new DiskBasedRecordStore(dataSourceId, options, _mockLogger.Object);

            // Act
            for (int i = 0; i < 6; i++) // Trigger index save at record 5
            {
                await store.AddRecordAsync(CreateTestRecord($"test{i}", i, $"value{i}"));
            }

            // Assert - Index file should exist
            var indexFiles = Directory.GetFiles(_testDirectory, "*.idx");
            Assert.NotEmpty(indexFiles);
        }

        [Fact]
        public async Task IndexPersistence_ShouldSaveIndexOnReadOnlySwitch()
        {
            // Arrange
            var dataSourceId = Guid.NewGuid();
            using var store = new DiskBasedRecordStore(dataSourceId, _options, _mockLogger.Object);

            await store.AddRecordAsync(CreateTestRecord("test", 1, "value"));

            // Act
            await store.SwitchToReadOnlyModeAsync();

            // Assert
            var indexFiles = Directory.GetFiles(_testDirectory, "*.idx");
            Assert.NotEmpty(indexFiles);
        }

        #endregion

        #region Concurrent Access Tests

        [Fact]
        public async Task ConcurrentWrites_ShouldBeThreadSafe()
        {
            // Arrange
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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

        #endregion

        #region Edge Cases and Error Handling

        [Fact]
        public async Task AddRecord_ExceedingMaxSize_ShouldThrow()
        {
            // Arrange
            var options = new QGramIndexerOptions
            {
                TempDirectory = _testDirectory,
                MaxRecordSize = 100,
                DiskBufferSize = 4096
            };

            using var store = new DiskBasedRecordStore(Guid.NewGuid(), options, _mockLogger.Object);
            var largeRecord = CreateTestRecord("test", 1, new string('X', 1000));

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await store.AddRecordAsync(largeRecord));
        }

        /*[Fact]
        public async Task GetRecord_CorruptedData_ShouldReturnNull()
        {
            // Arrange
            var dataSourceId = Guid.NewGuid();
            using var store = new DiskBasedRecordStore(dataSourceId, _options, _mockLogger.Object);
            await store.AddRecordAsync(CreateTestRecord("test", 1, "value"));

            // Corrupt the data file
            var dataFiles = Directory.GetFiles(_testDirectory, "*.data");
            File.WriteAllText(dataFiles[0], "CORRUPTED DATA");

            // Act
            var result = await store.GetRecordAsync(0);

            // Assert
            Assert.Null(result);
        }*/

        [Fact]
        public async Task Dispose_ShouldCleanupFiles()
        {
            // Arrange
            var dataSourceId = Guid.NewGuid();
            var store = new DiskBasedRecordStore(dataSourceId, _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            Assert.Equal("DiskCompressed", stats.StorageType);
        }

        [Fact]
        public async Task EmptyRecord_ShouldStoreAndRetrieve()
        {
            // Arrange
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
            var record = new Dictionary<string, object>
            {
                ["name"] = "test",
                ["value"] = null,
                ["number"] = 42
            };

            // Act
            await store.AddRecordAsync(record);
            var retrieved = await store.GetRecordAsync(0);

            // Assert
            Assert.NotNull(retrieved);
            Assert.Equal("test", retrieved["name"]);
            Assert.Null(retrieved["value"]);
            Assert.Equal(42L, JsonSerializer.Deserialize<JsonElement>(retrieved["number"].ToString()).GetInt64());
        }

        #endregion

        #region Performance and Stress Tests

        [Fact]
        public async Task LargeVolumeWrite_ShouldHandleThousandsOfRecords()
        {
            // Arrange
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
            using var store = new DiskBasedRecordStore(Guid.NewGuid(), _options, _mockLogger.Object);
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
