using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    /// <summary>
    /// Configuration options for memory-mapped record store
    /// </summary>
    public class MemoryMappedStoreOptions
    {
        public string TempDirectory { get; set; } = Path.GetTempPath();
        public long InitialFileSize { get; set; } = 100 * 1024 * 1024; // 100 MB
        public long MaxMemoryUsage { get; set; } = 1024 * 1024 * 1024; // 1 GB
        public int PageSize { get; set; } = 4096; // 4 KB pages
        public int MaxCachedPages { get; set; } = 10000; // Max pages to keep in memory
        public bool EnableCompression { get; set; } = false;
        public int WriteBufferSize { get; set; } = 1024 * 1024; // 1 MB write buffer
    }

    /// <summary>
    /// High-performance memory-mapped file based record store
    /// </summary>
    public class MemoryMappedRecordStore : IRecordStore
    {
        private readonly struct RecordMetadata
        {
            public long Offset { get; init; }
            public int Length { get; init; }
            public int RecordNumber { get; init; }
            public bool IsDeleted { get; init; }
        }

        private readonly struct PageInfo
        {
            public int PageNumber { get; init; }
            public long StartOffset { get; init; }
            public long EndOffset { get; init; }
            public DateTime LastAccess { get; init; }
        }

        private readonly MemoryMappedStoreOptions _options;
        private readonly ILogger _logger;
        private readonly string _dataFilePath;
        private readonly string _metadataFilePath;
        private readonly JsonSerializerOptions _jsonOptions;

        private MemoryMappedFile _memoryMappedFile;
        private MemoryMappedViewStream _viewStream;
        private readonly ConcurrentDictionary<int, RecordMetadata> _recordIndex;
        private readonly ConcurrentDictionary<int, PageInfo> _pageCache;
        private readonly SemaphoreSlim _writeLock;
        private readonly SemaphoreSlim _resizeLock;
        private readonly Timer _cacheCleanupTimer;

        private long _currentFileSize;
        private long _currentWritePosition;
        private int _recordCount;
        private bool _isReadOnly;
        private bool _disposed;

        public MemoryMappedRecordStore(
            Guid dataSourceId,
            MemoryMappedStoreOptions options = null,
            ILogger logger = null)
        {
            _options = options ?? new MemoryMappedStoreOptions();
            _logger = logger;
            _recordIndex = new ConcurrentDictionary<int, RecordMetadata>();
            _pageCache = new ConcurrentDictionary<int, PageInfo>();
            _writeLock = new SemaphoreSlim(1, 1);
            _resizeLock = new SemaphoreSlim(1, 1);

            // Setup JSON serialization with custom converter
            _jsonOptions = new JsonSerializerOptions
            {
                Converters = { new DictionaryStringObjectJsonConverter() },
                WriteIndented = false
            };

            var filePrefix = $"mmap_{dataSourceId:N}_{DateTime.UtcNow:yyyyMMdd_HHmmss}";
            _dataFilePath = Path.Combine(_options.TempDirectory, $"{filePrefix}.dat");
            _metadataFilePath = Path.Combine(_options.TempDirectory, $"{filePrefix}.meta");

            InitializeMemoryMappedFile();

            // Setup cache cleanup timer
            _cacheCleanupTimer = new Timer(
                CleanupCache,
                null,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(30));

            _logger?.LogDebug("Created memory-mapped store: {DataFile}", _dataFilePath);
        }

        private void InitializeMemoryMappedFile()
        {
            // Create and size the file initially
            using (var fs = new FileStream(
                _dataFilePath,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.Read))
            {
                _currentFileSize = _options.InitialFileSize;
                fs.SetLength(_currentFileSize);
            }

            // Create memory-mapped file directly from file path
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(
                _dataFilePath,
                FileMode.Open,
                null,  // mapName
                _currentFileSize,
                MemoryMappedFileAccess.ReadWrite);

            // Create initial view stream
            _viewStream = _memoryMappedFile.CreateViewStream(
                0,
                _currentFileSize,
                MemoryMappedFileAccess.ReadWrite);

            // Write header
            WriteHeader();
        }

        private void WriteHeader()
        {
            var header = new byte[64];
            var version = BitConverter.GetBytes(1); // Version 1
            var magic = Encoding.UTF8.GetBytes("MMRS"); // Magic bytes

            Buffer.BlockCopy(magic, 0, header, 0, 4);
            Buffer.BlockCopy(version, 0, header, 4, 4);

            _viewStream.Position = 0;
            _viewStream.Write(header, 0, header.Length);
            _currentWritePosition = 64; // Start after header
        }

        public async Task AddRecordAsync(IDictionary<string, object> record)
        {
            if (_isReadOnly)
                throw new InvalidOperationException("Store is in read-only mode");

            await _writeLock.WaitAsync();
            try
            {
                // Serialize record
                var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(record, _jsonOptions);
                var recordLength = jsonBytes.Length;

                // Check if we need to resize
                if (_currentWritePosition + recordLength + 8 > _currentFileSize)
                {
                    await ResizeFileAsync(recordLength);
                }

                // Write record length and data
                _viewStream.Position = _currentWritePosition;

                // Write length prefix
                var lengthBytes = BitConverter.GetBytes(recordLength);
                await _viewStream.WriteAsync(lengthBytes, 0, 4);

                // Write record data
                await _viewStream.WriteAsync(jsonBytes, 0, jsonBytes.Length);

                // Write length suffix (for backward scanning)
                await _viewStream.WriteAsync(lengthBytes, 0, 4);

                // Update index
                var metadata = new RecordMetadata
                {
                    Offset = _currentWritePosition,
                    Length = recordLength,
                    RecordNumber = _recordCount,
                    IsDeleted = false
                };

                _recordIndex[_recordCount] = metadata;

                // Update position and count
                _currentWritePosition += recordLength + 8; // +8 for length prefix and suffix
                _recordCount++;

                // Flush periodically
                if (_recordCount % 100 == 0)
                {
                    await _viewStream.FlushAsync();
                }
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public async Task<IDictionary<string, object>> GetRecordAsync(int rowNumber)
        {
            if (!_recordIndex.TryGetValue(rowNumber, out var metadata))
                return null;

            if (metadata.IsDeleted)
                return null;

            return await ReadRecordAtPositionAsync(metadata.Offset, metadata.Length);
        }

        public async Task<IList<IDictionary<string, object>>> GetRecordsAsync(IEnumerable<int> rowNumbers)
        {
            var results = new List<IDictionary<string, object>>();

            // Group by proximity for efficient reading
            var sortedRequests = rowNumbers
                .Select(rn => (_recordIndex.TryGetValue(rn, out RecordMetadata meta) ? meta : default, rn))
                .Where(x => x.Item1.Offset > 0 && !x.Item1.IsDeleted)
                .OrderBy(x => x.Item1.Offset)
                .ToList();

            foreach (var (metadata, rowNumber) in sortedRequests)
            {
                var record = await ReadRecordAtPositionAsync(metadata.Offset, metadata.Length);
                if (record != null)
                    results.Add(record);
            }

            return results;
        }

        private async Task<IDictionary<string, object>> ReadRecordAtPositionAsync(long offset, int length)
        {
            try
            {
                // Check cache management
                //ManageCacheMemory();

                var buffer = ArrayPool<byte>.Shared.Rent(length + 8);
                try
                {
                    // Use view accessor for thread-safe random access
                    using var accessor = _memoryMappedFile.CreateViewAccessor(
                        offset,
                        length + 8,
                        MemoryMappedFileAccess.Read);

                    // Read length prefix
                    var lengthBytes = new byte[4];
                    accessor.ReadArray(0, lengthBytes, 0, 4);
                    var recordLength = BitConverter.ToInt32(lengthBytes, 0);

                    if (recordLength != length)
                    {
                        _logger?.LogWarning("Length mismatch at offset {Offset}", offset);
                        return null;
                    }

                    // Read record data
                    var jsonBytes = new byte[recordLength];
                    accessor.ReadArray(4, jsonBytes, 0, recordLength);

                    // Deserialize
                    return JsonSerializer.Deserialize<Dictionary<string, object>>(
                        jsonBytes,
                        _jsonOptions);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error reading record at offset {Offset}", offset);
                return null;
            }
        }

        private async Task ResizeFileAsync(int requiredSpace)
        {
            await _resizeLock.WaitAsync();
            try
            {
                // Calculate new size
                var minRequired = _currentWritePosition + requiredSpace + _options.WriteBufferSize;
                var newSize = Math.Max(_currentFileSize * 2, minRequired);

                //if (newSize > _options.MaxMemoryUsage)
                //{
                //    newSize = _options.MaxMemoryUsage;
                //    if (_currentWritePosition + requiredSpace > newSize)
                //    {
                //        throw new InvalidOperationException("Maximum file size reached");
                //    }
                //}

                _logger?.LogInformation("Resizing memory-mapped file from {OldSize} to {NewSize}",
                    _currentFileSize, newSize);

                // Dispose current views
                _viewStream?.Dispose();
                _viewStream = null;
                _memoryMappedFile?.Dispose();
                _memoryMappedFile = null;

                // Resize the file directly using File operations
                using (var tempStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Write))
                {
                    tempStream.SetLength(newSize);
                }

                // Create memory-mapped file directly from the file path
                _memoryMappedFile = MemoryMappedFile.CreateFromFile(
                    _dataFilePath,
                    FileMode.Open,
                    null,  // mapName
                    newSize,
                    MemoryMappedFileAccess.ReadWrite);

                // Create new view
                _viewStream = _memoryMappedFile.CreateViewStream(
                    0,
                    newSize,
                    MemoryMappedFileAccess.ReadWrite);

                _currentFileSize = newSize;
            }
            finally
            {
                _resizeLock.Release();
            }
        }

        private void ManageCacheMemory()
        {
            // Simple memory pressure check
            var currentMemory = GC.GetTotalMemory(false);
            if (currentMemory > _options.MaxMemoryUsage * 0.8)
            {
                // Force cleanup if approaching limit
                GC.Collect(2, GCCollectionMode.Forced);
                _logger?.LogDebug("Forced GC due to memory pressure: {Memory}", currentMemory);
            }
        }

        private void CleanupCache(object state)
        {
            // Cleanup old page cache entries
            var cutoff = DateTime.UtcNow.AddMinutes(-5);
            var toRemove = _pageCache
                .Where(kvp => kvp.Value.LastAccess < cutoff)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var key in toRemove)
            {
                _pageCache.TryRemove(key, out _);
            }
        }

        public async Task SwitchToReadOnlyModeAsync()
        {
            await _writeLock.WaitAsync();
            try
            {
                if (_isReadOnly) return;

                // Flush any pending writes
                await _viewStream.FlushAsync();

                // Save metadata
                await SaveMetadataAsync();

                _isReadOnly = true;
                _logger?.LogInformation("Switched to read-only mode with {Count} records", _recordCount);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        private async Task SaveMetadataAsync()
        {
            try
            {
                var metadata = new
                {
                    RecordCount = _recordCount,
                    FileSize = _currentFileSize,
                    WritePosition = _currentWritePosition,
                    Index = _recordIndex.ToDictionary(kvp => kvp.Key.ToString(), kvp => kvp.Value)
                };

                var json = JsonSerializer.Serialize(metadata, _jsonOptions);
                await File.WriteAllTextAsync(_metadataFilePath, json);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to save metadata");
            }
        }

        public StorageStatistics GetStatistics()
        {
            return new StorageStatistics
            {
                RecordCount = _recordCount,
                TotalSizeBytes = _currentWritePosition,
                IsReadOnly = _isReadOnly,
                StorageType = "MemoryMapped"
            };
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try
            {
                _cacheCleanupTimer?.Dispose();
                SaveMetadataAsync().Wait(5000);

                _viewStream?.Dispose();
                _memoryMappedFile?.Dispose();

                _writeLock?.Dispose();
                _resizeLock?.Dispose();

                // Cleanup files
                if (File.Exists(_dataFilePath)) File.Delete(_dataFilePath);
                if (File.Exists(_metadataFilePath)) File.Delete(_metadataFilePath);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error during disposal");
            }
        }
    }
        
}