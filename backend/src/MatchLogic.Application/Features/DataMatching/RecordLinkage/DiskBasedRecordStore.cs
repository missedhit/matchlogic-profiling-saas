using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    internal readonly struct RecordLocation
    {
        public long Offset { get; init; }
        public int CompressedLength { get; init; }
        public int UncompressedLength { get; init; }
        public int RowNumber { get; init; }
    }

    public partial class DiskBasedRecordStore : IRecordStore
    {
        private readonly string _dataFilePath;
        private readonly string _indexFilePath;
        private readonly Dictionary<int, RecordLocation> _rowIndex;
        private readonly FileStream _dataFileStream;
        private readonly BinaryWriter _dataWriter;
        private readonly SemaphoreSlim _writeLock;
        private readonly ILogger _logger;
        private readonly ArrayPool<byte> _bufferPool;
        private readonly QGramIndexerOptions _options;
        private readonly JsonSerializerOptions _jsonOptions;
        private bool _disposed;
        private int _currentRowNumber;
        private bool _isReadOnly;

        public DiskBasedRecordStore(Guid dataSourceId, QGramIndexerOptions options, ILogger logger = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger;
            _bufferPool = ArrayPool<byte>.Shared;
            _writeLock = new SemaphoreSlim(1, 1);
            _rowIndex = new Dictionary<int, RecordLocation>();

            var tempDir = _options.TempDirectory ?? Path.GetTempPath();
            var filePrefix = $"qgram_{dataSourceId:N}_{DateTime.UtcNow:yyyyMMdd_HHmmss}";

            _dataFilePath = Path.Combine(tempDir, $"{filePrefix}.data");
            _indexFilePath = Path.Combine(tempDir, $"{filePrefix}.idx");

            _dataFileStream = new FileStream(_dataFilePath, FileMode.Create, FileAccess.ReadWrite,
                FileShare.Read, _options.DiskBufferSize);
            _dataWriter = new BinaryWriter(_dataFileStream);

            _jsonOptions = new JsonSerializerOptions
            {
                Converters = { new DictionaryStringObjectJsonConverter() },
                WriteIndented = false, // Compact JSON for storage efficiency
                PropertyNameCaseInsensitive = true
            };

            _logger?.LogDebug("Created disk store: {DataFile}", _dataFilePath);
        }

        public async Task AddRecordAsync(IDictionary<string, object> record)
        {
            if (_isReadOnly)
                throw new InvalidOperationException("Store is in read-only mode");

            if (EstimateRecordSize(record) > _options.MaxRecordSize)
                throw new ArgumentException($"Record exceeds maximum size of {_options.MaxRecordSize} bytes");

            await _writeLock.WaitAsync();
            try
            {
                // Use the custom JSON options
                var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(record, _jsonOptions);
                var currentOffset = _dataFileStream.Position;

                byte[] finalBytes;
                int uncompressedLength = jsonBytes.Length;

                if (_options.EnableCompression && jsonBytes.Length > 1024)
                {
                    finalBytes = CompressData(jsonBytes);
                }
                else
                {
                    finalBytes = jsonBytes;
                }

                _dataWriter.Write(uncompressedLength);
                _dataWriter.Write(finalBytes.Length);
                _dataWriter.Write(finalBytes);
                _dataWriter.Flush();

                _rowIndex[_currentRowNumber] = new RecordLocation
                {
                    Offset = currentOffset,
                    CompressedLength = finalBytes.Length,
                    UncompressedLength = uncompressedLength,
                    RowNumber = _currentRowNumber
                };

                _currentRowNumber++;

                if (_currentRowNumber % _options.IndexSaveFrequency == 0)
                {
                    await SaveIndexAsync();
                }
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public async Task<IDictionary<string, object>> GetRecordAsync(int rowNumber)
        {
            if (!_rowIndex.TryGetValue(rowNumber, out var location))
                return null;

            return await ReadRecordAsync(location);
        }

        public async Task<IList<IDictionary<string, object>>> GetRecordsAsync(IEnumerable<int> rowNumbers)
        {
            var results = new List<IDictionary<string, object>>();
            var locations = new List<(int RowNumber, RecordLocation Location)>();

            foreach (var rowNumber in rowNumbers)
            {
                if (_rowIndex.TryGetValue(rowNumber, out var location))
                    locations.Add((rowNumber, location));
            }

            if (locations.Count == 0)
                return results;

            // Sort by offset for sequential access
            locations.Sort((a, b) => a.Location.Offset.CompareTo(b.Location.Offset));

            using var readStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite, _options.DiskBufferSize);

            foreach (var (_, location) in locations)
            {
                var record = await ReadRecordFromStreamAsync(readStream, location);
                if (record != null)
                    results.Add(record);
            }

            return results;
        }

        public async Task SwitchToReadOnlyModeAsync()
        {
            await _writeLock.WaitAsync();
            try
            {
                if (_isReadOnly) return;

                _dataWriter?.Flush();
                _dataWriter?.Dispose();
                await SaveIndexAsync();
                _isReadOnly = true;

                _logger?.LogInformation("Switched to read-only mode with {Count} records", _currentRowNumber);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        private byte[] CompressData(byte[] data)
        {
            using var output = new MemoryStream();
            using (var gzip = new GZipStream(output, CompressionLevel.Fastest))
            {
                gzip.Write(data, 0, data.Length);
            }
            return output.ToArray();
        }

        private byte[] DecompressData(byte[] compressedData, int uncompressedLength)
        {
            var result = _bufferPool.Rent(uncompressedLength);
            try
            {
                using var input = new MemoryStream(compressedData);
                using var gzip = new GZipStream(input, CompressionMode.Decompress);

                var totalRead = 0;
                while (totalRead < uncompressedLength)
                {
                    var read = gzip.Read(result, totalRead, uncompressedLength - totalRead);
                    if (read == 0) break;
                    totalRead += read;
                }

                var finalResult = new byte[totalRead];
                Array.Copy(result, finalResult, totalRead);
                return finalResult;
            }
            finally
            {
                _bufferPool.Return(result);
            }
        }

        private async Task<IDictionary<string, object>> ReadRecordAsync(RecordLocation location)
        {
            using var readStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return await ReadRecordFromStreamAsync(readStream, location);
        }

        private async Task<IDictionary<string, object>> ReadRecordFromStreamAsync(Stream stream, RecordLocation location)
        {
            try
            {
                stream.Seek(location.Offset, SeekOrigin.Begin);

                var headerBytes = new byte[8];
                await stream.ReadExactlyAsync(headerBytes);

                var uncompressedLength = BitConverter.ToInt32(headerBytes, 0);
                var compressedLength = BitConverter.ToInt32(headerBytes, 4);

                var compressedData = new byte[compressedLength];
                await stream.ReadExactlyAsync(compressedData);

                byte[] jsonBytes;
                if (compressedLength != uncompressedLength)
                {
                    jsonBytes = DecompressData(compressedData, uncompressedLength);
                }
                else
                {
                    jsonBytes = compressedData;
                }

                // Use the custom JSON options for deserialization
                return JsonSerializer.Deserialize<Dictionary<string, object>>(jsonBytes, _jsonOptions);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error reading record at offset {Offset}", location.Offset);
                return null;
            }
        }

        private async Task SaveIndexAsync()
        {
            try
            {
                var indexData = JsonSerializer.SerializeToUtf8Bytes(_rowIndex, _jsonOptions);
                await File.WriteAllBytesAsync(_indexFilePath, indexData);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to save index to disk");
            }
        }

        private static long EstimateRecordSize(IDictionary<string, object> record)
        {
            return record.Sum(kvp => (kvp.Key?.Length ?? 0) + EstimateValueSize(kvp.Value)) + 100; // 100 bytes JSON overhead
        }

        private static long EstimateValueSize(object value) => value switch
        {
            string s => s.Length,
            _ => 20 // Conservative estimate for other types
        };

        public StorageStatistics GetStatistics()
        {
            var dataSize = File.Exists(_dataFilePath) ? new FileInfo(_dataFilePath).Length : 0;
            var indexSize = File.Exists(_indexFilePath) ? new FileInfo(_indexFilePath).Length : 0;

            return new StorageStatistics
            {
                RecordCount = _rowIndex.Count,
                TotalSizeBytes = dataSize + indexSize,
                IsReadOnly = _isReadOnly,
                StorageType = _options.EnableCompression ? "DiskCompressed" : "Disk"
            };
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try
            {
                SaveIndexAsync().Wait(5000);
                _dataWriter?.Dispose();
                _dataFileStream?.Dispose();
                _writeLock?.Dispose();

                // Cleanup temp files
                if (File.Exists(_dataFilePath)) File.Delete(_dataFilePath);
                if (File.Exists(_indexFilePath)) File.Delete(_indexFilePath);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error during disposal");
            }
        }
    }

    public class DictionaryStringObjectJsonConverter : JsonConverter<Dictionary<string, object>>
    {
        public override Dictionary<string, object> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException($"JsonTokenType was of type {reader.TokenType}, only objects are supported");
            }

            var dictionary = new Dictionary<string, object>();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    return dictionary;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException("JsonTokenType was not PropertyName");
                }

                var propertyName = reader.GetString();
                reader.Read();
                dictionary[propertyName] = ExtractValue(ref reader, options);
            }

            throw new JsonException("Expected EndObject token");
        }

        private object ExtractValue(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                    if (reader.TryGetDateTime(out var date))
                    {
                        return date;
                    }
                    if (reader.TryGetGuid(out var guid))
                    {
                        return guid;
                    }
                    return reader.GetString();

                case JsonTokenType.False:
                    return false;

                case JsonTokenType.True:
                    return true;

                case JsonTokenType.Null:
                    return null;

                case JsonTokenType.Number:
                    if (reader.TryGetInt32(out var intValue))
                        return intValue;
                    if (reader.TryGetInt64(out var longValue))
                        return longValue;
                    if (reader.TryGetDouble(out var doubleValue))
                        return doubleValue;
                    if (reader.TryGetDecimal(out var decimalValue))
                        return decimalValue;
                    return reader.GetDouble(); // fallback

                case JsonTokenType.StartObject:
                    return Read(ref reader, typeof(Dictionary<string, object>), options);

                case JsonTokenType.StartArray:
                    var list = new List<object>();
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    {
                        list.Add(ExtractValue(ref reader, options));
                    }
                    return list;

                default:
                    throw new JsonException($"'{reader.TokenType}' is not supported");
            }
        }

        public override void Write(Utf8JsonWriter writer, Dictionary<string, object> value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            foreach (var kvp in value)
            {
                writer.WritePropertyName(kvp.Key);

                if (kvp.Value == null)
                {
                    writer.WriteNullValue();
                }
                else
                {
                    JsonSerializer.Serialize(writer, kvp.Value, kvp.Value.GetType(), options);
                }
            }

            writer.WriteEndObject();
        }
    }

}
