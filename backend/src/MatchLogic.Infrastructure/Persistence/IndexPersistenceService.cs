using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using Mapster;
using MessagePack;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static MatchLogic.Application.Interfaces.LiveSearch.IQGramIndexManager;

namespace MatchLogic.Infrastructure.Persistence
{
    /// <summary>
    /// Persists Q-gram indexes using MessagePack serialization with compression.
    /// Supports chunking for large indexes that exceed MongoDB's 16MB document limit.
    /// </summary>
    public class IndexPersistenceService : IIndexPersistenceService
    {
        private readonly ILogger<IndexPersistenceService> _logger;
        private readonly MessagePackSerializerOptions _serializerOptions;

        // MongoDB document size limit is 16MB, use 15MB chunks to be safe
        private const int CHUNK_SIZE_BYTES = 15 * 1024 * 1024; // 15MB
        private const int MAX_MONGO_DOCUMENT_SIZE = 16 * 1024 * 1024; // 16MB

        public IndexPersistenceService(ILogger<IndexPersistenceService> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            // Configure MessagePack options
            _serializerOptions = MessagePackSerializerOptions.Standard
                .WithCompression(MessagePackCompression.Lz4BlockArray);
        }

        public async Task SaveIndexAsync(
            IDataStore dataStore,
            string collectionName,
            QGramIndexData indexData,
            CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Saving index to {Collection}", collectionName);

            try
            {
                var serializableEntity = ConvertToSerializable(indexData);

                // Serialize to MessagePack binary format
                var uncompressedData = MessagePackSerializer.Serialize(serializableEntity, _serializerOptions);

                // Additional GZip compression for storage
                var compressedData = await CompressAsync(uncompressedData, cancellationToken);

                _logger.LogInformation(
                    "Index serialized: {UncompressedMB:F2} MB → {CompressedMB:F2} MB ({CompressionPercent:F1}% reduction)",
                    uncompressedData.Length / (1024.0 * 1024.0),
                    compressedData.Length / (1024.0 * 1024.0),
                    (1 - (double)compressedData.Length / uncompressedData.Length) * 100);

                // Clear old index data
                await dataStore.DeleteCollection(collectionName);

                // Check if chunking is needed
                if (compressedData.Length > MAX_MONGO_DOCUMENT_SIZE)
                {
                    _logger.LogWarning(
                        "⚠️ Compressed data size ({CompressedMB:F2} MB) exceeds MongoDB limit ({LimitMB} MB). Using chunking strategy.",
                        compressedData.Length / (1024.0 * 1024.0),
                        MAX_MONGO_DOCUMENT_SIZE / (1024.0 * 1024.0));

                    await SaveIndexChunkedAsync(
                        dataStore,
                        collectionName,
                        indexData,
                        compressedData,
                        uncompressedData.Length,
                        cancellationToken);
                }
                else
                {
                    _logger.LogInformation("Index fits within single document, using legacy format.");

                    // Use original single-document format for backward compatibility
                    await SaveIndexLegacyAsync(
                        dataStore,
                        collectionName,
                        indexData,
                        compressedData,
                        uncompressedData.Length,
                        cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving index to {Collection}", collectionName);
                throw;
            }
        }

        /// <summary>
        /// Save index using chunking strategy for large indexes
        /// </summary>
        private async Task SaveIndexChunkedAsync(
            IDataStore dataStore,
            string collectionName,
            QGramIndexData indexData,
            byte[] compressedData,
            long uncompressedSize,
            CancellationToken cancellationToken)
        {
            // Calculate number of chunks needed
            int totalChunks = (int)Math.Ceiling((double)compressedData.Length / CHUNK_SIZE_BYTES);

            _logger.LogInformation(
                "Splitting index into {TotalChunks} chunks of ~{ChunkSizeMB:F2} MB each",
                totalChunks,
                CHUNK_SIZE_BYTES / (1024.0 * 1024.0));

            var chunks = new List<IndexChunkDocument>();

            for (int i = 0; i < totalChunks; i++)
            {
                int offset = i * CHUNK_SIZE_BYTES;
                int chunkSize = Math.Min(CHUNK_SIZE_BYTES, compressedData.Length - offset);

                // Extract chunk data
                byte[] chunkData = new byte[chunkSize];
                Array.Copy(compressedData, offset, chunkData, 0, chunkSize);

                var chunk = new IndexChunkDocument
                {
                    Id = Guid.NewGuid(),
                    IndexId = indexData.IndexId,
                    ProjectId = indexData.ProjectId,
                    ChunkNumber = i,
                    TotalChunks = totalChunks,
                    ChunkData = chunkData,
                    ChunkSizeBytes = chunkSize,

                    // Metadata only in first chunk
                    UncompressedSizeBytes = (i == 0) ? uncompressedSize : 0,
                    CompressedSizeBytes = (i == 0) ? compressedData.Length : 0,
                    TotalRecords = (i == 0) ? indexData.TotalRecords : 0,
                    TotalFields = (i == 0) ? indexData.GlobalFieldIndex.Count : 0,
                    DataSourceCount = (i == 0) ? indexData.DataSourceStats.Count : 0,
                    CreatedAt = (i == 0) ? indexData.CreatedAt : default,
                    SavedAt = DateTime.UtcNow,
                    IsChunked = true
                };

                chunks.Add(chunk);

                _logger.LogInformation(
                    "Created chunk {ChunkNumber}/{TotalChunks}: {ChunkSizeMB:F2} MB",
                    i + 1,
                    totalChunks,
                    chunkSize / (1024.0 * 1024.0));
            }

            // Save all chunks
            foreach (var chunk in chunks)
            {
                await dataStore.InsertAsync<IndexChunkDocument>(chunk, collectionName);
            }

            _logger.LogInformation(
                "✅ Index saved successfully as {TotalChunks} chunks: {UncompressedMB:F2} MB → {CompressedMB:F2} MB",
                totalChunks,
                uncompressedSize / (1024.0 * 1024.0),
                compressedData.Length / (1024.0 * 1024.0));
        }

        /// <summary>
        /// Save index using legacy single-document format
        /// </summary>
        private async Task SaveIndexLegacyAsync(
            IDataStore dataStore,
            string collectionName,
            QGramIndexData indexData,
            byte[] compressedData,
            long uncompressedSize,
            CancellationToken cancellationToken)
        {
            var document = new IndexStorageDocument
            {
                Id = Guid.NewGuid(),
                IndexId = indexData.IndexId,
                ProjectId = indexData.ProjectId,
                CompressedData = compressedData,
                UncompressedSizeBytes = uncompressedSize,
                CompressedSizeBytes = compressedData.Length,
                CompressionRatio = (double)compressedData.Length / uncompressedSize,
                TotalRecords = indexData.TotalRecords,
                TotalFields = indexData.GlobalFieldIndex.Count,
                DataSourceCount = indexData.DataSourceStats.Count,
                CreatedAt = indexData.CreatedAt,
                SavedAt = DateTime.UtcNow
            };

            await dataStore.InsertAsync<IndexStorageDocument>(document, collectionName);

            _logger.LogInformation(
                "✅ Index saved successfully: {UncompressedMB:F2} MB → {CompressedMB:F2} MB ({CompressionPercent:F1}% reduction)",
                document.UncompressedSizeMB,
                document.CompressedSizeMB,
                (1 - document.CompressionRatio) * 100);
        }

        public async Task<QGramIndexData> LoadIndexAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Loading index from {Collection}", collectionName);

            try
            {
                // Try to load as chunked format first
                var chunks = (await dataStore.GetAllAsync<IndexChunkDocument>(collectionName)).ToList();

                if (chunks.Any() && chunks.All(c => c.IsChunked))
                {
                    _logger.LogInformation("Detected chunked index format with {ChunkCount} chunks", chunks.Count);
                    return await LoadIndexChunkedAsync(chunks, cancellationToken);
                }

                // Fall back to legacy single-document format
                _logger.LogInformation("Attempting to load legacy single-document format");
                var document = (await dataStore.GetAllAsync<IndexStorageDocument>(collectionName)).FirstOrDefault();

                if (document == null)
                {
                    _logger.LogWarning("No index found in collection {Collection}", collectionName);
                    return null;
                }

                return await LoadIndexLegacyAsync(document, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading index from {Collection}", collectionName);
                throw;
            }
        }

        /// <summary>
        /// Load index from chunked format
        /// </summary>
        private async Task<QGramIndexData> LoadIndexChunkedAsync(
            List<IndexChunkDocument> chunks,
            CancellationToken cancellationToken)
        {
            // Validate chunks
            if (chunks == null || !chunks.Any())
            {
                throw new InvalidOperationException("No chunks found");
            }

            // Sort chunks by chunk number
            var sortedChunks = chunks.OrderBy(c => c.ChunkNumber).ToList();

            // Validate chunk sequence
            int expectedTotalChunks = sortedChunks[0].TotalChunks;
            if (sortedChunks.Count != expectedTotalChunks)
            {
                throw new InvalidOperationException(
                    $"Chunk count mismatch: expected {expectedTotalChunks}, found {sortedChunks.Count}");
            }

            for (int i = 0; i < sortedChunks.Count; i++)
            {
                if (sortedChunks[i].ChunkNumber != i)
                {
                    throw new InvalidOperationException(
                        $"Missing chunk {i} in sequence");
                }
            }

            // Get metadata from first chunk
            var firstChunk = sortedChunks[0];
            long totalCompressedSize = sortedChunks.Sum(c => c.ChunkSizeBytes);

            _logger.LogInformation(
                "Reconstructing index from {ChunkCount} chunks: {CompressedMB:F2} MB → {UncompressedMB:F2} MB",
                sortedChunks.Count,
                totalCompressedSize / (1024.0 * 1024.0),
                firstChunk.UncompressedSizeBytes / (1024.0 * 1024.0));

            // Reconstruct compressed data by concatenating chunks
            byte[] compressedData = new byte[totalCompressedSize];
            int offset = 0;

            foreach (var chunk in sortedChunks)
            {
                Array.Copy(chunk.ChunkData, 0, compressedData, offset, chunk.ChunkSizeBytes);
                offset += chunk.ChunkSizeBytes;

                _logger.LogInformation(
                    "Loaded chunk {ChunkNumber}/{TotalChunks}: {ChunkSizeMB:F2} MB",
                    chunk.ChunkNumber + 1,
                    chunk.TotalChunks,
                    chunk.ChunkSizeBytes / (1024.0 * 1024.0));
            }

            // Decompress
            var uncompressedData = await DecompressAsync(compressedData, cancellationToken);

            // Deserialize from MessagePack
            var serializedIndexData = MessagePackSerializer.Deserialize<SerializableQGramIndexData>(
                uncompressedData,
                _serializerOptions);

            var indexData = ConvertFromSerializable(serializedIndexData);

            _logger.LogInformation(
                "✅ Index loaded successfully from chunks: {Records:N0} records, {Fields} fields",
                indexData.TotalRecords,
                indexData.GlobalFieldIndex.Count);

            return indexData;
        }

        /// <summary>
        /// Load index from legacy single-document format
        /// </summary>
        private async Task<QGramIndexData> LoadIndexLegacyAsync(
            IndexStorageDocument document,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Decompressing index: {CompressedMB:F2} MB → {UncompressedMB:F2} MB",
                document.CompressedSizeMB,
                document.UncompressedSizeMB);

            // Decompress
            var uncompressedData = await DecompressAsync(document.CompressedData, cancellationToken);

            // Deserialize from MessagePack
            var serializedIndexData = MessagePackSerializer.Deserialize<SerializableQGramIndexData>(
                uncompressedData,
                _serializerOptions);

            var indexData = ConvertFromSerializable(serializedIndexData);

            _logger.LogInformation(
                "✅ Index loaded successfully: {Records:N0} records, {Fields} fields",
                indexData.TotalRecords,
                indexData.GlobalFieldIndex.Count);

            return indexData;
        }

        public async Task<bool> IndexExistsAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Check for chunked format
                var chunks = await dataStore.GetAllAsync<IndexChunkDocument>(collectionName);
                if (chunks.Any())
                {
                    return true;
                }

                // Check for legacy format
                var documents = await dataStore.GetAllAsync<IndexStorageDocument>(collectionName);
                return documents.Any();
            }
            catch
            {
                return false;
            }
        }

        public async Task<IndexMetadata> GetIndexMetadataAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default)
        {
            _logger.LogDebug("Getting index metadata from {Collection}", collectionName);

            try
            {
                // Try chunked format first
                var chunks = (await dataStore.GetAllAsync<IndexChunkDocument>(collectionName)).ToList();

                if (chunks.Any() && chunks.All(c => c.IsChunked))
                {
                    // Get metadata from first chunk
                    var firstChunk = chunks.OrderBy(c => c.ChunkNumber).First();
                    long totalCompressedSize = chunks.Sum(c => c.ChunkSizeBytes);

                    return new IndexMetadata
                    {
                        IndexId = firstChunk.IndexId,
                        ProjectId = firstChunk.ProjectId,
                        TotalRecords = firstChunk.TotalRecords,
                        TotalFields = firstChunk.TotalFields,
                        DataSourceCount = firstChunk.DataSourceCount,
                        CompressedSizeBytes = totalCompressedSize,
                        UncompressedSizeBytes = firstChunk.UncompressedSizeBytes,
                        CompressionRatio = (double)totalCompressedSize / firstChunk.UncompressedSizeBytes,
                        CreatedAt = firstChunk.CreatedAt,
                        SavedAt = firstChunk.SavedAt
                    };
                }

                // Fall back to legacy format
                var document = (await dataStore.GetAllAsync<IndexStorageDocument>(collectionName)).FirstOrDefault();

                if (document == null)
                {
                    _logger.LogWarning("No index found in collection {Collection}", collectionName);
                    return null;
                }

                return new IndexMetadata
                {
                    IndexId = document.IndexId,
                    ProjectId = document.ProjectId,
                    TotalRecords = document.TotalRecords,
                    TotalFields = document.TotalFields,
                    DataSourceCount = document.DataSourceCount,
                    CompressedSizeBytes = document.CompressedSizeBytes,
                    UncompressedSizeBytes = document.UncompressedSizeBytes,
                    CompressionRatio = document.CompressionRatio,
                    CreatedAt = document.CreatedAt,
                    SavedAt = document.SavedAt
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting index metadata from {Collection}", collectionName);
                throw;
            }
        }

        public async Task DeleteIndexAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Deleting index from {Collection}", collectionName);

            try
            {
                await dataStore.DeleteCollection(collectionName);
                _logger.LogInformation("Index deleted successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting index from {Collection}", collectionName);
                throw;
            }
        }

        #region Compression Helpers

        private async Task<byte[]> CompressAsync(byte[] data, CancellationToken cancellationToken)
        {
            using var outputStream = new MemoryStream();
            using (var gzipStream = new GZipStream(outputStream, CompressionLevel.Optimal))
            {
                await gzipStream.WriteAsync(data, 0, data.Length, cancellationToken);
            }
            return outputStream.ToArray();
        }

        private async Task<byte[]> DecompressAsync(byte[] compressedData, CancellationToken cancellationToken)
        {
            using var inputStream = new MemoryStream(compressedData);
            using var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
            using var outputStream = new MemoryStream();
            await gzipStream.CopyToAsync(outputStream, 81920, cancellationToken);
            return outputStream.ToArray();
        }

        #endregion

        #region Conversion Helpers

        /// <summary>
        /// Manually convert QGramIndexData to SerializableQGramIndexData
        /// Handles complex dictionary key conversions
        /// </summary>
        private SerializableQGramIndexData ConvertToSerializable(QGramIndexData indexData)
        {
            var serializable = new SerializableQGramIndexData
            {
                IndexId = indexData.IndexId,
                ProjectId = indexData.ProjectId,
                CreatedAt = indexData.CreatedAt,
                LoadedAt = indexData.LoadedAt,
                TotalRecords = indexData.TotalRecords,
                GlobalFieldIndex = new Dictionary<string, Dictionary<uint, List<SerializeablePostingEntry>>>(),
                RowMetadata = new Dictionary<(Guid, int), SerializableRowMetadataLight>(),
                DataSourceStats = new Dictionary<Guid, SerializableDataSourceStats>()
            };

            // Convert GlobalFieldIndex
            foreach (var field in indexData.GlobalFieldIndex)
            {
                var qgramDict = new Dictionary<uint, List<SerializeablePostingEntry>>();

                foreach (var qgramEntry in field.Value)
                {
                    var postings = qgramEntry.Value
                        .Select(tuple => new SerializeablePostingEntry(tuple.DataSourceId, tuple.RowNumber))
                        .ToList();

                    qgramDict[qgramEntry.Key] = postings;
                }

                serializable.GlobalFieldIndex[field.Key] = qgramDict;
            }

            // Convert RowMetadata - Tuple key directly
            foreach (var row in indexData.RowMetadata)
            {
                // Use tuple key directly
                var key = row.Key;

                serializable.RowMetadata[key] = new SerializableRowMetadataLight
                {
                    DataSourceId = row.Value.DataSourceId,
                    RowNumber = row.Value.RowNumber,
                    FieldHashes = new Dictionary<string, HashSet<uint>>(row.Value.FieldHashes),
                    BlockingValues = new Dictionary<string, string>(row.Value.BlockingValues)
                };
            }

            // Convert DataSourceStats
            foreach (var stat in indexData.DataSourceStats)
            {
                serializable.DataSourceStats[stat.Key] = new SerializableDataSourceStats
                {
                    DataSourceId = stat.Value.DataSourceId,
                    RecordCount = stat.Value.RecordCount,
                    IndexedFields = stat.Value.IndexedFields?.ToList() ?? new List<string>()
                };
            }

            return serializable;
        }

        /// <summary>
        /// Manually convert SerializableQGramIndexData back to QGramIndexData
        /// Handles complex dictionary key conversions
        /// </summary>
        private QGramIndexData ConvertFromSerializable(SerializableQGramIndexData serializable)
        {
            _logger.LogInformation(
                "Converting from serializable: {Records} records, {Fields} fields in serialized data",
                serializable.TotalRecords,
                serializable.GlobalFieldIndex?.Count ?? 0);

            var indexData = new QGramIndexData
            {
                IndexId = serializable.IndexId,
                ProjectId = serializable.ProjectId,
                CreatedAt = serializable.CreatedAt,
                LoadedAt = serializable.LoadedAt,
                TotalRecords = serializable.TotalRecords,
                GlobalFieldIndex = new Dictionary<string, Dictionary<uint, List<PostingEntry>>>(),
                RowMetadata = new Dictionary<(Guid DataSourceId, int RowNumber), RowMetadataLight>(),
                DataSourceStats = new Dictionary<Guid, DataSourceStats>()
            };

            // ✅ FIX: Iterate over SERIALIZABLE data, not the empty indexData
            if (serializable.GlobalFieldIndex != null)
            {
                foreach (var field in serializable.GlobalFieldIndex)
                {
                    var qgramDict = new Dictionary<uint, List<PostingEntry>>();

                    foreach (var qgramEntry in field.Value)
                    {
                        var postings = qgramEntry.Value
                            .Select(p => new PostingEntry(p.DataSourceId, p.RowNumber))
                            .ToList();

                        qgramDict[qgramEntry.Key] = postings;
                    }

                    indexData.GlobalFieldIndex[field.Key] = qgramDict;
                }
            }

            _logger.LogInformation(
                "Converted GlobalFieldIndex: {Count} fields",
                indexData.GlobalFieldIndex.Count);

            // Convert RowMetadata - tuple key
            if (serializable.RowMetadata != null)
            {
                foreach (var row in serializable.RowMetadata)
                {
                    var tupleKey = (DataSourceId: row.Key.Item1, RowNumber: row.Key.Item2);

                    indexData.RowMetadata[tupleKey] = new RowMetadataLight
                    {
                        DataSourceId = row.Value.DataSourceId,
                        RowNumber = row.Value.RowNumber,
                        FieldHashes = row.Value.FieldHashes != null
                            ? new Dictionary<string, HashSet<uint>>(row.Value.FieldHashes)
                            : new Dictionary<string, HashSet<uint>>(),
                        BlockingValues = row.Value.BlockingValues != null
                            ? new Dictionary<string, string>(row.Value.BlockingValues)
                            : new Dictionary<string, string>()
                    };
                }
            }

            _logger.LogInformation(
                "Converted RowMetadata: {Count} rows",
                indexData.RowMetadata.Count);

            // Convert DataSourceStats
            if (serializable.DataSourceStats != null)
            {
                foreach (var stat in serializable.DataSourceStats)
                {
                    indexData.DataSourceStats[stat.Key] = new DataSourceStats
                    {
                        DataSourceId = stat.Value.DataSourceId,
                        RecordCount = stat.Value.RecordCount,
                        IndexedFields = stat.Value.IndexedFields?.ToList() ?? new List<string>()
                    };
                }
            }

            _logger.LogInformation(
                "✅ Conversion complete: {Records} records, {Fields} fields, {Stats} data sources",
                indexData.TotalRecords,
                indexData.GlobalFieldIndex.Count,
                indexData.DataSourceStats.Count);

            return indexData;
        }

        #endregion
    }

    /// <summary>
    /// Document stored in database (metadata + compressed binary data)
    /// Legacy format for indexes smaller than 16MB
    /// </summary>
    internal class IndexStorageDocument
    {
        public Guid Id { get; set; }
        public Guid IndexId { get; set; }
        public Guid ProjectId { get; set; }
        public byte[] CompressedData { get; set; }
        public long UncompressedSizeBytes { get; set; }
        public long CompressedSizeBytes { get; set; }
        public double CompressionRatio { get; set; }
        public int TotalRecords { get; set; }
        public int TotalFields { get; set; }
        public int DataSourceCount { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime SavedAt { get; set; }

        public double UncompressedSizeMB => UncompressedSizeBytes / (1024.0 * 1024.0);
        public double CompressedSizeMB => CompressedSizeBytes / (1024.0 * 1024.0);
    }

    /// <summary>
    /// Chunk document for storing large indexes that exceed MongoDB's 16MB limit
    /// </summary>
    internal class IndexChunkDocument
    {
        public Guid Id { get; set; }
        public Guid IndexId { get; set; }
        public Guid ProjectId { get; set; }

        /// <summary>
        /// Chunk sequence number (0-based)
        /// </summary>
        public int ChunkNumber { get; set; }

        /// <summary>
        /// Total number of chunks for this index
        /// </summary>
        public int TotalChunks { get; set; }

        /// <summary>
        /// Chunk data (15MB max)
        /// </summary>
        public byte[] ChunkData { get; set; }

        /// <summary>
        /// Size of this chunk in bytes
        /// </summary>
        public int ChunkSizeBytes { get; set; }

        /// <summary>
        /// Flag to identify chunked format
        /// </summary>
        public bool IsChunked { get; set; }

        // Metadata (only populated in chunk 0)
        public long UncompressedSizeBytes { get; set; }
        public long CompressedSizeBytes { get; set; }
        public int TotalRecords { get; set; }
        public int TotalFields { get; set; }
        public int DataSourceCount { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime SavedAt { get; set; }

        public double ChunkSizeMB => ChunkSizeBytes / (1024.0 * 1024.0);
    }

    [MessagePackObject]
    public class SerializableQGramIndexData
    {
        [Key(0)]
        public Guid IndexId { get; set; }

        [Key(1)]
        public Guid ProjectId { get; set; }

        [Key(2)]
        public DateTime CreatedAt { get; set; }

        [Key(3)]
        public DateTime LoadedAt { get; set; }

        [Key(4)]
        public int TotalRecords { get; set; }

        /// <summary>
        /// Inverted index: Field → QGram → List of (DataSourceId, RowNumber)
        /// Uses Dictionary for MessagePack compatibility
        /// </summary>
        [Key(5)]
        public Dictionary<string, Dictionary<uint, List<SerializeablePostingEntry>>> GlobalFieldIndex { get; set; }

        /// <summary>
        /// Row metadata: (DataSourceId, RowNumber) → Metadata
        /// Stored as Dictionary with tuple key for MessagePack compatibility
        /// </summary>
        [Key(6)]
        public Dictionary<(Guid, int), SerializableRowMetadataLight> RowMetadata { get; set; }

        /// <summary>
        /// Statistics per data source
        /// </summary>
        [Key(7)]
        public Dictionary<Guid, SerializableDataSourceStats> DataSourceStats { get; set; }
    }

    [MessagePackObject]
    public class SerializeablePostingEntry
    {
        [Key(0)]
        public Guid DataSourceId { get; set; }

        [Key(1)]
        public int RowNumber { get; set; }

        public SerializeablePostingEntry() { }

        public SerializeablePostingEntry(Guid dataSourceId, int rowNumber)
        {
            DataSourceId = dataSourceId;
            RowNumber = rowNumber;
        }

        // Deconstruct for tuple-like usage
        public void Deconstruct(out Guid dataSourceId, out int rowNumber)
        {
            dataSourceId = DataSourceId;
            rowNumber = RowNumber;
        }
    }

    [MessagePackObject]
    public class SerializableRowMetadataLight
    {
        [Key(0)]
        public Guid DataSourceId { get; set; }

        [Key(1)]
        public int RowNumber { get; set; }

        /// <summary>
        /// Q-gram hashes per field
        /// Field name → Set of q-gram hashes
        /// </summary>
        [Key(2)]
        public Dictionary<string, HashSet<uint>> FieldHashes { get; set; }

        /// <summary>
        /// Blocking values for exact match filtering
        /// Field name → Normalized value
        /// </summary>
        [Key(3)]
        public Dictionary<string, string> BlockingValues { get; set; }

        public SerializableRowMetadataLight()
        {
            FieldHashes = new Dictionary<string, HashSet<uint>>();
            BlockingValues = new Dictionary<string, string>();
        }
    }

    [MessagePackObject]
    public class SerializableDataSourceStats
    {
        [Key(0)]
        public Guid DataSourceId { get; set; }

        [Key(1)]
        public int RecordCount { get; set; }

        [Key(2)]
        public List<string> IndexedFields { get; set; }

        public SerializableDataSourceStats()
        {
            IndexedFields = new List<string>();
        }
    }
}