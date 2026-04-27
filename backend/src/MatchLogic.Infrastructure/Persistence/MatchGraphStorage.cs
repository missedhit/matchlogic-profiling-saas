using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Features.DataMatching.Storage;
using MatchLogic.Application.Interfaces.Persistence;
using LiteDB;
using MessagePack;
using Microsoft.Extensions.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence
{
    /// <summary>
    /// Optimized storage for MatchGraph using MessagePack serialization with GZip compression.
    /// Handles LiteDB-specific types by sanitizing data before serialization.
    /// </summary>
    public class MatchGraphStorage : IMatchGraphStorage
    {
        private readonly ILogger<MatchGraphStorage> _logger;
        private const string GRAPH_DOCUMENT_ID = "match_graph";

        public MatchGraphStorage(ILogger<MatchGraphStorage> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task SaveMatchGraphAsync(
            IDataStore dataStore,
            string collectionName,
            MatchGraphDME graph,
            CancellationToken cancellationToken = default)
        {
            if (dataStore == null) throw new ArgumentNullException(nameof(dataStore));
            if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("Collection name cannot be empty", nameof(collectionName));
            if (graph == null) throw new ArgumentNullException(nameof(graph));

            _logger.LogInformation(
                "Serializing match graph with {Nodes} nodes and {Edges} edges to collection {Collection}",
                graph.TotalNodes, graph.TotalEdges, collectionName);

            try
            {
                // Convert to serializable model (with data sanitization)
                var serializableGraph = ConvertToSerializable(graph);

                // Serialize with MessagePack
                var msgPackBytes = MessagePackSerializer.Serialize(serializableGraph);
                _logger.LogInformation("MessagePack serialization: {Size:N0} bytes", msgPackBytes.Length);

                // Compress with GZip
                var compressedBytes = CompressBytes(msgPackBytes);
                _logger.LogInformation(
                    "Compressed size: {Size:N0} bytes ({Reduction:F1}% reduction)",
                    compressedBytes.Length,
                    (1 - (double)compressedBytes.Length / msgPackBytes.Length) * 100);

                // Create storage document
                var document = new MatchGraphDocument
                {
                    Id = GRAPH_DOCUMENT_ID,
                    Data = compressedBytes,
                    GraphId = graph.GraphId,
                    ProjectId = graph.ProjectId,
                    TotalNodes = graph.TotalNodes,
                    TotalEdges = graph.TotalEdges,
                    IsCompressed = true,
                    CompressionType = "gzip",
                    SerializationFormat = "messagepack",
                    UncompressedSizeBytes = msgPackBytes.Length,
                    CompressedSizeBytes = compressedBytes.Length,
                    CreatedAt = graph.CreatedAt,
                    SavedAt = DateTime.UtcNow
                };

                // Save to datastore
                await dataStore.InsertAsync(document, collectionName);

                _logger.LogInformation(
                    "Successfully saved match graph to {Collection}. Final size: {Size:N0} bytes",
                    collectionName, compressedBytes.Length);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving match graph to {Collection}", collectionName);
                throw;
            }
        }

        public async Task<MatchGraphDME> LoadMatchGraphAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default)
        {
            if (dataStore == null) throw new ArgumentNullException(nameof(dataStore));
            if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("Collection name cannot be empty", nameof(collectionName));

            _logger.LogInformation("Loading match graph from collection {Collection}", collectionName);

            try
            {
                var document = await dataStore.GetByIdAsync<MatchGraphDocument, string>(
                    GRAPH_DOCUMENT_ID,
                    collectionName);

                if (document == null)
                {
                    _logger.LogWarning("No match graph found in collection {Collection}", collectionName);
                    return null;
                }

                _logger.LogInformation(
                    "Found graph document: {Nodes} nodes, {Edges} edges, compressed size: {Size:N0} bytes",
                    document.TotalNodes, document.TotalEdges, document.CompressedSizeBytes);

                byte[] msgPackBytes;
                if (document.IsCompressed)
                {
                    msgPackBytes = DecompressBytes(document.Data);
                    _logger.LogInformation("Decompressed to {Size:N0} bytes", msgPackBytes.Length);
                }
                else
                {
                    msgPackBytes = document.Data;
                }

                var serializableGraph = MessagePackSerializer.Deserialize<SerializableMatchGraph>(msgPackBytes);
                var graph = ConvertFromSerializable(serializableGraph);

                _logger.LogInformation(
                    "Successfully loaded match graph: {Nodes} nodes, {Edges} edges",
                    graph.TotalNodes, graph.TotalEdges);

                return graph;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading match graph from {Collection}", collectionName);
                throw;
            }
        }

        public async Task<bool> GraphExistsAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var document = await dataStore.GetByIdAsync<MatchGraphDocument, string>(
                    GRAPH_DOCUMENT_ID,
                    collectionName);

                return document != null;
            }
            catch
            {
                return false;
            }
        }

        public async Task<MatchGraphMetadata> GetGraphMetadataAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default)
        {
            var document = await dataStore.GetByIdAsync<MatchGraphDocument, string>(
                GRAPH_DOCUMENT_ID,
                collectionName);

            if (document == null)
                return null;

            return new MatchGraphMetadata
            {
                GraphId = document.GraphId,
                ProjectId = document.ProjectId,
                TotalNodes = document.TotalNodes,
                TotalEdges = document.TotalEdges,
                CompressedSizeBytes = document.CompressedSizeBytes,
                UncompressedSizeBytes = document.UncompressedSizeBytes,
                CompressionRatio = (double)document.CompressedSizeBytes / document.UncompressedSizeBytes,
                CreatedAt = document.CreatedAt,
                SavedAt = document.SavedAt
            };
        }

        #region Data Sanitization - Handles LiteDB Types

        /// <summary>
        /// Sanitizes a dictionary to remove LiteDB-specific types that MessagePack can't serialize.
        /// Converts ObjectId, BsonValue, etc. to primitive types.
        /// </summary>
        private Dictionary<string, object> SanitizeRecordData(Dictionary<string, object> data)
        {
            if (data == null)
                return new Dictionary<string, object>();

            var sanitized = new Dictionary<string, object>(data.Count);
            foreach (var kvp in data)
            {
                sanitized[kvp.Key] = SanitizeValue(kvp.Value);
            }
            return sanitized;
        }

        /// <summary>
        /// Recursively sanitizes a value, converting LiteDB types to serializable primitives.
        /// </summary>
        private object SanitizeValue(object value)
        {
            return value switch
            {
                null => null,

                // LiteDB specific types - convert to strings
                ObjectId oid => oid.ToString(),
                BsonValue bson => ConvertBsonValue(bson),

                // Primitive types - pass through
                string s => s,
                bool b => b,
                int i => i,
                long l => l,
                float f => f,
                double d => d,
                decimal dec => (double)dec, // MessagePack handles double better
                DateTime dt => dt,
                Guid g => g,

                // Collections - recursively sanitize
                IDictionary<string, object> dict => dict.ToDictionary(
                    k => k.Key,
                    k => SanitizeValue(k.Value)),

                IDictionary dict => SanitizeDictionary(dict),

                IEnumerable<object> list => list.Select(SanitizeValue).ToList(),

                IEnumerable enumerable when !(enumerable is string) =>
                    SanitizeEnumerable(enumerable),

                // Fallback - convert to string
                _ => value.ToString()
            };
        }

        private object ConvertBsonValue(BsonValue bson)
        {
            return bson.Type switch
            {
                BsonType.Null => null,
                BsonType.Int32 => bson.AsInt32,
                BsonType.Int64 => bson.AsInt64,
                BsonType.Double => bson.AsDouble,
                BsonType.Decimal => (double)bson.AsDecimal,
                BsonType.String => bson.AsString,
                BsonType.Boolean => bson.AsBoolean,
                BsonType.DateTime => bson.AsDateTime,
                BsonType.Guid => bson.AsGuid,
                BsonType.ObjectId => bson.AsObjectId.ToString(),
                BsonType.Array => bson.AsArray.Select(v => ConvertBsonValue(v)).ToList(),
                BsonType.Document => bson.AsDocument.ToDictionary(
                    k => k.Key,
                    k => ConvertBsonValue(k.Value)),
                _ => bson.ToString()
            };
        }

        private Dictionary<string, object> SanitizeDictionary(IDictionary dict)
        {
            var result = new Dictionary<string, object>();
            foreach (DictionaryEntry entry in dict)
            {
                var key = entry.Key?.ToString() ?? "null";
                result[key] = SanitizeValue(entry.Value);
            }
            return result;
        }

        private List<object> SanitizeEnumerable(IEnumerable enumerable)
        {
            var result = new List<object>();
            foreach (var item in enumerable)
            {
                result.Add(SanitizeValue(item));
            }
            return result;
        }

        #endregion

        #region Conversion Methods

        private SerializableMatchGraph ConvertToSerializable(MatchGraphDME graph)
        {
            var serializable = new SerializableMatchGraph
            {
                GraphId = graph.GraphId,
                ProjectId = graph.ProjectId,
                CreatedAt = graph.CreatedAt,
                AdjacencyList = new Dictionary<string, List<string>>(),
                EdgeDetails = new Dictionary<string, SerializableEdgeDetails>(),
                NodeMetadata = new Dictionary<string, SerializableNodeMetadata>()
            };

            // Convert adjacency list
            foreach (var kvp in graph.AdjacencyList)
            {
                var key = RecordKeyToString(kvp.Key);
                var neighbors = kvp.Value.Select(RecordKeyToString).ToList();
                serializable.AdjacencyList[key] = neighbors;
            }

            // Convert edge details
            foreach (var kvp in graph.EdgeDetails)
            {
                var key = $"{RecordKeyToString(kvp.Key.Item1)}|{RecordKeyToString(kvp.Key.Item2)}";
                serializable.EdgeDetails[key] = new SerializableEdgeDetails
                {
                    PairId = kvp.Value.PairId,
                    MaxScore = kvp.Value.MaxScore,
                    MatchDefinitionIndices = kvp.Value.MatchDefinitionIndices,
                    MatchedAt = kvp.Value.MatchedAt
                };
            }

            // Convert node metadata WITH SANITIZATION
            foreach (var kvp in graph.NodeMetadata)
            {
                var key = RecordKeyToString(kvp.Key);
                serializable.NodeMetadata[key] = new SerializableNodeMetadata
                {
                    DataSourceId = kvp.Value.RecordKey.DataSourceId,
                    RowNumber = kvp.Value.RecordKey.RowNumber,
                    RecordData = SanitizeRecordData(kvp.Value.RecordData), // <-- KEY FIX
                    FirstSeenAt = kvp.Value.FirstSeenAt,
                    DegreeCount = kvp.Value.DegreeCount,
                    ParticipatingDefinitions = kvp.Value.ParticipatingDefinitions?.ToList()
                };
            }

            return serializable;
        }

        private MatchGraphDME ConvertFromSerializable(SerializableMatchGraph serializable)
        {
            var graph = new MatchGraphDME(serializable.ProjectId, serializable.AdjacencyList.Count)
            {
                GraphId = serializable.GraphId,
                CreatedAt = serializable.CreatedAt
            };

            // Restore node metadata first
            foreach (var kvp in serializable.NodeMetadata)
            {
                var recordKey = StringToRecordKey(kvp.Key);
                graph.NodeMetadata[recordKey] = new NodeMetadata
                {
                    RecordKey = recordKey,
                    RecordData = kvp.Value.RecordData ?? new Dictionary<string, object>(),
                    FirstSeenAt = kvp.Value.FirstSeenAt,
                    DegreeCount = kvp.Value.DegreeCount,
                    ParticipatingDefinitions = kvp.Value.ParticipatingDefinitions?.ToHashSet() ?? new HashSet<int>()
                };
            }

            // Restore adjacency list
            foreach (var kvp in serializable.AdjacencyList)
            {
                var key = StringToRecordKey(kvp.Key);
                var neighbors = kvp.Value.Select(StringToRecordKey).ToHashSet();
                graph.AdjacencyList[key] = neighbors;
            }

            // Restore edge details
            foreach (var kvp in serializable.EdgeDetails)
            {
                var keys = kvp.Key.Split('|');
                var key1 = StringToRecordKey(keys[0]);
                var key2 = StringToRecordKey(keys[1]);

                var edgeKey = (key1, key2);
                graph.EdgeDetails[edgeKey] = new MatchEdgeDetails
                {
                    PairId = kvp.Value.PairId,
                    MaxScore = kvp.Value.MaxScore,
                    MatchDefinitionIndices = kvp.Value.MatchDefinitionIndices ?? new List<int>(),
                    MatchedAt = kvp.Value.MatchedAt,
                    ScoresByDefinition = new Dictionary<int, MatchScoreDetail>()
                };
            }

            return graph;
        }

        private string RecordKeyToString(RecordKey key) => $"{key.DataSourceId}:{key.RowNumber}";

        private RecordKey StringToRecordKey(string str)
        {
            var parts = str.Split(':');
            return new RecordKey(Guid.Parse(parts[0]), int.Parse(parts[1]));
        }

        private byte[] CompressBytes(byte[] data)
        {
            using var outputStream = new MemoryStream();
            using (var gzipStream = new GZipStream(outputStream, CompressionLevel.Optimal))
            {
                gzipStream.Write(data, 0, data.Length);
            }
            return outputStream.ToArray();
        }

        private byte[] DecompressBytes(byte[] data)
        {
            using var inputStream = new MemoryStream(data);
            using var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
            using var outputStream = new MemoryStream();
            gzipStream.CopyTo(outputStream);
            return outputStream.ToArray();
        }

        #endregion
    }

    #region Internal Storage Models (Infrastructure only)

    internal class MatchGraphDocument
    {
        public string Id { get; set; }
        public byte[] Data { get; set; }
        public Guid GraphId { get; set; }
        public Guid ProjectId { get; set; }
        public int TotalNodes { get; set; }
        public int TotalEdges { get; set; }
        public bool IsCompressed { get; set; }
        public string CompressionType { get; set; }
        public string SerializationFormat { get; set; }
        public long UncompressedSizeBytes { get; set; }
        public long CompressedSizeBytes { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime SavedAt { get; set; }
    }

    #endregion

    #region MessagePack Models (Infrastructure only)

    [MessagePackObject]
    internal class SerializableMatchGraph
    {
        [Key(0)] public Guid GraphId { get; set; }
        [Key(1)] public Guid ProjectId { get; set; }
        [Key(2)] public DateTime CreatedAt { get; set; }
        [Key(3)] public Dictionary<string, List<string>> AdjacencyList { get; set; }
        [Key(4)] public Dictionary<string, SerializableEdgeDetails> EdgeDetails { get; set; }
        [Key(5)] public Dictionary<string, SerializableNodeMetadata> NodeMetadata { get; set; }
    }

    [MessagePackObject]
    internal class SerializableEdgeDetails
    {
        [Key(0)] public long PairId { get; set; }
        [Key(1)] public double MaxScore { get; set; }
        [Key(2)] public List<int> MatchDefinitionIndices { get; set; }
        [Key(3)] public DateTime MatchedAt { get; set; }
    }

    [MessagePackObject]
    internal class SerializableNodeMetadata
    {
        [Key(0)] public Guid DataSourceId { get; set; }
        [Key(1)] public int RowNumber { get; set; }
        [Key(2)] public Dictionary<string, object> RecordData { get; set; }
        [Key(3)] public DateTime FirstSeenAt { get; set; }
        [Key(4)] public int DegreeCount { get; set; }
        [Key(5)] public List<int> ParticipatingDefinitions { get; set; }
    }

    #endregion
}