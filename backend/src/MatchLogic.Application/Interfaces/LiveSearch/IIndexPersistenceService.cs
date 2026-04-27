using MatchLogic.Application.Interfaces.Persistence;
using System;
using System.Threading;
using System.Threading.Tasks;
using static MatchLogic.Application.Interfaces.LiveSearch.IQGramIndexManager;

namespace MatchLogic.Application.Interfaces.LiveSearch
{
    /// <summary>
    /// Interface for persisting and retrieving Q-gram indexes.
    /// Implementation handles MessagePack serialization and compression.
    /// Follows same pattern as IMatchGraphStorage.
    /// </summary>
    public interface IIndexPersistenceService
    {
        /// <summary>
        /// Saves a QGramIndexData to persistent storage using MessagePack
        /// </summary>
        Task SaveIndexAsync(
            IDataStore dataStore,
            string collectionName,
            QGramIndexData indexData,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Loads a QGramIndexData from persistent storage
        /// </summary>
        Task<QGramIndexData> LoadIndexAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Checks if an index exists in the collection
        /// </summary>
        Task<bool> IndexExistsAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets metadata about the stored index without loading the entire index
        /// </summary>
        Task<IndexMetadata> GetIndexMetadataAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes an index from storage
        /// </summary>
        Task DeleteIndexAsync(
            IDataStore dataStore,
            string collectionName,
            CancellationToken cancellationToken = default);
    }

    /// <summary>
    /// Metadata about a stored index (without loading the full index)
    /// </summary>
    public class IndexMetadata
    {
        public Guid IndexId { get; set; }
        public Guid ProjectId { get; set; }
        public int TotalRecords { get; set; }
        public int TotalFields { get; set; }
        public int DataSourceCount { get; set; }
        public long CompressedSizeBytes { get; set; }
        public long UncompressedSizeBytes { get; set; }
        public double CompressionRatio { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime SavedAt { get; set; }

        public string CompressedSizeMB => $"{CompressedSizeBytes / (1024.0 * 1024.0):F2} MB";
        public string UncompressedSizeMB => $"{UncompressedSizeBytes / (1024.0 * 1024.0):F2} MB";
        public string CompressionPercentage => $"{(1 - CompressionRatio) * 100:F1}%";
    }
}