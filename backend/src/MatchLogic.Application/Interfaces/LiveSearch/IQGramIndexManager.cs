using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.LiveSearch
{
    public interface IQGramIndexManager
    {
        /// <summary>
        /// Check if index is loaded in memory for a project
        /// </summary>
        bool IsIndexLoaded(Guid projectId);

        /// <summary>
        /// Store index data in memory (called by indexing node after building)
        /// </summary>
        Task StoreIndexAsync(Guid projectId, QGramIndexData indexData, CancellationToken cancellationToken = default);

        /// <summary>
        /// Retrieve index from memory (returns null if not loaded)
        /// </summary>
        Task<QGramIndexData> GetIndexAsync(Guid projectId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Load index from Data store into memory (used by query nodes)
        /// </summary>
        Task<bool> LoadIndexFromDatabaseAsync(Guid projectId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Wait for index to become available in Data store (for secondary nodes)
        /// Polls database until index exists or timeout
        /// </summary>
        Task<bool> WaitForIndexAvailabilityAsync(
            Guid projectId,
            TimeSpan timeout,
            TimeSpan pollInterval,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Clear index from memory
        /// </summary>
        Task ClearIndexAsync(Guid projectId);

        /// <summary>
        /// Get statistics about loaded indexes
        /// </summary>
        IndexManagerStatistics GetStatistics();
    }

    public class QGramIndexData
    {
        public Guid IndexId { get; set; } = Guid.NewGuid();
        public Guid ProjectId { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime LoadedAt { get; set; }
        public int TotalRecords { get; set; }
        public Dictionary<string, Dictionary<uint, List<PostingEntry>>> GlobalFieldIndex { get; set; }
        public Dictionary<(Guid, int), RowMetadataLight> RowMetadata { get; set; }
        public Dictionary<Guid, DataSourceStats> DataSourceStats { get; set; }

        public long EstimatedMemoryBytes
        {
            get
            {
                // Rough estimation
                long size = 0;

                // GlobalFieldIndex
                foreach (var field in GlobalFieldIndex)
                {
                    size += field.Key.Length * 2; // string overhead
                    foreach (var qgram in field.Value)
                    {
                        size += 4; // uint
                        size += qgram.Value.Count * 24; // PostingEntry approx 24 bytes
                    }
                }

                // RowMetadata
                size += RowMetadata.Count * 200; // approximate per metadata entry

                return size;
            }
        }
    }

    public class PostingEntry
    {
        public Guid DataSourceId { get; set; }

        public int RowNumber { get; set; }

        public PostingEntry() { }

        public PostingEntry(Guid dataSourceId, int rowNumber)
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

    public class RowMetadataLight
    {
        public Guid DataSourceId { get; set; }
        public int RowNumber { get; set; }
        public Dictionary<string, HashSet<uint>> FieldHashes { get; set; }
        public Dictionary<string, string> BlockingValues { get; set; }
    }

    public class DataSourceStats
    {
        public Guid DataSourceId { get; set; }
        public int RecordCount { get; set; }
        public List<string> IndexedFields { get; set; }
    }

    public class IndexManagerStatistics
    {
        public int LoadedProjects { get; set; }
        public long TotalMemoryMB { get; set; }
        public List<ProjectIndexStats> ProjectDetails { get; set; }
    }

    public class ProjectIndexStats
    {
        public Guid ProjectId { get; set; }
        public int RecordCount { get; set; }
        public DateTime LoadedAt { get; set; }
        public TimeSpan Age { get; set; }
    }
}
