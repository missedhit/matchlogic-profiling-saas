using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.LiveSearch
{
    /// <summary>
    /// Manages record stores for all data sources (Singleton)
    /// Each node builds its own stores independently
    /// </summary>
    public interface IRecordStoreManager
    {
        /// <summary>
        /// Build record stores for all data sources in a project
        /// Streams from cleanse_* collections
        /// </summary>
        Task BuildStoresAsync(
            Guid projectId,
            IEnumerable<Guid> dataSourceIds,
            IProgress<RecordStoreBuildProgress> progress = null,
            CancellationToken cancellationToken = default);

        void RegisterRecordStore(Guid datasourceId, IRecordStore store);

        /// <summary>
        /// Get a single record by row number
        /// </summary>
        Task<IDictionary<string, object>> GetRecordAsync(
            Guid dataSourceId,
            int rowNumber,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Get multiple records by row numbers (batch fetch)
        /// </summary>
        Task<List<IDictionary<string, object>>> GetRecordsAsync(
            Guid dataSourceId,
            IEnumerable<int> rowNumbers,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Check if stores are built for a project
        /// </summary>
        bool AreStoresBuilt(Guid projectId);

        /// <summary>
        /// Get statistics about record stores
        /// </summary>
        RecordStoreStatistics GetStatistics();

        IRecordStore GetRecordStoreAsync(Guid datasourceId);
    }

    public class RecordStoreBuildProgress
    {
        public Guid DataSourceId { get; set; }
        public int ProcessedRecords { get; set; }
        public int TotalRecords { get; set; }
        public double PercentComplete { get; set; }
        public string Message { get; set; }
    }

    public class RecordStoreStatistics
    {
        public int TotalDataSources { get; set; }
        public int TotalRecords { get; set; }
        public long TotalMemoryMB { get; set; }
        public List<DataSourceStoreStats> DataSourceStats { get; set; }
    }

    public class DataSourceStoreStats
    {
        public Guid DataSourceId { get; set; }
        public int RecordCount { get; set; }
        public long MemoryMB { get; set; }
        public string StoreType { get; set; }
    }
}