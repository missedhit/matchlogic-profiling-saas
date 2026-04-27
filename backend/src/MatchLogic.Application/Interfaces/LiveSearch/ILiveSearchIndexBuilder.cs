using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static MatchLogic.Application.Interfaces.LiveSearch.IQGramIndexManager;

namespace MatchLogic.Application.Interfaces.LiveSearch
{
    public interface ILiveSearchIndexBuilder
    {
        /// <summary>
        /// Build index from scratch by streaming from cleanse collections
        /// </summary>
        Task<QGramIndexData> BuildIndexAsync(
            Guid projectId,
            IEnumerable<Guid> dataSourceIds,
            IProgress<IndexBuildProgress> progress = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Persist built index to MongoDB
        /// </summary>
        Task PersistIndexAsync(
            Guid projectId,
            QGramIndexData indexData,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Validate that persisted index exists and is complete
        /// </summary>
        Task<bool> ValidatePersistedIndexAsync(
            Guid projectId,
            CancellationToken cancellationToken = default);
    }

    public class IndexBuildProgress
    {
        public string Stage { get; set; }
        public int ProcessedRecords { get; set; }
        public int TotalRecords { get; set; }
        public double PercentComplete { get; set; }
        public string Message { get; set; }
    }
}
