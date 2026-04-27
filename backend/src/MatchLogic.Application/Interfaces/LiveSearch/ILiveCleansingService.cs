using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.LiveSearch
{
    /// <summary>
    /// Manages cleansing rules and applies them to new records
    /// Supports multiple data sources with different rule sets (Singleton)
    /// </summary>
    public interface ILiveCleansingService
    {
        /// <summary>
        /// Load cleansing rules for all data sources in a project
        /// Called once at startup
        /// </summary>
        Task LoadProjectRulesAsync(
            Guid projectId,
            IEnumerable<Guid> dataSourceIds,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Cleanse a single record using rules for specific data source
        /// Thread-safe, can be called concurrently
        /// </summary>
        IDictionary<string, object> CleanseRecord(
            Guid dataSourceId,
            IDictionary<string, object> record);

        /// <summary>
        /// Check if cleansing is configured for a data source
        /// </summary>
        bool HasCleansingRules(Guid dataSourceId);

        /// <summary>
        /// Get list of derived columns for a data source
        /// (Columns created by MappingRules, like Address_City)
        /// </summary>
        IReadOnlyList<string> GetDerivedColumns(Guid dataSourceId);

        /// <summary>
        /// Check if project has cleansing loaded
        /// </summary>
        bool IsProjectLoaded(Guid projectId);
    }
}
