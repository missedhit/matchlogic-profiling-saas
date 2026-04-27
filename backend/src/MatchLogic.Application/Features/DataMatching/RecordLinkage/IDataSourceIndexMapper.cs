using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public interface IDataSourceIndexMapper
    {
        /// <summary>
        /// Initialize mapper by fetching data sources and match definitions for a project
        /// </summary>
        Task InitializeAsync(Guid projectId);

        /// <summary>
        /// Get integer index for a data source GUID
        /// </summary>
        int GetDataSourceIndex(Guid dataSourceId);

        /// <summary>
        /// Get integer index for a match definition GUID
        /// </summary>
        int GetDefinitionIndex(Guid definitionId);

        /// <summary>
        /// Get data source GUID from integer index
        /// </summary>
        Guid GetDataSourceId(int index);

        /// <summary>
        /// Get match definition GUID from integer index
        /// </summary>
        Guid GetDefinitionId(int index);

        /// <summary>
        /// Try to get data source index (safe version)
        /// </summary>
        bool TryGetDataSourceIndex(Guid dataSourceId, out int index);

        /// <summary>
        /// Try to get definition index (safe version)
        /// </summary>
        bool TryGetDefinitionIndex(Guid definitionId, out int index);

        /// <summary>
        /// Get all mapped data source IDs
        /// </summary>
        IReadOnlyCollection<Guid> GetAllDataSourceIds();

        /// <summary>
        /// Get all mapped definition IDs
        /// </summary>
        IReadOnlyCollection<Guid> GetAllDefinitionIds();

        /// <summary>
        /// Get complete data source to index mapping
        /// </summary>
        IReadOnlyDictionary<Guid, int> GetDataSourceIndexMap();

        /// <summary>
        /// Get complete definition to index mapping
        /// </summary>
        IReadOnlyDictionary<Guid, int> GetDefinitionIndexMap();
        bool TryGetDataSourceName(Guid dataSourceId, out string dsName);

        bool IsInitialized { get; }
        int DataSourceCount { get; }
        int DefinitionCount { get; }
    }
}
