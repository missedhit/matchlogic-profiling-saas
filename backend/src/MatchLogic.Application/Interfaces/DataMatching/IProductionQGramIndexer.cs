using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataMatching;

public interface IProductionQGramIndexer : IDisposable
{
    Task<IndexingResult> IndexDataSourceAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        DataSourceIndexingConfig config,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Primary method: Generate candidates based on match definitions with proper tracking
    /// </summary>
    IAsyncEnumerable<CandidatePair> GenerateCandidatesFromMatchDefinitionsAsync(
        MatchDefinitionCollection matchDefinitions,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Legacy method: Generate cross-source candidates by field names (for backward compatibility)
    /// </summary>
    IAsyncEnumerable<CandidatePair> GenerateCrossSourceCandidatesAsync(
        Guid sourceId1, Guid sourceId2, List<string> fieldNames,
        double minSimilarityThreshold = 0.1,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Legacy method: Generate within-source candidates by field names (for backward compatibility)
    /// </summary>
    IAsyncEnumerable<CandidatePair> GenerateWithinSourceCandidatesAsync(
        Guid sourceId, List<string> fieldNames,
        double minSimilarityThreshold = 0.1,
        CancellationToken cancellationToken = default);

    Task<IDictionary<string, object>> GetRecordAsync(Guid dataSourceId, int rowNumber);

    Task<IList<IDictionary<string, object>>> GetRecordsAsync(Guid dataSourceId, IEnumerable<int> rowNumbers);

    IndexerStatistics GetStatistics();

    void ClearCaches();

    // Live Search Methods
    QGramIndexData BuildIndexDataForPersistence(Guid projectId);
    void LoadIndexDataFromPersistence(QGramIndexData indexData);
    Task<List<CandidatePair>> GenerateCandidatesForSingleRecordAsync(
        Guid projectId,
        Guid newRecordDataSourceId,
        IDictionary<string, object> newRecord,
        MatchDefinitionCollection matchDefinitions,
        double minSimilarityThreshold = 0.1,
        int maxCandidates = 1000,
        CancellationToken cancellationToken = default);
    void RegisterRecordStore(Guid dataSourceId, IRecordStore store);
    public IRecordStore GetRecordStore(Guid dataSourceId);
}
