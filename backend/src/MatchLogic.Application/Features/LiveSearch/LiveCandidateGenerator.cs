using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MathNet.Numerics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch
{
    public class LiveCandidateGenerator
    {
        private readonly ProductionQGramIndexerDME _indexer;
        private readonly IQGramIndexManager _indexManager;
        private readonly IRecordStoreManager _recordStoreManager;
        private readonly IGenericRepository<MatchLogic.Domain.Entities.MatchDefinitionCollection, Guid> _matchDefRepo;
        private readonly ILogger<LiveCandidateGenerator> _logger;

        public LiveCandidateGenerator(
            ProductionQGramIndexerDME indexer,
            IQGramIndexManager indexManager,
            IRecordStoreManager recordStoreManager,
            IGenericRepository<MatchLogic.Domain.Entities.MatchDefinitionCollection, Guid> matchDefRepo,
            ILogger<LiveCandidateGenerator> logger)
        {
            _indexer = indexer ?? throw new ArgumentNullException(nameof(indexer));
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
            _recordStoreManager = recordStoreManager ?? throw new ArgumentNullException(nameof(recordStoreManager));
            _matchDefRepo = matchDefRepo ?? throw new ArgumentNullException(nameof(matchDefRepo));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Generate candidates by delegating to ProductionQGramIndexerDME
        /// </summary>
        public async Task<List<CandidatePair>> GenerateCandidatesAsync(
            Guid projectId,
            Guid sourceDataSourceId,
            IDictionary<string, object> record,
            LiveSearchOptions options,
            CancellationToken cancellationToken = default)
        {
            // Load match definitions
            var matchDefCollection = (await _matchDefRepo.QueryAsync(
                md => md.ProjectId == projectId,
                MatchLogic.Application.Common.Constants.Collections.MatchDefinitionCollection)).First();

            return await GenerateCandidatesAsync(
                projectId, sourceDataSourceId, record, matchDefCollection, options, cancellationToken);
        }

        // Overload used by Live Search hot-path so match definitions loaded from the metadata cache
        // aren't re-fetched from Mongo on every request.
        public async Task<List<CandidatePair>> GenerateCandidatesAsync(
            Guid projectId,
            Guid sourceDataSourceId,
            IDictionary<string, object> record,
            MatchLogic.Domain.Entities.MatchDefinitionCollection matchDefCollection,
            LiveSearchOptions options,
            CancellationToken cancellationToken = default)
        {
            _logger.LogInformation(
                "Generating candidates for record (Project: {ProjectId}, DataSource: {DataSourceId})",
                projectId,
                sourceDataSourceId);

            // Delegate to ProductionQGramIndexerDME.GenerateCandidatesForSingleRecordAsync
            var candidates = await _indexer.GenerateCandidatesForSingleRecordAsync(
                projectId,
                sourceDataSourceId,
                record,
                matchDefCollection,
                options.MinSimilarityThreshold,
                options.MaxCandidates,
                cancellationToken);

            _logger.LogInformation(
                "Generated {Count} candidates",
                candidates.Count);

            return candidates;
        }
    }

    /// <summary>
    /// Options for candidate generation
    /// </summary>
    public class LiveSearchOptions
    {
        public int QGramSize { get; set; } = 3;
        public double MinSimilarityThreshold { get; set; } = 0.3;
        public int MaxCandidates { get; set; } = 1000;
    }
}
