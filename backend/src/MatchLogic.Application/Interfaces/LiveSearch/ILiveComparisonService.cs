using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.LiveSearch;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.LiveSearch
{
    /// <summary>
    /// Compares and scores candidate pairs using strategy-based approach (Scoped)
    /// </summary>
    public interface ILiveComparisonService
    {
        /// <summary>
        /// Compare new record against candidates with strategy-driven behavior
        /// </summary>
        Task<List<QualifiedMatch>> CompareAsync(
            IDictionary<string, object> newRecord,
            List<CandidateMatch> candidates,
            SearchStrategy strategy,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Reset pair ID counter for new session
        /// </summary>
        void ResetPairIdCounter();
    }
}