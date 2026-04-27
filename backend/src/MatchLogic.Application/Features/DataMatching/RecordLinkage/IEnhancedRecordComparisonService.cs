using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Domain.Entities;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkageg
{
    public interface IEnhancedRecordComparisonService : IAsyncDisposable
    {
        /// <summary>
        /// Compare candidates from ProductionQGramIndexer against their qualifying match definitions
        /// </summary>
        IAsyncEnumerable<ScoredMatchPair> CompareAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            [EnumeratorCancellation] CancellationToken cancellationToken = default);

        Task<(IAsyncEnumerable<ScoredMatchPair>, MatchGraph)> CompareWithGraphAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            CancellationToken cancellationToken = default);

        Task<(IAsyncEnumerable<ScoredMatchPair>, MatchGraph, Task)> CompareWithGraphAndProcessingAsync(
        IAsyncEnumerable<CandidatePair> candidates,
        MatchDefinitionCollection matchDefinitions,
        IDataSourceIndexMapper indexMapper,
        CancellationToken cancellationToken = default);

        /// <summary>
        /// Reset the pair ID counter for new processing session
        /// </summary>
        void ResetPairIdCounter();

        Task<(List<ScoredMatchPair>, MatchDefinitionCollection)> CompareAndCollectPairsAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            CancellationToken cancellationToken = default);

        // Overload for callers (e.g. Live Search) that need to cap per-request parallelism
        // independent of the configured batch default. Pass -1 to use the default.
        Task<(List<ScoredMatchPair>, MatchDefinitionCollection)> CompareAndCollectPairsAsync(
            IAsyncEnumerable<CandidatePair> candidates,
            MatchDefinitionCollection matchDefinitions,
            IDataSourceIndexMapper indexMapper,
            int maxDegreeOfParallelism,
            CancellationToken cancellationToken = default);
    }

    /// <summary>
    /// Represents a scored match pair with all relevant metadata
    /// </summary>
    public class ScoredMatchPair
    {
        public long PairId { get; set; }
        public Guid DataSource1Id { get; set; }
        public int DataSource1Index { get; set; }
        public int Row1Number { get; set; }
        public IDictionary<string, object> Record1 { get; set; }

        public Guid DataSource2Id { get; set; }
        public int DataSource2Index { get; set; }
        public int Row2Number { get; set; }
        public IDictionary<string, object> Record2 { get; set; }

        public List<int> MatchDefinitionIndices { get; set; } = new();
        public Dictionary<int, MatchScoreDetail> ScoresByDefinition { get; set; } = new();
        public double MaxScore { get; set; }
        public Dictionary<string, object> Metadata { get; set; } = new();
    }

    public class MatchScoreDetail
    {
        public double WeightedScore { get; set; }
        public double FinalScore { get; set; }
        public Dictionary<string, double> FieldScores { get; set; } = new();
        public Dictionary<string, double> FieldWeights { get; set; } = new();
    }
}
