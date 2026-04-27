using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MatchConfiguration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Grouping;

public class EnhancedGroupingService : IEnhancedGroupingService
{
    private readonly ILogger<EnhancedGroupingService> _logger;
    private readonly GroupingConfiguration _config;
    private int _nextGroupId;

    public EnhancedGroupingService(
        ILogger<EnhancedGroupingService> logger,
        IOptions<GroupingConfiguration> config = null)
    {
        _logger = logger;
        _config = config?.Value ?? GroupingConfiguration.Default();
        _nextGroupId = 0;
    }

    /// <summary>
    /// Create groups from MatchGraph using graph algorithms
    /// </summary>
    public async IAsyncEnumerable<MatchGroup> CreateGroupsFromGraphAsync(
        MatchGraph matchGraph,
        bool requireTransitive = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (matchGraph == null)
            throw new ArgumentNullException(nameof(matchGraph));

        _logger.LogInformation("Creating groups from graph with {Nodes} nodes and {Edges} edges. Transitive: {Transitive}",
            matchGraph.TotalNodes, matchGraph.TotalEdges, requireTransitive);

        if (!requireTransitive)
        {
            // Non-transitive: Simple connected components
            await foreach (var group in CreateNonTransitiveGroups(matchGraph, cancellationToken))
            {
                yield return group;
            }
        }
        else
        {
            // Transitive: Must form cliques
            await foreach (var group in CreateTransitiveGroups(matchGraph, cancellationToken))
            {
                yield return group;
            }
        }
    }

    /// <summary>
    /// Legacy method: Create groups from stream of match results
    /// </summary>
    public async IAsyncEnumerable<MatchGroup> CreateGroupsFromStreamAsync(
        IAsyncEnumerable<ScoredMatchPair> matchResults,
        bool requireTransitive = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Build graph from stream first
        var graphBuilder = new MatchGraphBuilder();
        var matchGraph = await graphBuilder.BuildFromStreamAsync(matchResults, cancellationToken);

        // Then use graph-based grouping
        await foreach (var group in CreateGroupsFromGraphAsync(matchGraph, requireTransitive, cancellationToken))
        {
            yield return group;
        }
    }

    private async IAsyncEnumerable<MatchGroup> CreateNonTransitiveGroups(
        MatchGraph matchGraph,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var components = matchGraph.GetConnectedComponents();
        _logger.LogInformation("Found {Count} connected components", components.Count);

        foreach (var component in components)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            if (component.Count < _config.MinGroupSize)
                continue;

            var group = CreateGroupFromComponent(component, matchGraph);
            yield return group;
        }
    }

    private async IAsyncEnumerable<MatchGroup> CreateTransitiveGroups(
        MatchGraph matchGraph,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var components = matchGraph.GetConnectedComponents();
        _logger.LogInformation("Processing {Count} components for transitive grouping", components.Count);

        foreach (var component in components)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            if (component.Count < _config.MinGroupSize)
                continue;

            // Check if entire component is a clique
            if (IsClique(component, matchGraph))
            {
                // Entire component is fully connected
                var group = CreateGroupFromComponent(component, matchGraph);
                yield return group;
            }
            else
            {
                // Find maximal cliques within the component
                var cliques = FindMaximalCliques(component, matchGraph);

                foreach (var clique in cliques)
                {
                    if (clique.Count >= _config.MinGroupSize)
                    {
                        var group = CreateGroupFromComponent(clique, matchGraph);
                        yield return group;
                    }
                }
            }
        }
    }

    private bool IsClique(HashSet<RecordKey> nodes, MatchGraph graph)
    {
        if (nodes.Count <= 1)
            return true;

        var nodeList = nodes.ToList();

        // Check every pair of nodes has an edge
        for (int i = 0; i < nodeList.Count; i++)
        {
            for (int j = i + 1; j < nodeList.Count; j++)
            {
                var node1 = nodeList[i];
                var node2 = nodeList[j];

                // Check if edge exists
                if (!graph.AdjacencyList.TryGetValue(node1, out var neighbors) ||
                    !neighbors.Contains(node2))
                {
                    return false;
                }
            }
        }

        return true;
    }

    private List<HashSet<RecordKey>> FindMaximalCliques(
        HashSet<RecordKey> component,
        MatchGraph graph)
    {
        // Bron-Kerbosch algorithm for finding maximal cliques
        var cliques = new List<HashSet<RecordKey>>();
        var candidates = new HashSet<RecordKey>(component);
        var currentClique = new HashSet<RecordKey>();
        var processed = new HashSet<RecordKey>();

        BronKerbosch(currentClique, candidates, processed, cliques, graph);

        return cliques;
    }

    private void BronKerbosch(
        HashSet<RecordKey> currentClique,
        HashSet<RecordKey> candidates,
        HashSet<RecordKey> processed,
        List<HashSet<RecordKey>> cliques,
        MatchGraph graph)
    {
        if (!candidates.Any() && !processed.Any())
        {
            // Found a maximal clique
            if (currentClique.Count >= _config.MinGroupSize)
            {
                cliques.Add(new HashSet<RecordKey>(currentClique));
            }
            return;
        }

        // Choose pivot to minimize branching
        var pivot = ChoosePivot(candidates, processed, graph);
        var toExplore = new HashSet<RecordKey>(candidates);

        if (pivot != null && graph.AdjacencyList.TryGetValue(pivot.Value, out var pivotNeighbors))
        {
            toExplore.ExceptWith(pivotNeighbors);
        }

        foreach (var vertex in toExplore)
        {
            currentClique.Add(vertex);

            var newCandidates = new HashSet<RecordKey>(candidates);
            var newProcessed = new HashSet<RecordKey>(processed);

            if (graph.AdjacencyList.TryGetValue(vertex, out var neighbors))
            {
                newCandidates.IntersectWith(neighbors);
                newProcessed.IntersectWith(neighbors);
            }
            else
            {
                newCandidates.Clear();
                newProcessed.Clear();
            }

            BronKerbosch(currentClique, newCandidates, newProcessed, cliques, graph);

            currentClique.Remove(vertex);
            candidates.Remove(vertex);
            processed.Add(vertex);
        }
    }

    private RecordKey? ChoosePivot(
        HashSet<RecordKey> candidates,
        HashSet<RecordKey> processed,
        MatchGraph graph)
    {
        var union = candidates.Union(processed);
        RecordKey? bestPivot = null;
        int maxConnections = -1;

        foreach (var node in union)
        {
            if (graph.AdjacencyList.TryGetValue(node, out var neighbors))
            {
                var connections = neighbors.Intersect(candidates).Count();
                if (connections > maxConnections)
                {
                    maxConnections = connections;
                    bestPivot = node;
                }
            }
        }

        return bestPivot;
    }

    private MatchGroup CreateGroupFromComponent(
HashSet<RecordKey> component,
MatchGraph matchGraph)
    {
        var groupId = Interlocked.Increment(ref _nextGroupId);
        var records = new List<IDictionary<string, object>>();
        var groupMetadata = new Dictionary<string, object>();

        // Collect all records with enrichment
        foreach (var recordKey in component)
        {
            if (matchGraph.NodeMetadata.TryGetValue(recordKey, out var metadata))
            {
                var enrichedRecord = EnrichRecordWithMatchInfo(
                    metadata.RecordData,
                    recordKey,
                    component,
                    matchGraph);

                records.Add(enrichedRecord);
            }
        }

        // Calculate group statistics
        var (avgScore, minScore, maxScore) = CalculateGroupScores(component, matchGraph);

        groupMetadata["avg_match_score"] = avgScore;
        groupMetadata["min_match_score"] = minScore;
        groupMetadata["max_match_score"] = maxScore;
        groupMetadata["is_clique"] = IsClique(component, matchGraph);
        groupMetadata["size"] = component.Count;

        // Add aggregated match definition indices for the entire group
        var groupMatchDefIndices = new HashSet<int>();
        foreach (var record in records)
        {
            if (record.TryGetValue("MatchDefinitionIndices", out var indices) &&
                indices is List<int> indicesList)
            {
                foreach (var idx in indicesList)
                {
                    groupMatchDefIndices.Add(idx);
                }
            }
        }
        groupMetadata["group_match_definition_indices"] = groupMatchDefIndices.ToList();

        return new MatchGroup
        {
            GroupId = groupId,
            Records = records,
            GroupHash = ComputeGroupHash(records),
            Metadata = groupMetadata
        };
    }

    private IDictionary<string, object> EnrichRecordWithMatchInfo(
IDictionary<string, object> record,
RecordKey recordKey,
HashSet<RecordKey> component,
MatchGraph matchGraph)
    {
        var enriched = new Dictionary<string, object>(record);
        var matchScores = new Dictionary<string, double>();
        var matchDetails = new List<Dictionary<string, object>>();
        var allMatchDefinitionIndices = new HashSet<int>();

        if (matchGraph.AdjacencyList.TryGetValue(recordKey, out var neighbors))
        {
            foreach (var neighbor in neighbors)
            {
                if (component.Contains(neighbor))
                {
                    var edgeKey = recordKey.CompareTo(neighbor) <= 0
                        ? (recordKey, neighbor)
                        : (neighbor, recordKey);

                    if (matchGraph.EdgeDetails.TryGetValue(edgeKey, out var edge))
                    {
                        matchScores[neighbor.ToString()] = edge.MaxScore;

                        var matchDetail = new Dictionary<string, object>
                        {
                            ["neighbor"] = neighbor.ToString(),
                            ["PairId"] = edge.PairId,
                            ["MaxScore"] = edge.MaxScore,
                            ["MatchDefinitionIndices"] = edge.MatchDefinitionIndices ?? new List<int>()
                        };

                        // Convert ScoresByDefinition to a nested dictionary structure
                        if (edge.ScoresByDefinition != null && edge.ScoresByDefinition.Any())
                        {
                            var scoresByDefDict = new Dictionary<string, object>();

                            foreach (var kvp in edge.ScoresByDefinition)
                            {
                                var scoreDetail = new Dictionary<string, object>
                                {
                                    ["WeightedScore"] = kvp.Value.WeightedScore,
                                    ["FinalScore"] = kvp.Value.FinalScore
                                };

                                // Convert FieldScores dictionary
                                if (kvp.Value.FieldScores != null)
                                {
                                    scoreDetail["FieldScores"] = new Dictionary<string, object>(
                                        kvp.Value.FieldScores.ToDictionary(
                                            fs => fs.Key,
                                            fs => (object)fs.Value
                                        )
                                    );
                                }

                                // Convert FieldWeights dictionary
                                if (kvp.Value.FieldWeights != null)
                                {
                                    scoreDetail["FieldWeights"] = new Dictionary<string, object>(
                                        kvp.Value.FieldWeights.ToDictionary(
                                            fw => fw.Key,
                                            fw => (object)fw.Value
                                        )
                                    );
                                }

                                scoresByDefDict[kvp.Key.ToString()] = scoreDetail;
                            }

                            matchDetail["ScoresByDefinition"] = scoresByDefDict;
                        }

                        matchDetails.Add(matchDetail);

                        // Aggregate match definition indices
                        if (edge.MatchDefinitionIndices != null)
                        {
                            foreach (var idx in edge.MatchDefinitionIndices)
                            {
                                allMatchDefinitionIndices.Add(idx);
                            }
                        }
                    }
                }
            }
        }

        enriched["_group_match_scores"] = matchScores;
        enriched["_group_match_details"] = matchDetails;
        enriched["_group_degree"] = matchScores.Count;
        enriched["_group_avg_score"] = matchScores.Any() ? matchScores.Values.Average() : 0.0;

        return enriched;
    }

    private (double avg, double min, double max) CalculateGroupScores(
        HashSet<RecordKey> component,
        MatchGraph matchGraph)
    {
        var scores = new List<double>();

        var nodeList = component.ToList();
        for (int i = 0; i < nodeList.Count; i++)
        {
            for (int j = i + 1; j < nodeList.Count; j++)
            {
                var edgeKey = nodeList[i].CompareTo(nodeList[j]) <= 0
                    ? (nodeList[i], nodeList[j])
                    : (nodeList[j], nodeList[i]);

                if (matchGraph.EdgeDetails.TryGetValue(edgeKey, out var edge))
                {
                    scores.Add(edge.MaxScore);
                }
            }
        }

        if (!scores.Any())
            return (0, 0, 0);

        return (scores.Average(), scores.Min(), scores.Max());
    }

    private string ComputeGroupHash(List<IDictionary<string, object>> records)
    {
        // Simple hash based on record identifiers
        var recordIdentifiers = records
            .Select(r => GetRecordIdentifier(r))
            .OrderBy(id => id)
            .ToList();

        var combinedString = string.Join("|", recordIdentifiers);
        using (var sha256 = System.Security.Cryptography.SHA256.Create())
        {
            var bytes = Encoding.UTF8.GetBytes(combinedString);
            var hash = sha256.ComputeHash(bytes);
            return Convert.ToBase64String(hash);
        }
    }

    private string GetRecordIdentifier(IDictionary<string, object> record)
    {
        // Try to get unique identifier from metadata
        if (record.TryGetValue("_metadata", out var metadata) &&
            metadata is IDictionary<string, object> metaDict)
        {
            if (metaDict.TryGetValue("RowNumber", out var rowNum))
                return rowNum.ToString();
        }

        // Fallback to first non-null value
        return record.Values.FirstOrDefault(v => v != null)?.ToString() ?? "";
    }

    public IAsyncEnumerable<MatchGroup> CreateGroupsFromGraphAsync(MatchGraphDME matchGraph, bool requireTransitive = false, bool preferSmallestGroup = true, bool useLegacyTransitiveAlgorithm = true, MatchingDataSourcePairs configuredSourcePairs = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}

public class GroupingConfiguration
{
    public int MinGroupSize { get; set; }
    public int MaxGroupSize { get; set; }
    public double MinMatchScore { get; set; }

    public static GroupingConfiguration Default()
    {
        return new GroupingConfiguration
        {
            MinGroupSize = 2,
            MaxGroupSize = int.MaxValue,
            MinMatchScore = 0.0,
        };
    }
}

public interface IEnhancedGroupingService
{
    IAsyncEnumerable<MatchGroup> CreateGroupsFromGraphAsync(
        MatchGraph matchGraph,
        bool requireTransitive = false,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<MatchGroup> CreateGroupsFromStreamAsync(
        IAsyncEnumerable<ScoredMatchPair> matchResults,
        bool requireTransitive = false,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<MatchGroup> CreateGroupsFromGraphAsync(
        MatchGraphDME matchGraph,
        bool requireTransitive = false,
        bool preferSmallestGroup = true,
        bool useLegacyTransitiveAlgorithm = true,
        MatchingDataSourcePairs configuredSourcePairs = null,
        CancellationToken cancellationToken = default);
}

// Helper class to build graph from stream if needed
public class MatchGraphBuilder
{
    public async Task<MatchGraph> BuildFromStreamAsync(
        IAsyncEnumerable<ScoredMatchPair> matchResults,
        CancellationToken cancellationToken)
    {
        var graph = new MatchGraph();

        await foreach (var result in matchResults.WithCancellation(cancellationToken))
        {
            var node1 = new RecordKey(result.DataSource1Id, result.Row1Number);
            var node2 = new RecordKey(result.DataSource2Id, result.Row2Number);

            graph.AddNode(node1, result.Record1);
            graph.AddNode(node2, result.Record2);

            var edgeDetails = new MatchEdgeDetails
            {
                PairId = result.PairId,
                MaxScore = result.MaxScore,
                MatchDefinitionIndices = result.MatchDefinitionIndices,
                ScoresByDefinition = result.ScoresByDefinition
            };

            graph.AddEdge(node1, node2, edgeDetails);
        }

        return graph;
    }
}
