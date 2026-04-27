using MatchLogic.Application.Features.DataMatching.Grouping;
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

public class EnhancedGroupingServiceDME : IEnhancedGroupingService
{
    private readonly ILogger<EnhancedGroupingService> _logger;
    private readonly GroupingConfiguration _config;
    private int _nextGroupId;

    public EnhancedGroupingServiceDME(
        ILogger<EnhancedGroupingService> logger,
        IOptions<GroupingConfiguration> config = null)
    {
        _logger = logger;
        _config = GroupingConfiguration.Default();
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

    public async IAsyncEnumerable<MatchGroup> CreateGroupsFromGraphAsync(
        MatchGraphDME matchGraph,
        bool requireTransitive = false,
        bool preferSmallestGroup = true,
        bool useLegacyTransitiveAlgorithm = true,
        MatchingDataSourcePairs configuredSourcePairs = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (matchGraph == null)
            throw new ArgumentNullException(nameof(matchGraph));

        _logger.LogInformation(
            "Creating groups from optimized graph with {Nodes} nodes and {Edges} edges. Transitive: {Transitive}",
            matchGraph.TotalNodes, matchGraph.TotalEdges, requireTransitive);

        if (!requireTransitive)
        {
            // Non-transitive: Simple connected components
            await foreach (var group in CreateNonTransitiveGroupsDME(matchGraph, cancellationToken))
            {
                yield return group;
            }
        }
        else
        {
            if (useLegacyTransitiveAlgorithm)
            {
                await foreach (var group in CreateTransitiveGroupsLegacyDME(matchGraph, configuredSourcePairs, cancellationToken))
                {
                    yield return group;
                }
            }
            else
            {
                await foreach (var group in CreateTransitiveGroupsDME(matchGraph, preferSmallestGroup, cancellationToken))
                {
                    yield return group;
                }
            }
        }
    }

    private async IAsyncEnumerable<MatchGroup> CreateNonTransitiveGroupsDME(
        MatchGraphDME matchGraph,
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

            var group = CreateGroupFromComponentDME(component, matchGraph);
            yield return group;
        }
    }

    private async IAsyncEnumerable<MatchGroup> CreateTransitiveGroupsDME(
       MatchGraphDME matchGraph,
       bool preferSmallestGroup,
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
            if (IsCliqueDME(component, matchGraph))
            {
                var group = CreateGroupFromComponentDME(component, matchGraph);
                yield return group;
            }
            else
            {
                // Find maximal cliques within the component
                var cliques = FindMaximalCliquesDME(component, matchGraph);

                if (preferSmallestGroup)
                {
                    // Sort cliques by size (smallest first) and assign records uniquely
                    var sortedCliques = cliques
                        .Where(c => c.Count >= _config.MinGroupSize)
                        .OrderBy(c => c.Count)
                        .ThenByDescending(c => CalculateCliqueAvgScoreDME(c, matchGraph))
                        .ToList();

                    // Pre-calculate scores for all cliques
                    var cliqueScores = new Dictionary<int, double>();
                    for (int i = 0; i < sortedCliques.Count; i++)
                    {
                        cliqueScores[i] = CalculateCliqueAvgScoreDME(sortedCliques[i], matchGraph);
                    }

                    // Build a map of which cliques each record appears in
                    var recordToCliques = new Dictionary<RecordKey, List<int>>();
                    for (int i = 0; i < sortedCliques.Count; i++)
                    {
                        foreach (var record in sortedCliques[i])
                        {
                            if (!recordToCliques.ContainsKey(record))
                                recordToCliques[record] = new List<int>();
                            recordToCliques[record].Add(i);
                        }
                    }

                    var assignedRecords = new HashSet<RecordKey>();
                    var sacrificedCliques = new HashSet<int>(); // Cliques we've decided to skip

                    for (int cliqueIdx = 0; cliqueIdx < sortedCliques.Count; cliqueIdx++)
                    {
                        if (sacrificedCliques.Contains(cliqueIdx))
                            continue;

                        var clique = sortedCliques[cliqueIdx];

                        // Remove already assigned records from this clique
                        var availableRecords = clique.Where(r => !assignedRecords.Contains(r)).ToHashSet();

                        // Check if we have enough unassigned records
                        if (availableRecords.Count < _config.MinGroupSize)
                            continue;

                        // Find conflicting cliques that would be orphaned if we process this one
                        var conflictingCliques = new List<int>();
                        foreach (var record in availableRecords)
                        {
                            if (recordToCliques.TryGetValue(record, out var participatingCliques))
                            {
                                foreach (var otherCliqueIdx in participatingCliques)
                                {
                                    if (otherCliqueIdx == cliqueIdx ||
                                        sacrificedCliques.Contains(otherCliqueIdx) ||
                                        conflictingCliques.Contains(otherCliqueIdx))
                                        continue;

                                    var otherClique = sortedCliques[otherCliqueIdx];

                                    // Count how many records would be available in the other clique
                                    var recordsToAssign = new HashSet<RecordKey>(availableRecords);
                                    var otherAvailableCount = otherClique
                                        .Count(r => !assignedRecords.Contains(r) && !recordsToAssign.Contains(r));

                                    // If the other clique would be orphaned, it's a conflict
                                    if (otherAvailableCount < _config.MinGroupSize)
                                    {
                                        conflictingCliques.Add(otherCliqueIdx);
                                    }
                                }
                            }
                        }

                        // If there are conflicts, choose the best scoring clique and sacrifice others
                        if (conflictingCliques.Any())
                        {
                            // Compare current clique with all conflicting ones
                            var currentScore = cliqueScores[cliqueIdx];
                            var allInvolvedCliques = new List<(int index, double score)>
                            {
                                (cliqueIdx, currentScore)
                            };

                            foreach (var conflictIdx in conflictingCliques)
                            {
                                allInvolvedCliques.Add((conflictIdx, cliqueScores[conflictIdx]));
                            }

                            // Find the best scoring clique
                            var bestClique = allInvolvedCliques.OrderByDescending(c => c.score).First();

                            // If current clique is not the best, sacrifice it and continue
                            if (bestClique.index != cliqueIdx)
                            {
                                sacrificedCliques.Add(cliqueIdx);
                                _logger.LogDebug("Sacrificing clique {Index} (size {Size}, score {Score:F2}) in favor of better scoring clique {BestIndex} (score {BestScore:F2})",
                                    cliqueIdx, clique.Count, currentScore, bestClique.index, bestClique.score);
                                continue;
                            }

                            // Current clique is the best, sacrifice all conflicting ones
                            foreach (var conflictIdx in conflictingCliques)
                            {
                                sacrificedCliques.Add(conflictIdx);
                                _logger.LogDebug("Sacrificing clique {Index} (score {Score:F2}) in favor of current clique {CurrentIndex} (score {CurrentScore:F2})",
                                    conflictIdx, cliqueScores[conflictIdx], cliqueIdx, currentScore);
                            }
                        }

                        // Create the group
                        var group = CreateGroupFromComponentDME(availableRecords, matchGraph);

                        // Mark these records as assigned
                        foreach (var record in availableRecords)
                        {
                            assignedRecords.Add(record);
                        }

                        yield return group;
                    }

                    _logger.LogInformation("Assigned {Assigned} unique records to groups from {Total} cliques. Sacrificed {Sacrificed} cliques.",
                        assignedRecords.Count, cliques.Count, sacrificedCliques.Count);
                }
                else
                {
                    // Original behavior: allow records in multiple groups
                    foreach (var clique in cliques)
                    {
                        if (clique.Count >= _config.MinGroupSize)
                        {
                            var group = CreateGroupFromComponentDME(clique, matchGraph);
                            yield return group;
                        }
                    }
                }
            }
        }
    }

    private bool IsCliqueDME(HashSet<RecordKey> nodes, MatchGraphDME graph)
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

    private List<HashSet<RecordKey>> FindMaximalCliquesDME(
        HashSet<RecordKey> component,
        MatchGraphDME graph)
    {
        // Bron-Kerbosch algorithm for finding maximal cliques
        var cliques = new List<HashSet<RecordKey>>();
        var candidates = new HashSet<RecordKey>(component);
        var currentClique = new HashSet<RecordKey>();
        var processed = new HashSet<RecordKey>();

        BronKerboschDME(currentClique, candidates, processed, cliques, graph);

        return cliques;
    }

    private void BronKerboschDME(
        HashSet<RecordKey> currentClique,
        HashSet<RecordKey> candidates,
        HashSet<RecordKey> processed,
        List<HashSet<RecordKey>> cliques,
        MatchGraphDME graph)
    {
        if (!candidates.Any() && !processed.Any())
        {
            if (currentClique.Count >= _config.MinGroupSize)
            {
                cliques.Add(new HashSet<RecordKey>(currentClique));
            }
            return;
        }

        var pivot = ChoosePivotDME(candidates, processed, graph);
        var toExplore = new HashSet<RecordKey>(candidates);

        if (pivot != null && graph.AdjacencyList.TryGetValue(pivot.Value, out var pivotNeighbors))
        {
            toExplore.ExceptWith(pivotNeighbors);
        }

        foreach (var vertex in toExplore.ToList())
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

            BronKerboschDME(currentClique, newCandidates, newProcessed, cliques, graph);

            currentClique.Remove(vertex);
            candidates.Remove(vertex);
            processed.Add(vertex);
        }
    }

    private RecordKey? ChoosePivotDME(
        HashSet<RecordKey> candidates,
        HashSet<RecordKey> processed,
        MatchGraphDME graph)
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

    private MatchGroup CreateGroupFromComponentDME(
            HashSet<RecordKey> component,
            MatchGraphDME matchGraph)
    {
        var groupId = Interlocked.Increment(ref _nextGroupId);
        var records = new List<IDictionary<string, object>>();
        var groupMetadata = new Dictionary<string, object>();

        // Collect all records with enrichment
        bool isFirstRecord = true;
        foreach (var recordKey in component)
        {
            if (matchGraph.NodeMetadata.TryGetValue(recordKey, out var metadata))
            {
                var enrichedRecord = EnrichRecordWithMatchInfoDME(
                    metadata.RecordData,
                    recordKey,
                    component,
                    matchGraph);

                // Add system fields
                enrichedRecord[RecordSystemFieldNames.IsMasterRecord] = isFirstRecord;
                enrichedRecord[RecordSystemFieldNames.IsMasterRecord_DefaultChanged] = false;
                enrichedRecord[RecordSystemFieldNames.Selected] = false;
                enrichedRecord[RecordSystemFieldNames.Selected_DefaultChanged] = false;
                enrichedRecord[RecordSystemFieldNames.NotDuplicate] = false;
                enrichedRecord[RecordSystemFieldNames.NotDuplicate_DefaultChanged] = false;

                records.Add(enrichedRecord);
                isFirstRecord = false; // Only the first record is master
            }
        }

        // Calculate group statistics
        var (avgScore, minScore, maxScore) = CalculateGroupScoresDME(component, matchGraph);

        groupMetadata["avg_match_score"] = avgScore;
        groupMetadata["min_match_score"] = minScore;
        groupMetadata["max_match_score"] = maxScore;
        groupMetadata["is_clique"] = IsCliqueDME(component, matchGraph);
        groupMetadata["size"] = component.Count;

        // Add aggregated match definition indices
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

    private IDictionary<string, object> EnrichRecordWithMatchInfoDME(
        IDictionary<string, object> record,
        RecordKey recordKey,
        HashSet<RecordKey> component,
        MatchGraphDME matchGraph)
    {
        var enriched = new Dictionary<string, object>(record);
        // ═══════════════════════════════════════════════════════════════
        // NEW: Ensure _metadata contains DataSourceId for THIS record
        // ═══════════════════════════════════════════════════════════════
        if (enriched.TryGetValue("_metadata", out var metadataObj) &&
            metadataObj is IDictionary<string, object> existingMeta)
        {
            // Create a COPY to avoid modifying original NodeMetadata
            var newMetadata = new Dictionary<string, object>(existingMeta)
            {
                ["DataSourceId"] = recordKey.DataSourceId  // Current record's DataSourceId
            };
            enriched["_metadata"] = newMetadata;
        }
        else
        {
            // Create _metadata if it doesn't exist
            enriched["_metadata"] = new Dictionary<string, object>
            {
                ["DataSourceId"] = recordKey.DataSourceId,
                ["RowNumber"] = recordKey.RowNumber
            };
        }
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

                                if (kvp.Value.FieldScores != null)
                                {
                                    scoreDetail["FieldScores"] = new Dictionary<string, object>(
                                        kvp.Value.FieldScores.ToDictionary(
                                            fs => fs.Key,
                                            fs => (object)fs.Value));
                                }

                                if (kvp.Value.FieldWeights != null)
                                {
                                    scoreDetail["FieldWeights"] = new Dictionary<string, object>(
                                        kvp.Value.FieldWeights.ToDictionary(
                                            fw => fw.Key,
                                            fw => (object)fw.Value));
                                }

                                scoresByDefDict[kvp.Key.ToString()] = scoreDetail;
                            }

                            matchDetail["ScoresByDefinition"] = scoresByDefDict;
                        }

                        matchDetails.Add(matchDetail);

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

    private (double avg, double min, double max) CalculateGroupScoresDME(
        HashSet<RecordKey> component,
        MatchGraphDME matchGraph)
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

    private double CalculateCliqueAvgScoreDME(HashSet<RecordKey> clique, MatchGraphDME matchGraph)
    {
        var scores = new List<double>();
        var nodeList = clique.ToList();

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

        return scores.Any() ? scores.Average() : 0.0;
    }


    #region Legacy Transitive Algorithm (Union-Find with Pair Validation)

    /// <summary>
    /// Creates transitive groups using legacy Union-Find algorithm.
    /// This matches the behavior of legacy MatchEngine.AllRecordsInGroupMustBeSimilar.
    /// Key difference from Bron-Kerbosch: does NOT require within-group pairs,
    /// only validates that new records have pairs with existing group members.
    /// </summary>
    private async IAsyncEnumerable<MatchGroup> CreateTransitiveGroupsLegacyDME(
        MatchGraphDME matchGraph,
        MatchingDataSourcePairs configuredSourcePairs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing transitive grouping using Legacy Union-Find algorithm");

        // Build pair lookup from graph edges
        var pairLookup = BuildPairLookupDME(matchGraph);

        // Track group assignments: RecordKey -> GroupId
        var recordToGroup = new Dictionary<RecordKey, int>();

        // Track group members: GroupId -> Set of RecordKeys
        var groupMembers = new Dictionary<int, HashSet<RecordKey>>();

        // Track similar groups that might need merging: GroupId -> Set of similar GroupIds
        var similarGroups = new Dictionary<int, HashSet<int>>();

        int nextGroupId = 1;

        // Process each edge (pair) in the graph
        foreach (var edgeKvp in matchGraph.EdgeDetails)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            var (node1, node2) = edgeKvp.Key;

            var hasGroup1 = recordToGroup.TryGetValue(node1, out int groupId1);
            var hasGroup2 = recordToGroup.TryGetValue(node2, out int groupId2);

            if (!hasGroup1 && !hasGroup2)
            {
                // Neither record has a group - create new group
                var newGroupId = nextGroupId++;
                recordToGroup[node1] = newGroupId;
                recordToGroup[node2] = newGroupId;
                groupMembers[newGroupId] = new HashSet<RecordKey> { node1, node2 };
            }
            else if (hasGroup1 && hasGroup2)
            {
                // Both have groups
                if (groupId1 != groupId2)
                {
                    // Track as potentially similar groups for later merging
                    if (!similarGroups.ContainsKey(groupId1))
                        similarGroups[groupId1] = new HashSet<int> { groupId1 };
                    if (!similarGroups.ContainsKey(groupId2))
                        similarGroups[groupId2] = new HashSet<int> { groupId2 };

                    // Union the similar group sets
                    var union = similarGroups[groupId1];
                    union.UnionWith(similarGroups[groupId2]);

                    foreach (var gid in union)
                    {
                        similarGroups[gid] = union;
                    }
                }
            }
            else
            {
                // One has a group, one doesn't
                var existingGroupId = hasGroup1 ? groupId1 : groupId2;
                var newRecord = hasGroup1 ? node2 : node1;

                // Check if new record is similar to all existing group members
                if (RecordIsSimilarToAllGroupMembersDME(newRecord, groupMembers[existingGroupId], pairLookup, configuredSourcePairs))
                {
                    recordToGroup[newRecord] = existingGroupId;
                    groupMembers[existingGroupId].Add(newRecord);
                }
                // If not similar to all, record remains unassigned for now
                // It may get assigned via another pair or form its own group
            }
        }

        // Merge similar groups where all cross-group pairs exist
        var processedGroups = new HashSet<int>();
        var finalGroups = new List<HashSet<RecordKey>>();

        foreach (var groupId in groupMembers.Keys.OrderBy(g => g))
        {
            if (processedGroups.Contains(groupId))
                continue;

            var currentGroup = new HashSet<RecordKey>(groupMembers[groupId]);
            processedGroups.Add(groupId);

            // Check if this group has similar groups to merge with
            if (similarGroups.TryGetValue(groupId, out var similarSet))
            {
                foreach (var similarGroupId in similarSet.OrderBy(g => g))
                {
                    if (similarGroupId == groupId || processedGroups.Contains(similarGroupId))
                        continue;

                    if (!groupMembers.ContainsKey(similarGroupId))
                        continue;

                    // Check if groups can merge (all cross-pairs exist)
                    if (GroupsCanMergeDME(currentGroup, groupMembers[similarGroupId], pairLookup, configuredSourcePairs))
                    {
                        currentGroup.UnionWith(groupMembers[similarGroupId]);
                        processedGroups.Add(similarGroupId);
                    }
                }
            }

            if (currentGroup.Count >= _config.MinGroupSize)
            {
                finalGroups.Add(currentGroup);
            }
        }

        // Log unassigned records
        var assignedRecords = finalGroups.SelectMany(g => g).ToHashSet();
        var allRecords = matchGraph.NodeMetadata.Keys.ToHashSet();
        var unassignedCount = allRecords.Except(assignedRecords).Count();

        if (unassignedCount > 0)
        {
            _logger.LogDebug("{Count} records could not be assigned to transitive groups", unassignedCount);
        }

        // Yield the final groups
        foreach (var group in finalGroups)
        {
            var matchGroup = CreateGroupFromComponentDME(group, matchGraph);
            yield return matchGroup;
        }

        _logger.LogInformation("Legacy Union-Find grouping completed: {Count} groups from {TotalRecords} records",
            finalGroups.Count, assignedRecords.Count);
    }

    /// <summary>
    /// Builds a lookup of which records have direct pairs with which other records.
    /// </summary>
    private Dictionary<RecordKey, HashSet<RecordKey>> BuildPairLookupDME(MatchGraphDME graph)
    {
        var pairLookup = new Dictionary<RecordKey, HashSet<RecordKey>>();

        foreach (var kvp in graph.AdjacencyList)
        {
            pairLookup[kvp.Key] = new HashSet<RecordKey>(kvp.Value);
        }

        return pairLookup;
    }

    /// <summary>
    /// Checks if a candidate record has a direct pair with ALL existing members of a group.
    /// This mimics legacy's GroupIdIsNewOrAllRecordsInGroupAreSimilar behavior.
    /// </summary>
    private bool RecordIsSimilarToAllGroupMembersDME(
        RecordKey candidate,
        HashSet<RecordKey> groupMembers,
        Dictionary<RecordKey, HashSet<RecordKey>> pairLookup,
        MatchingDataSourcePairs configuredSourcePairs)
    {
        if (!pairLookup.TryGetValue(candidate, out var candidatePairs))
        {
            // Candidate has no pairs - check if any configured member requires one
            foreach (var member in groupMembers)
            {
                if (member.Equals(candidate))
                    continue;

                if (AreDataSourcesInMatchingPair(candidate.DataSourceId, member.DataSourceId, configuredSourcePairs))
                {
                    return false;
                }
            }
            return true;
        }

        foreach (var member in groupMembers)
        {
            if (member.Equals(candidate))
                continue;

            // Only check if these sources are configured for matching
            if (!AreDataSourcesInMatchingPair(candidate.DataSourceId, member.DataSourceId, configuredSourcePairs))
                continue;

            if (!candidatePairs.Contains(member))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Checks if all records in group1 have pairs with all records in group2.
    /// This mimics legacy's RecordsInBothGroupsAreSimilarBetweenThemselves behavior.
    /// </summary>
    private bool GroupsCanMergeDME(
        HashSet<RecordKey> group1,
        HashSet<RecordKey> group2,
        Dictionary<RecordKey, HashSet<RecordKey>> pairLookup,
        MatchingDataSourcePairs configuredSourcePairs)
    {
        foreach (var record1 in group1)
        {
            foreach (var record2 in group2)
            {
                // Only check if these sources are configured for matching
                if (!AreDataSourcesInMatchingPair(record1.DataSourceId, record2.DataSourceId, configuredSourcePairs))
                    continue;

                if (!pairLookup.TryGetValue(record1, out var record1Pairs))
                    return false;

                if (!record1Pairs.Contains(record2))
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Checks if two data sources are configured as a matching pair.
    /// This mimics legacy behavior where only configured source pairs are validated.
    /// </summary>
    private bool AreDataSourcesInMatchingPair(
        Guid dataSource1Id,
        Guid dataSource2Id,
        MatchingDataSourcePairs configuredSourcePairs)
    {
        if (configuredSourcePairs == null || configuredSourcePairs.Count == 0)
        {
            // If no configuration, default to checking all cross-source pairs
            return dataSource1Id != dataSource2Id;
        }

        return configuredSourcePairs.Contains(dataSource1Id, dataSource2Id);
    }

    #endregion

}
