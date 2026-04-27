using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class MatchGraph
    {
        public ConcurrentDictionary<RecordKey, HashSet<RecordKey>> AdjacencyList { get; }
        public ConcurrentDictionary<(RecordKey, RecordKey), MatchEdgeDetails> EdgeDetails { get; }
        public ConcurrentDictionary<RecordKey, NodeMetadata> NodeMetadata { get; }

        public Guid GraphId { get; set; }
        public Guid ProjectId { get; set; }
        public DateTime CreatedAt { get; set; }

        public int TotalNodes => AdjacencyList.Count;
        public int TotalEdges => EdgeDetails.Count;

        public MatchGraph(Guid projectId = default)
        {
            GraphId = Guid.NewGuid();
            ProjectId = projectId;
            CreatedAt = DateTime.UtcNow;

            AdjacencyList = new ConcurrentDictionary<RecordKey, HashSet<RecordKey>>();
            EdgeDetails = new ConcurrentDictionary<(RecordKey, RecordKey), MatchEdgeDetails>();
            NodeMetadata = new ConcurrentDictionary<RecordKey, NodeMetadata>();
        }

        public void AddNode(RecordKey node, IDictionary<string, object> recordData)
        {
            var metadata = new NodeMetadata
            {
                RecordKey = node,
                RecordData = new Dictionary<string, object>(recordData),
                FirstSeenAt = DateTime.UtcNow
            };

            NodeMetadata.TryAdd(node, metadata);
        }

        public void AddEdge(RecordKey node1, RecordKey node2, MatchEdgeDetails details)
        {
            AdjacencyList.AddOrUpdate(node1,
                _ => new HashSet<RecordKey> { node2 },
                (_, set) => { lock (set) { set.Add(node2); } return set; });

            AdjacencyList.AddOrUpdate(node2,
                _ => new HashSet<RecordKey> { node1 },
                (_, set) => { lock (set) { set.Add(node1); } return set; });

            var edgeKey = GetCanonicalEdgeKey(node1, node2);
            EdgeDetails.TryAdd(edgeKey, details);
        }

        private (RecordKey, RecordKey) GetCanonicalEdgeKey(RecordKey node1, RecordKey node2)
        {
            return node1.CompareTo(node2) <= 0 ? (node1, node2) : (node2, node1);
        }

        public List<HashSet<RecordKey>> GetConnectedComponents()
        {
            var visited = new HashSet<RecordKey>();
            var components = new List<HashSet<RecordKey>>();

            foreach (var node in AdjacencyList.Keys)
            {
                if (!visited.Contains(node))
                {
                    var component = new HashSet<RecordKey>();
                    DFS(node, visited, component);
                    components.Add(component);
                }
            }

            return components;
        }

        private void DFS(RecordKey node, HashSet<RecordKey> visited, HashSet<RecordKey> component)
        {
            visited.Add(node);
            component.Add(node);

            if (AdjacencyList.TryGetValue(node, out var neighbors))
            {
                foreach (var neighbor in neighbors)
                {
                    if (!visited.Contains(neighbor))
                    {
                        DFS(neighbor, visited, component);
                    }
                }
            }
        }
    }

    public struct RecordKey : IComparable<RecordKey>, IEquatable<RecordKey>
    {
        public Guid DataSourceId { get; set; }
        public int RowNumber { get; set; }

        public RecordKey(Guid dataSourceId, int rowNumber)
        {
            DataSourceId = dataSourceId;
            RowNumber = rowNumber;
        }

        public int CompareTo(RecordKey other)
        {
            var dsCompare = DataSourceId.CompareTo(other.DataSourceId);
            return dsCompare != 0 ? dsCompare : RowNumber.CompareTo(other.RowNumber);
        }

        public bool Equals(RecordKey other)
        {
            return DataSourceId == other.DataSourceId && RowNumber == other.RowNumber;
        }

        public override int GetHashCode() => HashCode.Combine(DataSourceId, RowNumber);
        public override string ToString() => $"{DataSourceId}:{RowNumber}";
    }

    public class NodeMetadata
    {
        public RecordKey RecordKey { get; set; }
        public Dictionary<string, object> RecordData { get; set; }
        public DateTime FirstSeenAt { get; set; }
        public int DegreeCount { get; set; }
        public HashSet<int> ParticipatingDefinitions { get; set; }

        public NodeMetadata()
        {
            RecordData = new Dictionary<string, object>();
            ParticipatingDefinitions = new HashSet<int>();
        }
    }

    public class MatchEdgeDetails
    {
        public long PairId { get; set; }
        public double MaxScore { get; set; }
        public List<int> MatchDefinitionIndices { get; set; }
        public Dictionary<int, MatchScoreDetail> ScoresByDefinition { get; set; }
        public DateTime MatchedAt { get; set; }

        public MatchEdgeDetails()
        {
            MatchDefinitionIndices = new List<int>();
            ScoresByDefinition = new Dictionary<int, MatchScoreDetail>();
            MatchedAt = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// Optimized MatchGraph for single-threaded, deferred building after matching completes.
    /// No concurrent collections or locks needed - focuses on bulk performance and memory efficiency.
    /// </summary>
    public class MatchGraphDME
    {
        // Regular dictionaries - no concurrency needed since single-threaded
        public Dictionary<RecordKey, HashSet<RecordKey>> AdjacencyList { get; }
        public Dictionary<(RecordKey, RecordKey), MatchEdgeDetails> EdgeDetails { get; }
        public Dictionary<RecordKey, NodeMetadata> NodeMetadata { get; }

        public Guid GraphId { get; set; }
        public Guid ProjectId { get; set; }
        public DateTime CreatedAt { get; set; }

        public int TotalNodes => AdjacencyList.Count;
        public int TotalEdges => EdgeDetails.Count;

        /// <summary>
        /// Creates a new optimized MatchGraph with optional capacity pre-allocation
        /// </summary>
        public MatchGraphDME(Guid projectId = default, int estimatedNodeCapacity = 0)
        {
            GraphId = Guid.NewGuid();
            ProjectId = projectId;
            CreatedAt = DateTime.UtcNow;

            // Pre-allocate capacity for better performance
            if (estimatedNodeCapacity > 0)
            {
                AdjacencyList = new Dictionary<RecordKey, HashSet<RecordKey>>(estimatedNodeCapacity);
                NodeMetadata = new Dictionary<RecordKey, NodeMetadata>(estimatedNodeCapacity);

                // Edges are typically ~2-3x nodes in dense graphs
                var estimatedEdgeCapacity = estimatedNodeCapacity * 2;
                EdgeDetails = new Dictionary<(RecordKey, RecordKey), MatchEdgeDetails>(estimatedEdgeCapacity);
            }
            else
            {
                AdjacencyList = new Dictionary<RecordKey, HashSet<RecordKey>>();
                EdgeDetails = new Dictionary<(RecordKey, RecordKey), MatchEdgeDetails>();
                NodeMetadata = new Dictionary<RecordKey, NodeMetadata>();
            }
        }

        /// <summary>
        /// Adds a node to the graph (no locking needed - single-threaded)
        /// </summary>
        public void AddNode(RecordKey node, IDictionary<string, object> recordData)
        {
            if (!NodeMetadata.ContainsKey(node))
            {
                NodeMetadata[node] = new NodeMetadata
                {
                    RecordKey = node,
                    RecordData = new Dictionary<string, object>(recordData),
                    FirstSeenAt = DateTime.UtcNow,
                    DegreeCount = 0,
                    ParticipatingDefinitions = new HashSet<int>()
                };
            }
        }

        /// <summary>
        /// Adds an edge to the graph (no locking needed - single-threaded)
        /// </summary>
        public void AddEdge(RecordKey node1, RecordKey node2, MatchEdgeDetails details)
        {
            // Ensure both nodes exist in adjacency list
            if (!AdjacencyList.ContainsKey(node1))
            {
                AdjacencyList[node1] = new HashSet<RecordKey>();
            }

            if (!AdjacencyList.ContainsKey(node2))
            {
                AdjacencyList[node2] = new HashSet<RecordKey>();
            }

            // Add bidirectional edges
            AdjacencyList[node1].Add(node2);
            AdjacencyList[node2].Add(node1);

            // Store edge details with canonical key ordering
            var edgeKey = GetCanonicalEdgeKey(node1, node2);
            EdgeDetails[edgeKey] = details;
        }

        /// <summary>
        /// Bulk add nodes - optimized for adding many nodes at once
        /// </summary>
        public void AddNodesBulk(IEnumerable<(RecordKey node, IDictionary<string, object> recordData)> nodes)
        {
            foreach (var (node, recordData) in nodes)
            {
                AddNode(node, recordData);
            }
        }

        /// <summary>
        /// Bulk add edges - optimized for adding many edges at once
        /// </summary>
        public void AddEdgesBulk(IEnumerable<(RecordKey node1, RecordKey node2, MatchEdgeDetails details)> edges)
        {
            foreach (var (node1, node2, details) in edges)
            {
                AddEdge(node1, node2, details);
            }
        }

        /// <summary>
        /// Updates node metadata degree counts after all edges are added
        /// Call this once after bulk operations for best performance
        /// </summary>
        public void UpdateNodeDegrees()
        {
            foreach (var kvp in AdjacencyList)
            {
                var node = kvp.Key;
                var neighbors = kvp.Value;

                if (NodeMetadata.TryGetValue(node, out var metadata))
                {
                    metadata.DegreeCount = neighbors.Count;
                }
            }
        }

        /// <summary>
        /// Gets the canonical edge key (smaller node first for consistent ordering)
        /// </summary>
        private (RecordKey, RecordKey) GetCanonicalEdgeKey(RecordKey node1, RecordKey node2)
        {
            return node1.CompareTo(node2) <= 0 ? (node1, node2) : (node2, node1);
        }

        /// <summary>
        /// Finds connected components using iterative DFS (avoids stack overflow on large graphs)
        /// </summary>
        public List<HashSet<RecordKey>> GetConnectedComponents()
        {
            var visited = new HashSet<RecordKey>();
            var components = new List<HashSet<RecordKey>>();

            foreach (var node in AdjacencyList.Keys)
            {
                if (!visited.Contains(node))
                {
                    var component = new HashSet<RecordKey>();
                    DFSIterative(node, visited, component);
                    components.Add(component);
                }
            }

            return components;
        }

        /// <summary>
        /// Iterative DFS to avoid stack overflow on large graphs
        /// </summary>
        private void DFSIterative(RecordKey startNode, HashSet<RecordKey> visited, HashSet<RecordKey> component)
        {
            var stack = new Stack<RecordKey>();
            stack.Push(startNode);

            while (stack.Count > 0)
            {
                var node = stack.Pop();

                if (visited.Contains(node))
                    continue;

                visited.Add(node);
                component.Add(node);

                if (AdjacencyList.TryGetValue(node, out var neighbors))
                {
                    foreach (var neighbor in neighbors)
                    {
                        if (!visited.Contains(neighbor))
                        {
                            stack.Push(neighbor);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets graph statistics for monitoring and debugging
        /// </summary>
        public GraphStatistics GetStatistics()
        {
            var stats = new GraphStatistics
            {
                TotalNodes = TotalNodes,
                TotalEdges = TotalEdges,
                CreatedAt = CreatedAt,
                ProjectId = ProjectId
            };

            if (AdjacencyList.Any())
            {
                var degrees = AdjacencyList.Values.Select(n => n.Count).ToList();
                stats.AverageDegree = degrees.Average();
                stats.MinDegree = degrees.Min();
                stats.MaxDegree = degrees.Max();
                stats.MedianDegree = CalculateMedian(degrees);
            }

            // Calculate edge score statistics
            if (EdgeDetails.Any())
            {
                var scores = EdgeDetails.Values.Select(e => e.MaxScore).ToList();
                stats.AverageEdgeScore = scores.Average();
                stats.MinEdgeScore = scores.Min();
                stats.MaxEdgeScore = scores.Max();
            }

            // Memory estimate (rough)
            stats.EstimatedMemoryBytes = EstimateMemoryUsage();

            return stats;
        }

        private double CalculateMedian(List<int> values)
        {
            var sorted = values.OrderBy(v => v).ToList();
            int count = sorted.Count;

            if (count == 0)
                return 0;

            if (count % 2 == 0)
                return (sorted[count / 2 - 1] + sorted[count / 2]) / 2.0;
            else
                return sorted[count / 2];
        }

        private long EstimateMemoryUsage()
        {
            long bytes = 0;

            // Adjacency list
            bytes += AdjacencyList.Count * 64; // Dictionary overhead
            bytes += AdjacencyList.Sum(kvp => kvp.Value.Count * 32); // HashSet entries

            // Edge details
            bytes += EdgeDetails.Count * (64 + 128); // Key + value overhead

            // Node metadata
            bytes += NodeMetadata.Count * 256; // Rough estimate per node

            return bytes;
        }
    }
    /// <summary>
    /// Statistics about the match graph structure
    /// </summary>
    public class GraphStatistics
    {
        public int TotalNodes { get; set; }
        public int TotalEdges { get; set; }
        public double AverageDegree { get; set; }
        public int MinDegree { get; set; }
        public int MaxDegree { get; set; }
        public double MedianDegree { get; set; }
        public double AverageEdgeScore { get; set; }
        public double MinEdgeScore { get; set; }
        public double MaxEdgeScore { get; set; }
        public long EstimatedMemoryBytes { get; set; }
        public DateTime CreatedAt { get; set; }
        public Guid ProjectId { get; set; }

        public string EstimatedMemoryMB => $"{EstimatedMemoryBytes / (1024.0 * 1024.0):F1} MB";
    }
}
