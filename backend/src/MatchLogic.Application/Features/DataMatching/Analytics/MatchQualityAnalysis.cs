using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Analytics
{
    public class MatchQualityAnalysis
    {
        public DefinitionPerformanceReport DefinitionPerformance { get; set; }
        public DataSourceAnalysis DataSourceAnalysis { get; set; }
        public MatchDistributionReport MatchDistribution { get; set; }
        public QualityMetrics QualityMetrics { get; set; }
        public List<AnomalyReport> Anomalies { get; set; }
        public DateTime AnalyzedAt { get; set; }

        public static MatchQualityAnalysis Generate(
            MatchGraph matchGraph,
            MatchDefinitionCollection definitions = null)
        {
            var analysis = new MatchQualityAnalysis
            {
                AnalyzedAt = DateTime.UtcNow,
                DefinitionPerformance = AnalyzeDefinitionPerformance(matchGraph, definitions),
                DataSourceAnalysis = AnalyzeDataSources(matchGraph),
                MatchDistribution = AnalyzeMatchDistribution(matchGraph),
                QualityMetrics = CalculateQualityMetrics(matchGraph),
                Anomalies = DetectAnomalies(matchGraph)
            };

            return analysis;
        }

        private static DefinitionPerformanceReport AnalyzeDefinitionPerformance(
            MatchGraph matchGraph,
            MatchDefinitionCollection definitions)
        {
            var report = new DefinitionPerformanceReport
            {
                DefinitionMetrics = new List<DefinitionMetric>()
            };

            // Group edges by definition
            var edgesByDefinition = new Dictionary<int, List<MatchEdgeDetails>>();

            foreach (var edge in matchGraph.EdgeDetails.Values)
            {
                foreach (var defIndex in edge.MatchDefinitionIndices)
                {
                    if (!edgesByDefinition.ContainsKey(defIndex))
                        edgesByDefinition[defIndex] = new List<MatchEdgeDetails>();

                    edgesByDefinition[defIndex].Add(edge);
                }
            }

            // Calculate metrics for each definition
            foreach (var kvp in edgesByDefinition)
            {
                var defIndex = kvp.Key;
                var edges = kvp.Value;

                var metric = new DefinitionMetric
                {
                    DefinitionIndex = defIndex,
                    DefinitionName = GetDefinitionName(defIndex, definitions),
                    MatchCount = edges.Count,
                    AverageScore = edges.Average(e => e.ScoresByDefinition[defIndex].WeightedScore),
                    MinScore = edges.Min(e => e.ScoresByDefinition[defIndex].WeightedScore),
                    MaxScore = edges.Max(e => e.ScoresByDefinition[defIndex].WeightedScore),
                    ScoreStdDev = CalculateStandardDeviation(
                        edges.Select(e => e.ScoresByDefinition[defIndex].WeightedScore)),
                    Precision = CalculatePrecision(edges, defIndex),
                    Coverage = (double)edges.Count / matchGraph.TotalEdges
                };

                // Score distribution
                metric.ScoreDistribution = edges
                    .GroupBy(e => Math.Floor(e.ScoresByDefinition[defIndex].WeightedScore * 10) / 10)
                    .ToDictionary(g => g.Key, g => g.Count());

                report.DefinitionMetrics.Add(metric);
            }

            // Rank definitions by effectiveness
            report.DefinitionMetrics = report.DefinitionMetrics
                .OrderByDescending(m => m.MatchCount * m.AverageScore)
                .ToList();

            return report;
        }

        private static DataSourceAnalysis AnalyzeDataSources(MatchGraph matchGraph)
        {
            var analysis = new DataSourceAnalysis
            {
                DataSourceMetrics = new List<DataSourceMetric>()
            };

            // Group nodes by data source
            var nodesBySource = matchGraph.NodeMetadata.Values
                .GroupBy(n => n.RecordKey.DataSourceId);

            foreach (var group in nodesBySource)
            {
                var dataSourceId = group.Key;
                var nodes = group.ToList();

                var metric = new DataSourceMetric
                {
                    DataSourceId = dataSourceId,
                    RecordCount = nodes.Count,
                    MatchedRecordCount = nodes.Count(n => n.DegreeCount > 0),
                    UnmatchedRecordCount = nodes.Count(n => n.DegreeCount == 0),
                    MatchRate = nodes.Count > 0
                        ? (double)nodes.Count(n => n.DegreeCount > 0) / nodes.Count
                        : 0,
                    AverageConnections = nodes.Average(n => n.DegreeCount),
                    MaxConnections = nodes.Any() ? nodes.Max(n => n.DegreeCount) : 0
                };

                // Cross-source matches
                var crossSourceMatches = 0;
                foreach (var node in nodes)
                {
                    if (matchGraph.AdjacencyList.TryGetValue(node.RecordKey, out var connections))
                    {
                        crossSourceMatches += connections.Count(c => c.DataSourceId != dataSourceId);
                    }
                }
                metric.CrossSourceMatches = crossSourceMatches;

                analysis.DataSourceMetrics.Add(metric);
            }

            // Calculate cross-source matrix
            analysis.CrossSourceMatrix = CalculateCrossSourceMatrix(matchGraph);

            return analysis;
        }

        private static Dictionary<(Guid, Guid), int> CalculateCrossSourceMatrix(MatchGraph matchGraph)
        {
            var matrix = new Dictionary<(Guid, Guid), int>();

            foreach (var edge in matchGraph.EdgeDetails.Keys)
            {
                var ds1 = edge.Item1.DataSourceId;
                var ds2 = edge.Item2.DataSourceId;

                var key = ds1.CompareTo(ds2) <= 0 ? (ds1, ds2) : (ds2, ds1);

                if (matrix.ContainsKey(key))
                    matrix[key]++;
                else
                    matrix[key] = 1;
            }

            return matrix;
        }

        private static MatchDistributionReport AnalyzeMatchDistribution(MatchGraph matchGraph)
        {
            var report = new MatchDistributionReport();

            // Score distribution
            var allScores = matchGraph.EdgeDetails.Values.Select(e => e.MaxScore).ToList();

            report.ScoreHistogram = new Dictionary<string, int>
            {
                ["0.0-0.5"] = allScores.Count(s => s < 0.5),
                ["0.5-0.6"] = allScores.Count(s => s >= 0.5 && s < 0.6),
                ["0.6-0.7"] = allScores.Count(s => s >= 0.6 && s < 0.7),
                ["0.7-0.8"] = allScores.Count(s => s >= 0.7 && s < 0.8),
                ["0.8-0.9"] = allScores.Count(s => s >= 0.8 && s < 0.9),
                ["0.9-1.0"] = allScores.Count(s => s >= 0.9)
            };

            // Component size distribution
            var components = matchGraph.GetConnectedComponents();
            report.ComponentSizeDistribution = components
                .GroupBy(c => c.Count)
                .ToDictionary(g => g.Key, g => g.Count());

            // Statistics
            report.AverageScore = allScores.Any() ? allScores.Average() : 0;
            report.MedianScore = CalculateMedian(allScores);
            report.ScoreStdDev = CalculateStandardDeviation(allScores);

            return report;
        }

        private static QualityMetrics CalculateQualityMetrics(MatchGraph matchGraph)
        {
            var metrics = new QualityMetrics();

            var components = matchGraph.GetConnectedComponents();

            // Completeness: ratio of matched records to total
            metrics.Completeness = matchGraph.TotalNodes > 0
                ? (double)matchGraph.NodeMetadata.Values.Count(n => n.DegreeCount > 0) / matchGraph.TotalNodes
                : 0;

            // Connectivity: average degree / potential connections
            var avgDegree = matchGraph.NodeMetadata.Values.Any()
                ? matchGraph.NodeMetadata.Values.Average(n => n.DegreeCount)
                : 0;
            metrics.Connectivity = matchGraph.TotalNodes > 1
                ? avgDegree / (matchGraph.TotalNodes - 1)
                : 0;

            // Clustering coefficient
            metrics.ClusteringCoefficient = CalculateClusteringCoefficient(matchGraph);

            // Component metrics
            metrics.LargestComponentSize = components.Any() ? components.Max(c => c.Count) : 0;
            metrics.SingletonCount = components.Count(c => c.Count == 1);
            metrics.AverageComponentSize = components.Any() ? components.Average(c => c.Count) : 0;

            return metrics;
        }

        private static List<AnomalyReport> DetectAnomalies(MatchGraph matchGraph)
        {
            var anomalies = new List<AnomalyReport>();

            // Detect unusually large components
            var components = matchGraph.GetConnectedComponents();
            var avgSize = components.Any() ? components.Average(c => c.Count) : 0;
            var stdDev = CalculateStandardDeviation(components.Select(c => (double)c.Count));

            foreach (var component in components.Where(c => c.Count > avgSize + 2 * stdDev))
            {
                anomalies.Add(new AnomalyReport
                {
                    Type = "LargeComponent",
                    Severity = "Warning",
                    Description = $"Unusually large component with {component.Count} records (avg: {avgSize:F1})",
                    AffectedRecords = component.Select(r => r.ToString()).ToList()
                });
            }

            // Detect low-quality matches
            var lowQualityThreshold = 0.6;
            var lowQualityEdges = matchGraph.EdgeDetails
                .Where(e => e.Value.MaxScore < lowQualityThreshold)
                .ToList();

            if (lowQualityEdges.Count > matchGraph.TotalEdges * 0.3)
            {
                anomalies.Add(new AnomalyReport
                {
                    Type = "LowQualityMatches",
                    Severity = "Warning",
                    Description = $"{lowQualityEdges.Count} matches ({100.0 * lowQualityEdges.Count / matchGraph.TotalEdges:F1}%) have score below {lowQualityThreshold}",
                    Metrics = new Dictionary<string, object>
                    {
                        ["Count"] = lowQualityEdges.Count,
                        ["Percentage"] = 100.0 * lowQualityEdges.Count / matchGraph.TotalEdges
                    }
                });
            }

            return anomalies;
        }

        // Helper methods
        private static string GetDefinitionName(int index, MatchDefinitionCollection definitions)
        {
            if (definitions?.Definitions != null && index < definitions.Definitions.Count)
            {
                return definitions.Definitions[index].Name ?? $"Definition_{index}";
            }
            return $"Definition_{index}";
        }

        private static double CalculateStandardDeviation(IEnumerable<double> values)
        {
            var list = values.ToList();
            if (!list.Any()) return 0;

            var avg = list.Average();
            var sum = list.Sum(v => Math.Pow(v - avg, 2));
            return Math.Sqrt(sum / list.Count);
        }

        private static double CalculateMedian(List<double> values)
        {
            if (!values.Any()) return 0;

            var sorted = values.OrderBy(v => v).ToList();
            var mid = sorted.Count / 2;

            return sorted.Count % 2 == 0
                ? (sorted[mid - 1] + sorted[mid]) / 2
                : sorted[mid];
        }

        private static double CalculatePrecision(List<MatchEdgeDetails> edges, int definitionIndex)
        {
            // Precision: ratio of high-confidence matches
            var highConfidenceThreshold = 0.8;
            var highConfidenceCount = edges.Count(e =>
                e.ScoresByDefinition[definitionIndex].WeightedScore >= highConfidenceThreshold);

            return edges.Count > 0 ? (double)highConfidenceCount / edges.Count : 0;
        }

        private static double CalculateClusteringCoefficient(MatchGraph graph)
        {
            double totalCoefficient = 0;
            int nodeCount = 0;

            foreach (var node in graph.AdjacencyList.Keys)
            {
                if (!graph.AdjacencyList.TryGetValue(node, out var neighbors) || neighbors.Count < 2)
                    continue;

                var neighborList = neighbors.ToList();
                int triangles = 0;
                int possibleTriangles = neighborList.Count * (neighborList.Count - 1) / 2;

                for (int i = 0; i < neighborList.Count; i++)
                {
                    for (int j = i + 1; j < neighborList.Count; j++)
                    {
                        if (graph.AdjacencyList[neighborList[i]].Contains(neighborList[j]))
                            triangles++;
                    }
                }

                totalCoefficient += (double)triangles / possibleTriangles;
                nodeCount++;
            }

            return nodeCount > 0 ? totalCoefficient / nodeCount : 0;
        }
    }

    // Supporting classes
    public class DefinitionPerformanceReport
    {
        public List<DefinitionMetric> DefinitionMetrics { get; set; }
    }

    public class DefinitionMetric
    {
        public int DefinitionIndex { get; set; }
        public string DefinitionName { get; set; }
        public int MatchCount { get; set; }
        public double AverageScore { get; set; }
        public double MinScore { get; set; }
        public double MaxScore { get; set; }
        public double ScoreStdDev { get; set; }
        public double Precision { get; set; }
        public double Coverage { get; set; }
        public Dictionary<double, int> ScoreDistribution { get; set; }
    }

    public class DataSourceAnalysis
    {
        public List<DataSourceMetric> DataSourceMetrics { get; set; }
        public Dictionary<(Guid, Guid), int> CrossSourceMatrix { get; set; }
    }

    public class DataSourceMetric
    {
        public Guid DataSourceId { get; set; }
        public int RecordCount { get; set; }
        public int MatchedRecordCount { get; set; }
        public int UnmatchedRecordCount { get; set; }
        public double MatchRate { get; set; }
        public double AverageConnections { get; set; }
        public int MaxConnections { get; set; }
        public int CrossSourceMatches { get; set; }
    }

    public class MatchDistributionReport
    {
        public Dictionary<string, int> ScoreHistogram { get; set; }
        public Dictionary<int, int> ComponentSizeDistribution { get; set; }
        public double AverageScore { get; set; }
        public double MedianScore { get; set; }
        public double ScoreStdDev { get; set; }
    }

    public class QualityMetrics
    {
        public double Completeness { get; set; }
        public double Connectivity { get; set; }
        public double ClusteringCoefficient { get; set; }
        public int LargestComponentSize { get; set; }
        public int SingletonCount { get; set; }
        public double AverageComponentSize { get; set; }
    }

    public class AnomalyReport
    {
        public string Type { get; set; }
        public string Severity { get; set; }
        public string Description { get; set; }
        public List<string> AffectedRecords { get; set; }
        public Dictionary<string, object> Metrics { get; set; }
    }
}
