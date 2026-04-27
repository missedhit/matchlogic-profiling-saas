using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Visualization
{
    public class NetworkVisualization
    {
        public List<NetworkNode> Nodes { get; set; }
        public List<NetworkEdge> Edges { get; set; }
        public NetworkMetadata Metadata { get; set; }
        public VisualizationConfig Config { get; set; }

        public static NetworkVisualization FromMatchGraph(
            MatchGraph matchGraph,
            IDataSourceIndexMapper indexMapper = null)
        {
            var visualization = new NetworkVisualization
            {
                Nodes = new List<NetworkNode>(),
                Edges = new List<NetworkEdge>(),
                Config = VisualizationConfig.Default()
            };

            // Create nodes
            foreach (var nodeMeta in matchGraph.NodeMetadata.Values)
            {
                var node = CreateNetworkNode(nodeMeta, indexMapper);
                visualization.Nodes.Add(node);
            }

            // Create edges
            foreach (var edge in matchGraph.EdgeDetails)
            {
                var networkEdge = CreateNetworkEdge(edge.Key, edge.Value);
                visualization.Edges.Add(networkEdge);
            }

            // Calculate layout hints
            CalculateLayoutHints(visualization, matchGraph);

            // Set metadata
            visualization.Metadata = new NetworkMetadata
            {
                TotalNodes = visualization.Nodes.Count,
                TotalEdges = visualization.Edges.Count,
                ConnectedComponents = matchGraph.GetConnectedComponents().Count,
                GeneratedAt = DateTime.UtcNow,
            };

            return visualization;
        }

        private static NetworkNode CreateNetworkNode(
            NodeMetadata metadata,
            IDataSourceIndexMapper indexMapper)
        {
            var node = new NetworkNode
            {
                Id = metadata.RecordKey.ToString(),
                DataSourceId = metadata.RecordKey.DataSourceId,
                RowNumber = metadata.RecordKey.RowNumber,
                Label = GenerateNodeLabel(metadata.RecordData),
                Tooltip = GenerateNodeTooltip(metadata.RecordData),
                Size = CalculateNodeSize(metadata),
                Color = GetDataSourceColor(metadata.RecordKey.DataSourceId),
                Type = "record",
                Properties = ExtractKeyProperties(metadata.RecordData)
            };

            // Set data source name if mapper available
            if (indexMapper != null &&
                indexMapper.TryGetDataSourceName(metadata.RecordKey.DataSourceId, out var dsName))
            {
                node.DataSourceName = dsName ?? String.Empty;
            }

            return node;
        }

        private static string GenerateNodeLabel(IDictionary<string, object> record)
        {
            // Try common name fields
            var nameFields = new[] { "Name", "name", "FullName", "Title", "CompanyName", "Id", "ID" };

            foreach (var field in nameFields)
            {
                if (record.TryGetValue(field, out var value) && value != null)
                {
                    var strValue = value.ToString();
                    if (!string.IsNullOrWhiteSpace(strValue))
                    {
                        // Truncate if too long
                        return strValue.Length > 30
                            ? strValue.Substring(0, 27) + "..."
                            : strValue;
                    }
                }
            }

            // Fallback to first non-null value
            var firstValue = record.Values.FirstOrDefault(v => v != null)?.ToString();
            return firstValue?.Length > 30
                ? firstValue.Substring(0, 27) + "..."
                : firstValue ?? "Record";
        }

        private static string GenerateNodeTooltip(IDictionary<string, object> record)
        {
            var tooltip = new StringBuilder();
            var count = 0;

            foreach (var kvp in record.Take(10)) // Limit to 10 fields
            {
                if (kvp.Value != null)
                {
                    tooltip.AppendLine($"{kvp.Key}: {kvp.Value}");
                    count++;
                }
            }

            if (record.Count > 10)
            {
                tooltip.AppendLine($"... and {record.Count - 10} more fields");
            }

            return tooltip.ToString();
        }

        private static Dictionary<string, object> ExtractKeyProperties(IDictionary<string, object> record)
        {
            var keyFields = new[] { "Id", "ID", "Name", "Email", "Phone", "Address" };
            var properties = new Dictionary<string, object>();

            foreach (var field in keyFields)
            {
                if (record.TryGetValue(field, out var value) && value != null)
                {
                    properties[field] = value;
                }
            }

            return properties;
        }

        private static int CalculateNodeSize(NodeMetadata metadata)
        {
            // Base size + connections
            var baseSize = 10;
            var connectionBonus = metadata.DegreeCount * 2;
            return Math.Min(baseSize + connectionBonus, 50); // Cap at 50
        }

        private static string GetDataSourceColor(Guid dataSourceId)
        {
            // Generate consistent color from GUID
            var hash = dataSourceId.GetHashCode();
            var colors = new[]
            {
                "#3498db", // Blue
                "#e74c3c", // Red
                "#2ecc71", // Green
                "#f39c12", // Orange
                "#9b59b6", // Purple
                "#1abc9c", // Turquoise
                "#34495e", // Dark Gray
                "#e67e22"  // Carrot
            };

            return colors[Math.Abs(hash) % colors.Length];
        }

        private static NetworkEdge CreateNetworkEdge(
            (RecordKey, RecordKey) edgeKey,
            MatchEdgeDetails details)
        {
            return new NetworkEdge
            {
                Id = $"{edgeKey.Item1}_{edgeKey.Item2}",
                Source = edgeKey.Item1.ToString(),
                Target = edgeKey.Item2.ToString(),
                Weight = details.MaxScore,
                Strength = ConvertScoreToStrength(details.MaxScore),
                Color = GetEdgeColor(details.MaxScore),
                Style = details.MaxScore > 0.95 ? "solid" : "dashed",
                Label = $"{details.MaxScore:P0}",
                MatchDetails = new EdgeMatchDetails
                {
                    PairId = details.PairId,
                    MatchDefinitions = details.MatchDefinitionIndices,
                    ScoreBreakdown = details.ScoresByDefinition
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value.WeightedScore)
                }
            };
        }

        private static string ConvertScoreToStrength(double score)
        {
            if (score >= 0.95) return "very-strong";
            if (score >= 0.85) return "strong";
            if (score >= 0.70) return "medium";
            if (score >= 0.50) return "weak";
            return "very-weak";
        }

        private static string GetEdgeColor(double score)
        {
            // Gradient from red (low) to green (high)
            if (score >= 0.9) return "#27ae60";  // Green
            if (score >= 0.7) return "#f39c12";  // Orange
            if (score >= 0.5) return "#e67e22";  // Dark Orange
            return "#e74c3c";  // Red
        }

        private static void CalculateLayoutHints(NetworkVisualization viz, MatchGraph graph)
        {
            var components = graph.GetConnectedComponents();
            var componentIndex = 0;

            foreach (var component in components)
            {
                var centerX = componentIndex * 200;
                var centerY = 0;
                var angle = 0.0;
                var radius = Math.Min(component.Count * 10, 150);

                foreach (var recordKey in component)
                {
                    var node = viz.Nodes.FirstOrDefault(n => n.Id == recordKey.ToString());
                    if (node != null)
                    {
                        // Arrange in circle for each component
                        node.X = centerX + radius * Math.Cos(angle);
                        node.Y = centerY + radius * Math.Sin(angle);
                        node.ComponentId = componentIndex;

                        angle += (2 * Math.PI) / component.Count;
                    }
                }

                componentIndex++;
            }
        }

        // Export methods for different visualization libraries
        public string ToD3Json()
        {
            var d3Data = new
            {
                nodes = Nodes.Select(n => new
                {
                    id = n.Id,
                    label = n.Label,
                    group = n.DataSourceId.ToString(),
                    size = n.Size,
                    color = n.Color,
                    x = n.X,
                    y = n.Y
                }),
                links = Edges.Select(e => new
                {
                    source = e.Source,
                    target = e.Target,
                    value = e.Weight,
                    color = e.Color
                })
            };

            return System.Text.Json.JsonSerializer.Serialize(d3Data);
        }

        public string ToCytoscapeJson()
        {
            var elements = new List<object>();

            // Add nodes
            elements.AddRange(Nodes.Select(n => new
            {
                data = new
                {
                    id = n.Id,
                    label = n.Label,
                    size = n.Size,
                    color = n.Color
                },
                position = new { x = n.X, y = n.Y }
            }));

            // Add edges
            elements.AddRange(Edges.Select(e => new
            {
                data = new
                {
                    id = e.Id,
                    source = e.Source,
                    target = e.Target,
                    weight = e.Weight,
                    label = e.Label
                }
            }));

            return System.Text.Json.JsonSerializer.Serialize(new { elements });
        }
    }

    public class NetworkNode
    {
        public string Id { get; set; }
        public Guid DataSourceId { get; set; }
        public string DataSourceName { get; set; }
        public int RowNumber { get; set; }
        public string Label { get; set; }
        public string Tooltip { get; set; }
        public int Size { get; set; }
        public string Color { get; set; }
        public string Type { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
        public int ComponentId { get; set; }
        public Dictionary<string, object> Properties { get; set; }
    }

    public class NetworkEdge
    {
        public string Id { get; set; }
        public string Source { get; set; }
        public string Target { get; set; }
        public double Weight { get; set; }
        public string Strength { get; set; }
        public string Color { get; set; }
        public string Style { get; set; }
        public string Label { get; set; }
        public EdgeMatchDetails MatchDetails { get; set; }
    }

    public class EdgeMatchDetails
    {
        public long PairId { get; set; }
        public List<int> MatchDefinitions { get; set; }
        public Dictionary<int, double> ScoreBreakdown { get; set; }
    }

    public class NetworkMetadata
    {
        public int TotalNodes { get; set; }
        public int TotalEdges { get; set; }
        public int ConnectedComponents { get; set; }
        public DateTime GeneratedAt { get; set; }
        public Guid JobId { get; set; }
    }

    public class VisualizationConfig
    {
        public string LayoutType { get; set; }
        public bool ShowLabels { get; set; }
        public bool ShowEdgeLabels { get; set; }
        public int MinNodeSize { get; set; }
        public int MaxNodeSize { get; set; }

        public static VisualizationConfig Default()
        {
            return new VisualizationConfig
            {
                LayoutType = "force-directed",
                ShowLabels = true,
                ShowEdgeLabels = false,
                MinNodeSize = 10,
                MaxNodeSize = 50
            };
        }
    }
}
