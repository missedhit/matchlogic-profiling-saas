using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.EntityResolution
{
    public class EntityResolutionView
    {
        public List<ResolvedEntity> Entities { get; set; }
        public EntityResolutionStatistics Statistics { get; set; }
        public DateTime GeneratedAt { get; set; }
        public Guid JobId { get; set; }

        public static EntityResolutionView FromMatchGraph(MatchGraph matchGraph)
        {
            var view = new EntityResolutionView
            {
                GeneratedAt = DateTime.UtcNow,
                Entities = new List<ResolvedEntity>()
            };

            var components = matchGraph.GetConnectedComponents();
            var entityId = 1;

            foreach (var component in components)
            {
                var entity = CreateResolvedEntity(entityId++, component, matchGraph);
                view.Entities.Add(entity);
            }

            view.Statistics = CalculateStatistics(view.Entities, matchGraph);
            return view;
        }

        private static ResolvedEntity CreateResolvedEntity(
            int entityId,
            HashSet<RecordKey> component,
            MatchGraph matchGraph)
        {
            var entity = new ResolvedEntity
            {
                EntityId = entityId,
                MemberRecords = new List<EntityMemberRecord>(),
                MasterRecord = new Dictionary<string, object>(),
                ConfidenceScore = 0.0
            };

            // Collect all member records
            foreach (var recordKey in component)
            {
                if (matchGraph.NodeMetadata.TryGetValue(recordKey, out var metadata))
                {
                    var member = new EntityMemberRecord
                    {
                        RecordKey = recordKey,
                        DataSourceId = recordKey.DataSourceId,
                        RowNumber = recordKey.RowNumber,
                        RecordData = metadata.RecordData,
                        ContributionScore = CalculateContributionScore(recordKey, component, matchGraph)
                    };
                    entity.MemberRecords.Add(member);
                }
            }

            // Build master record using best values
            entity.MasterRecord = BuildMasterRecord(entity.MemberRecords, matchGraph);

            // Calculate entity confidence
            entity.ConfidenceScore = CalculateEntityConfidence(component, matchGraph);

            // Determine data sources
            entity.DataSourceIds = entity.MemberRecords
                .Select(m => m.DataSourceId)
                .Distinct()
                .ToList();

            entity.RecordCount = entity.MemberRecords.Count;
            entity.IsCrossSource = entity.DataSourceIds.Count > 1;

            return entity;
        }

        private static Dictionary<string, object> BuildMasterRecord(
            List<EntityMemberRecord> members,
            MatchGraph matchGraph)
        {
            var masterRecord = new Dictionary<string, object>();

            // Get all unique field names
            var allFields = members
                .SelectMany(m => m.RecordData.Keys)
                .Distinct()
                .ToList();

            foreach (var fieldName in allFields)
            {
                // Find the best value for this field
                var bestValue = SelectBestFieldValue(fieldName, members);
                if (bestValue != null)
                {
                    masterRecord[fieldName] = bestValue;
                }
            }

            // Add metadata
            masterRecord["_entity_metadata"] = new Dictionary<string, object>
            {
                ["created_at"] = DateTime.UtcNow,
                ["member_count"] = members.Count,
                ["source_systems"] = members.Select(m => m.DataSourceId).Distinct().ToArray()
            };

            return masterRecord;
        }

        private static object SelectBestFieldValue(string fieldName, List<EntityMemberRecord> members)
        {
            var fieldValues = new List<(object value, double score)>();

            foreach (var member in members)
            {
                if (member.RecordData.TryGetValue(fieldName, out var value) && value != null)
                {
                    // Score based on contribution score and data completeness
                    var score = member.ContributionScore;

                    // Boost score for non-empty, non-whitespace strings
                    if (value is string strValue && !string.IsNullOrWhiteSpace(strValue))
                    {
                        score += 0.1;
                        // Longer, more complete values get higher scores
                        score += Math.Min(strValue.Length / 1000.0, 0.1);
                    }

                    fieldValues.Add((value, score));
                }
            }

            // Return the value with the highest score
            return fieldValues
                .OrderByDescending(fv => fv.score)
                .FirstOrDefault().value;
        }

        private static double CalculateContributionScore(
            RecordKey recordKey,
            HashSet<RecordKey> component,
            MatchGraph matchGraph)
        {
            if (!matchGraph.AdjacencyList.TryGetValue(recordKey, out var connections))
                return 0.0;

            // Base score on connectivity and match quality
            double totalScore = 0.0;
            int connectionCount = 0;

            foreach (var connectedRecord in connections)
            {
                if (component.Contains(connectedRecord))
                {
                    var edgeKey = GetEdgeKey(recordKey, connectedRecord);
                    if (matchGraph.EdgeDetails.TryGetValue(edgeKey, out var edge))
                    {
                        totalScore += edge.MaxScore;
                        connectionCount++;
                    }
                }
            }

            return connectionCount > 0 ? totalScore / connectionCount : 0.0;
        }

        private static double CalculateEntityConfidence(
            HashSet<RecordKey> component,
            MatchGraph matchGraph)
        {
            if (component.Count <= 1)
                return 1.0;

            double totalScore = 0.0;
            int edgeCount = 0;

            // Calculate average edge score within the component
            foreach (var node1 in component)
            {
                foreach (var node2 in component)
                {
                    if (node1.CompareTo(node2) < 0)
                    {
                        var edgeKey = (node1, node2);
                        if (matchGraph.EdgeDetails.TryGetValue(edgeKey, out var edge))
                        {
                            totalScore += edge.MaxScore;
                            edgeCount++;
                        }
                    }
                }
            }

            return edgeCount > 0 ? totalScore / edgeCount : 0.0;
        }

        private static (RecordKey, RecordKey) GetEdgeKey(RecordKey key1, RecordKey key2)
        {
            return key1.CompareTo(key2) <= 0 ? (key1, key2) : (key2, key1);
        }

        private static EntityResolutionStatistics CalculateStatistics(
            List<ResolvedEntity> entities,
            MatchGraph matchGraph)
        {
            return new EntityResolutionStatistics
            {
                TotalEntities = entities.Count,
                SingletonEntities = entities.Count(e => e.RecordCount == 1),
                MergedEntities = entities.Count(e => e.RecordCount > 1),
                CrossSourceEntities = entities.Count(e => e.IsCrossSource),
                AverageEntitySize = entities.Any() ? entities.Average(e => e.RecordCount) : 0,
                LargestEntitySize = entities.Any() ? entities.Max(e => e.RecordCount) : 0,
                AverageConfidence = entities.Any() ? entities.Average(e => e.ConfidenceScore) : 0,
                TotalRecords = matchGraph.TotalNodes,
                TotalMatches = matchGraph.TotalEdges
            };
        }
    }

    public class ResolvedEntity
    {
        public int EntityId { get; set; }
        public IDictionary<string, object> MasterRecord { get; set; }
        public List<EntityMemberRecord> MemberRecords { get; set; }
        public double ConfidenceScore { get; set; }
        public int RecordCount { get; set; }
        public List<Guid> DataSourceIds { get; set; }
        public bool IsCrossSource { get; set; }
    }

    public class EntityMemberRecord
    {
        public RecordKey RecordKey { get; set; }
        public Guid DataSourceId { get; set; }
        public int RowNumber { get; set; }
        public IDictionary<string, object> RecordData { get; set; }
        public double ContributionScore { get; set; }
    }

    public class EntityResolutionStatistics
    {
        public int TotalEntities { get; set; }
        public int SingletonEntities { get; set; }
        public int MergedEntities { get; set; }
        public int CrossSourceEntities { get; set; }
        public double AverageEntitySize { get; set; }
        public int LargestEntitySize { get; set; }
        public double AverageConfidence { get; set; }
        public int TotalRecords { get; set; }
        public int TotalMatches { get; set; }
    }
}
