using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.EntityResolution
{
    public class EnhancedEntityResolution
    {
        private readonly EntityResolutionConfig _config;
        private readonly ILogger<EnhancedEntityResolution> _logger;

        public EnhancedEntityResolution(
            EntityResolutionConfig config,
            ILogger<EnhancedEntityResolution> logger)
        {
            _config = config ?? EntityResolutionConfig.Default();
            _logger = logger;
        }

        /// <summary>
        /// Create entities from MatchGraph using graph components
        /// </summary>
        public List<EnhacedResolvedEntity> ResolveFromGraph(MatchGraph matchGraph)
        {
            var entities = new List<EnhacedResolvedEntity>();
            var components = matchGraph.GetConnectedComponents();
            var entityId = 1;

            foreach (var component in components)
            {
                if (component.Count < _config.MinRecordsPerEntity)
                    continue;

                var entity = CreateEntityFromComponent(
                    entityId++,
                    component,
                    matchGraph);

                entities.Add(entity);
            }

            _logger.LogInformation("Resolved {Count} entities from graph", entities.Count);
            return entities;
        }

        /// <summary>
        /// Create entities from MatchGroups
        /// </summary>
        public List<EnhacedResolvedEntity> ResolveFromGroups(List<MatchGroup> matchGroups)
        {
            var entities = new List<EnhacedResolvedEntity>();
            var entityId = 1;

            foreach (var group in matchGroups)
            {
                if (group.Records.Count < _config.MinRecordsPerEntity)
                    continue;

                var entity = CreateEntityFromGroup(entityId++, group);
                entities.Add(entity);
            }

            _logger.LogInformation("Resolved {Count} entities from groups", entities.Count);
            return entities;
        }

        private EnhacedResolvedEntity CreateEntityFromComponent(
            int entityId,
            HashSet<RecordKey> component,
            MatchGraph matchGraph)
        {
            var memberRecords = new List<IDictionary<string, object>>();

            foreach (var recordKey in component)
            {
                if (matchGraph.NodeMetadata.TryGetValue(recordKey, out var metadata))
                {
                    memberRecords.Add(metadata.RecordData);
                }
            }

            var masterRecord = CreateMasterRecord(memberRecords);

            return new EnhacedResolvedEntity
            {
                EntityId = entityId.ToString(),
                MasterRecord = masterRecord,
                MemberRecords = memberRecords,
                RecordCount = memberRecords.Count,
                CreatedAt = DateTime.UtcNow,
                ResolutionMethod = _config.MasterRecordStrategy.ToString()
            };
        }

        private EnhacedResolvedEntity CreateEntityFromGroup(int entityId, MatchGroup group)
        {
            var masterRecord = CreateMasterRecord(group.Records);

            return new EnhacedResolvedEntity
            {
                EntityId = entityId.ToString(),
                GroupId = group.GroupId.ToString(),
                MasterRecord = masterRecord,
                MemberRecords = group.Records.ToList(),
                RecordCount = group.Records.Count,
                CreatedAt = DateTime.UtcNow,
                ResolutionMethod = _config.MasterRecordStrategy.ToString()
            };
        }

        private IDictionary<string, object> CreateMasterRecord(
            List<IDictionary<string, object>> memberRecords)
        {
            if (!memberRecords.Any())
                return new Dictionary<string, object>();

            return _config.MasterRecordStrategy switch
            {
                MasterRecordStrategy.FirstRecord =>
                    SelectFirstRecord(memberRecords),

                MasterRecordStrategy.MostCompleteRecord =>
                    SelectMostCompleteRecord(memberRecords),

                MasterRecordStrategy.MostRecentRecord =>
                    SelectMostRecentRecord(memberRecords),

                MasterRecordStrategy.FieldLevelMerge =>
                    MergeAtFieldLevel(memberRecords),

                MasterRecordStrategy.CustomStrategy =>
                    _config.CustomMergeFunction?.Invoke(memberRecords)
                        ?? SelectFirstRecord(memberRecords),

                _ => SelectFirstRecord(memberRecords)
            };
        }

        private IDictionary<string, object> SelectFirstRecord(
            List<IDictionary<string, object>> records)
        {
            var master = new Dictionary<string, object>(records.First());
            AddMetadata(master, records);
            return master;
        }

        private IDictionary<string, object> SelectMostCompleteRecord(
            List<IDictionary<string, object>> records)
        {
            var mostComplete = records
                .OrderByDescending(r => r.Count(kvp =>
                    kvp.Value != null &&
                    (kvp.Value is not string str || !string.IsNullOrWhiteSpace(str))))
                .First();

            var master = new Dictionary<string, object>(mostComplete);
            AddMetadata(master, records);
            return master;
        }

        private IDictionary<string, object> SelectMostRecentRecord(
            List<IDictionary<string, object>> records)
        {
            // Try to find records with timestamp fields
            var timestampFields = _config.TimestampFields;

            IDictionary<string, object> mostRecent = null;
            DateTime? mostRecentTime = null;

            foreach (var record in records)
            {
                DateTime? recordTime = null;

                foreach (var field in timestampFields)
                {
                    if (record.TryGetValue(field, out var value))
                    {
                        if (value is DateTime dt)
                            recordTime = dt;
                        else if (value is string str && DateTime.TryParse(str, out var parsed))
                            recordTime = parsed;

                        if (recordTime.HasValue)
                            break;
                    }
                }

                if (recordTime.HasValue && (!mostRecentTime.HasValue || recordTime > mostRecentTime))
                {
                    mostRecentTime = recordTime;
                    mostRecent = record;
                }
            }

            var master = new Dictionary<string, object>(mostRecent ?? records.First());
            AddMetadata(master, records);
            return master;
        }

        private IDictionary<string, object> MergeAtFieldLevel(
            List<IDictionary<string, object>> records)
        {
            var master = new Dictionary<string, object>();

            // Get all unique field names
            var allFields = records
                .SelectMany(r => r.Keys)
                .Distinct()
                .Where(k => !k.StartsWith("_")) // Skip metadata fields
                .ToList();

            foreach (var field in allFields)
            {
                var fieldValues = records
                    .Where(r => r.ContainsKey(field) && r[field] != null)
                    .Select(r => r[field])
                    .ToList();

                if (fieldValues.Any())
                {
                    // Apply field-level merge strategy
                    master[field] = _config.FieldMergeStrategy switch
                    {
                        FieldMergeStrategy.FirstNonNull =>
                            fieldValues.First(),

                        FieldMergeStrategy.LongestValue =>
                            fieldValues
                                .OrderByDescending(v => v?.ToString()?.Length ?? 0)
                                .First(),

                        FieldMergeStrategy.MostFrequent =>
                            fieldValues
                                .GroupBy(v => v?.ToString())
                                .OrderByDescending(g => g.Count())
                                .First()
                                .First(),

                        FieldMergeStrategy.Concatenate =>
                            string.Join(_config.ConcatenationSeparator,
                                fieldValues.Select(v => v?.ToString())),

                        _ => fieldValues.First()
                    };
                }
            }

            AddMetadata(master, records);
            return master;
        }

        private void AddMetadata(
            IDictionary<string, object> master,
            List<IDictionary<string, object>> memberRecords)
        {
            master["_entity_metadata"] = new Dictionary<string, object>
            {
                ["resolved_at"] = DateTime.UtcNow,
                ["member_count"] = memberRecords.Count,
                ["resolution_method"] = _config.MasterRecordStrategy.ToString(),
                ["field_merge_strategy"] = _config.FieldMergeStrategy.ToString()
            };
        }
    }

    /// <summary>
    /// Configuration for entity resolution
    /// </summary>
    public class EntityResolutionConfig
    {
        public MasterRecordStrategy MasterRecordStrategy { get; set; }
        public FieldMergeStrategy FieldMergeStrategy { get; set; }
        public int MinRecordsPerEntity { get; set; }
        public List<string> TimestampFields { get; set; }
        public string ConcatenationSeparator { get; set; }
        public Func<List<IDictionary<string, object>>, IDictionary<string, object>> CustomMergeFunction { get; set; }

        public static EntityResolutionConfig Default()
        {
            return new EntityResolutionConfig
            {
                MasterRecordStrategy = MasterRecordStrategy.MostCompleteRecord,
                FieldMergeStrategy = FieldMergeStrategy.FirstNonNull,
                MinRecordsPerEntity = 1,
                TimestampFields = new List<string>
                {
                    "UpdatedAt", "ModifiedAt", "LastModified",
                    "CreatedAt", "Timestamp", "Date"
                },
                ConcatenationSeparator = "; "
            };
        }

        public static EntityResolutionConfig FieldLevelMerge()
        {
            return new EntityResolutionConfig
            {
                MasterRecordStrategy = MasterRecordStrategy.FieldLevelMerge,
                FieldMergeStrategy = FieldMergeStrategy.MostFrequent,
                MinRecordsPerEntity = 1,
                TimestampFields = Default().TimestampFields,
                ConcatenationSeparator = "; "
            };
        }
    }

    public enum MasterRecordStrategy
    {
        FirstRecord,
        MostCompleteRecord,
        MostRecentRecord,
        FieldLevelMerge,
        CustomStrategy
    }

    public enum FieldMergeStrategy
    {
        FirstNonNull,
        LongestValue,
        MostFrequent,
        Concatenate,
        Custom
    }

    /// <summary>
    /// Represents a resolved entity
    /// </summary>
    public class EnhacedResolvedEntity
    {
        public string EntityId { get; set; }
        public string GroupId { get; set; }
        public IDictionary<string, object> MasterRecord { get; set; }
        public List<IDictionary<string, object>> MemberRecords { get; set; }
        public int RecordCount { get; set; }
        public DateTime CreatedAt { get; set; }
        public string ResolutionMethod { get; set; }
    }
}
