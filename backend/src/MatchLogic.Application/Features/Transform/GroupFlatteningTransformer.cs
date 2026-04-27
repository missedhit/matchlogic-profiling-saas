using MatchLogic.Application.Features.Transform;
using MatchLogic.Application.Interfaces.Transform;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Transform;

/// <summary>
/// Flattens complex nested Group documents into individual record rows.
/// Each group record becomes a separate row with group metadata and score information.
/// Multi-row transformer: 1 group input → N record outputs
/// 
/// Configuration Example:
///   Settings: { 
///     "fieldMappings": List<MappedFieldRow>,
///     "dataSourceDict": Dictionary<Guid, string>
///   }
/// 
/// Transforms Group document structure to match frontend expectations.
/// Each record in the group becomes a separate row with:
/// - Completely flattened record data (no nested objects)
/// - Group metadata (avgMatchScore, size, etc.)
/// - Score information by definition
/// - Visual grouping information
/// </summary>
[HandlesDataTransformer("groups")]
internal class GroupFlatteningTransformer : BaseDataTransformer
{
    private readonly List<MappedFieldRow>? _fieldMappings;
    private readonly Dictionary<Guid, string>? _dataSourceDict;

    public override string Name => "group_flatten";

    public GroupFlatteningTransformer(TransformerConfiguration configuration, ILogger? logger = null)
        : base(configuration, logger)
    {
        _fieldMappings = ExtractFieldMappings(configuration.Settings);
        _dataSourceDict = ExtractDataSourceDict(configuration.Settings);

        Logger?.LogInformation(
            "GroupFlatteningTransformer initialized with {FieldMappingCount} field mappings and {DataSourceCount} data sources",
            _fieldMappings?.Count ?? 0,
            _dataSourceDict?.Count ?? 0);
    }

    /// <summary>
    /// Multi-row transformation: 1 group → N record rows
    /// </summary>
    protected override async Task<IEnumerable<IDictionary<string, object>>> TransformRowInternalAsync(
        IDictionary<string, object> row,
        CancellationToken cancellationToken= default)
    {
        return await Task.FromResult(TransformGroupToRows(row, _fieldMappings, _dataSourceDict ?? new Dictionary<Guid, string>()));
    }

    public IEnumerable<IDictionary<string, object>> TransformGroupToRows(
        IDictionary<string, object> group,
        List<MappedFieldRow>? fieldMappings,
        Dictionary<Guid, string> dataSourceDict)
    {
        if (group == null || !group.TryGetValue("Records", out var recordsObj))
            return Enumerable.Empty<IDictionary<string, object>>();

        var records = ExtractRecordsFromGroup(recordsObj);
        if (!records.Any())
            return Enumerable.Empty<IDictionary<string, object>>();

        var fieldMappingDict = BuildFieldMappingDictionary(fieldMappings);
        var groupMetadata = ExtractGroupMetadata(group);
        var transformedRows = new List<IDictionary<string, object>>();

        for (int recordIndex = 0; recordIndex < records.Count; recordIndex++)
        {
            var record = records[recordIndex];
            var transformedRow = TransformRecordToRow(
                record,
                group,
                recordIndex,
                records.Count,
                fieldMappingDict,
                dataSourceDict,
                groupMetadata);

            transformedRows.Add(transformedRow);
        }

        return transformedRows;
    }

    private List<IDictionary<string, object>> ExtractRecordsFromGroup(object? recordsObj)
    {
        var records = new List<IDictionary<string, object>>();

        if (recordsObj is IEnumerable<object> recordsList)
        {
            foreach (var rec in recordsList)
            {
                if (rec is IDictionary<string, object> recordDict)
                {
                    records.Add(recordDict);
                }
            }
        }

        return records;
    }

    private IDictionary<string, object> ExtractGroupMetadata(IDictionary<string, object> group)
    {
        var metadata = new Dictionary<string, object>();

        if (group.TryGetValue("Metadata", out var metadataObj) && metadataObj is IDictionary<string, object> metadataDict)
        {
            metadata["avgMatchScore"] = GetNumericValue(metadataDict, "avg_match_score");
            metadata["minMatchScore"] = GetNumericValue(metadataDict, "min_match_score");
            metadata["maxMatchScore"] = GetNumericValue(metadataDict, "max_match_score");
            metadata["isClique"] = GetBooleanValue(metadataDict, "is_clique");
            metadata["size"] = GetNumericValue(metadataDict, "size");
        }

        return metadata;
    }

    private IDictionary<string, object> TransformRecordToRow(
        IDictionary<string, object> record,
        IDictionary<string, object> group,
        int recordIndex,
        int totalRecordsInGroup,
        Dictionary<string, Dictionary<string, string>> fieldMappingDict,
        Dictionary<Guid, string> dataSourceDict,
        IDictionary<string, object> groupMetadata)
    {
        var transformedRow = new Dictionary<string, object>();

        // Extract basic group information - FLATTENED
        transformedRow["groupId"] = GetNumericValue(group, "GroupId");
        transformedRow["recordIndex"] = recordIndex;
        transformedRow["totalRecordsInGroup"] = totalRecordsInGroup;
        transformedRow["isFirstInGroup"] = recordIndex == 0;
        transformedRow["isLastInGroup"] = recordIndex == totalRecordsInGroup - 1;
        transformedRow["groupRowSpan"] = recordIndex == 0 ? totalRecordsInGroup : (object?)null;

        // Extract match details and scores - FLATTENED
        var matchDetails = ExtractMatchDetails(record);
        transformedRow["pairId"] = matchDetails.PairId;
        transformedRow["mds"] = string.Join(" ", matchDetails.MatchDefinitionIndices);
        transformedRow["maxScore"] = matchDetails.MaxScore;
        transformedRow["avgScore"] = GetNumericValue(record, "_group_avg_score");

        // Extract data source information - FLATTENED
        var recordDataSourceId = ExtractDataSourceId(record, matchDetails.Neighbor, fieldMappingDict);
        transformedRow["dataSourceName"] = GetDataSourceName(recordDataSourceId, dataSourceDict);
        transformedRow["matchingRecords"] = ExtractMatchingRecords(matchDetails.Neighbor);

        // Extract metadata - FLATTENED
        if (record.TryGetValue("_metadata", out var metadataObj) && metadataObj is IDictionary<string, object> metadata)
        {
            transformedRow["rowNumber"] = GetNumericValue(metadata, "RowNumber");
        }

        // Map record data using field mappings - COMPLETELY FLATTENED
        var mappedRecordData = MapRecordData(record, recordDataSourceId, fieldMappingDict);
        foreach (var kvp in mappedRecordData)
        {
            transformedRow[kvp.Key] = kvp.Value;
        }

        // Add group metadata with flattened keys
        foreach (var kvp in groupMetadata)
        {
            transformedRow[$"group{char.ToUpper(kvp.Key[0])}{kvp.Key.Substring(1)}"] = kvp.Value;
        }

        // Add score details - COMPLETELY FLATTENED
        var scoreDetails = ExtractScoreDetails(matchDetails);
        foreach (var kvp in scoreDetails)
        {
            transformedRow[kvp.Key] = kvp.Value;
        }

        // Add control fields - FLATTENED
        transformedRow["master"] = GetBooleanValue(record, "Master");
        transformedRow["selected"] = GetBooleanValue(record, "Selected");
        transformedRow["notDuplicate"] = GetBooleanValue(record, "NotDuplicate");

        return transformedRow;
    }

    private (long? PairId, double MaxScore, string Neighbor, List<int> MatchDefinitionIndices, IDictionary<string, object> ScoresByDefinition) ExtractMatchDetails(IDictionary<string, object> record)
    {
        long? pairId = null;
        double maxScore = 0;
        string neighbor = string.Empty;
        var matchDefinitionIndices = new List<int>();
        var scoresByDefinition = new Dictionary<string, object>();

        if (record.TryGetValue("_group_match_details", out var detailsObj) &&
            detailsObj is IEnumerable<object> details)
        {
            var firstDetail = details.FirstOrDefault() as IDictionary<string, object>;
            if (firstDetail != null)
            {
                pairId = GetLongValue(firstDetail, "PairId");
                maxScore = GetDoubleValue(firstDetail, "MaxScore");
                neighbor = GetStringValue(firstDetail, "neighbor") ?? string.Empty;

                if (firstDetail.TryGetValue("MatchDefinitionIndices", out var indicesObj) &&
                    indicesObj is IEnumerable<object> indices)
                {
                    matchDefinitionIndices.AddRange(indices.Select(i => Convert.ToInt32(GetNumericValue(i))));
                }

                if (firstDetail.TryGetValue("ScoresByDefinition", out var scoresObj) &&
                    scoresObj is IDictionary<string, object> scores)
                {
                    scoresByDefinition = (Dictionary<string, object>)scores;
                }
            }
        }

        return (pairId, maxScore, neighbor, matchDefinitionIndices, scoresByDefinition);
    }

    private string ExtractDataSourceId(IDictionary<string, object> record, string neighbor, Dictionary<string, Dictionary<string, string>> fieldMappingDict)
    {
        // Try multiple ways to get the data source ID
        if (record.TryGetValue("_metadata", out var metadataObj) && metadataObj is IDictionary<string, object> metadata)
        {
            var dataSourceId = GetStringValue(metadata, "DataSourceId") ?? GetStringValue(metadata, "dataSourceId");
            if (!string.IsNullOrEmpty(dataSourceId))
                return dataSourceId;
        }

        // Try from record directly
        var directDataSourceId = GetStringValue(record, "_datasource_id");
        if (!string.IsNullOrEmpty(directDataSourceId))
            return directDataSourceId;

        // Extract from neighbor
        var neighborParts = neighbor?.Split(':');
        if (neighborParts?.Length > 0 && !string.IsNullOrEmpty(neighborParts[0]))
            return neighborParts[0];

        // Fallback: determine from field mappings
        foreach (var columnMapping in fieldMappingDict.Values)
        {
            foreach (var (dsId, fieldName) in columnMapping)
            {
                if (!string.IsNullOrEmpty(fieldName) && record.ContainsKey(fieldName) && record[fieldName] != null)
                    return dsId;
            }
        }

        return string.Empty;
    }

    private string GetDataSourceName(string dataSourceId, Dictionary<Guid, string> dataSourceDict)
    {
        if (string.IsNullOrEmpty(dataSourceId) || !Guid.TryParse(dataSourceId, out var guid))
            return string.Empty;

        return dataSourceDict.TryGetValue(guid, out var name) ? name : string.Empty;
    }

    private string ExtractMatchingRecords(string neighbor)
    {
        var neighborParts = neighbor?.Split(':');
        return neighborParts?.Length > 1 ? neighborParts[1] : string.Empty;
    }

    private Dictionary<string, object> MapRecordData(IDictionary<string, object> record, string recordDataSourceId, Dictionary<string, Dictionary<string, string>> fieldMappingDict)
    {
        var mappedData = new Dictionary<string, object>();

        // Map fields based on field mappings
        foreach (var (columnHeader, dsMapping) in fieldMappingDict)
        {
            // Try to get the field name for this record's data source
            if (dsMapping.TryGetValue(recordDataSourceId, out var fieldNameInRecord) &&
                !string.IsNullOrEmpty(fieldNameInRecord) &&
                record.TryGetValue(fieldNameInRecord, out var value))
            {
                // FLATTEN ANY NESTED VALUES
                mappedData[columnHeader] = FlattenValue(value);
            }
            else
            {
                // Fallback: try all possible field names for this column
                object? foundValue = null;
                foreach (var fieldName in dsMapping.Values)
                {
                    if (!string.IsNullOrEmpty(fieldName) && record.TryGetValue(fieldName, out var fallbackValue) && fallbackValue != null)
                    {
                        foundValue = fallbackValue;
                        break;
                    }
                }

                if (foundValue != null)
                {
                    mappedData[columnHeader] = FlattenValue(foundValue);
                }
                else if (record.TryGetValue(columnHeader, out var directValue))
                {
                    // Last resort: try the column header directly
                    mappedData[columnHeader] = FlattenValue(directValue);
                }
                else
                {
                    mappedData[columnHeader] = null;
                }
            }
        }

        // Include unmapped fields that don't start with underscore and aren't already mapped
        var mappedFieldNames = new HashSet<string>(fieldMappingDict.Values.SelectMany(ds => ds.Values).Where(f => !string.IsNullOrEmpty(f)));

        foreach (var (key, value) in record)
        {
            if (!key.StartsWith("_") && !mappedFieldNames.Contains(key) && !mappedData.ContainsKey(key))
            {
                // FLATTEN ANY NESTED VALUES
                mappedData[key] = FlattenValue(value);
            }
        }

        return mappedData;
    }

    private Dictionary<string, object> ExtractScoreDetails((long? PairId, double MaxScore, string Neighbor, List<int> MatchDefinitionIndices, IDictionary<string, object> ScoresByDefinition) matchDetails)
    {
        var scoreDetails = new Dictionary<string, object>();

        // FLATTEN definition scores - no nested objects
        var definitionScoresFlat = new Dictionary<string, object>();
        var criteriaFieldScoresFlat = new Dictionary<string, object>();

        double bestScore = 0;
        int bestDefinitionIndex = 1;

        foreach (var (defIndex, scoreDetailObj) in matchDetails.ScoresByDefinition)
        {
            if (scoreDetailObj is IDictionary<string, object> scoreDetail)
            {
                var finalScore = GetDoubleValue(scoreDetail, "FinalScore");
                
                // Flatten: use "definitionScore_{index}" instead of nested dictionary
                definitionScoresFlat[$"definitionScore_{defIndex}"] = finalScore;

                if (finalScore > bestScore)
                {
                    bestScore = finalScore;
                    bestDefinitionIndex = int.TryParse(defIndex, out var idx) ? idx : 1;
                }

                // Extract field scores and flatten them
                if (scoreDetail.TryGetValue("FieldScores", out var fieldScoresObj) &&
                    fieldScoresObj is IDictionary<string, object> fieldScores)
                {
                    foreach (var (fieldName, score) in fieldScores)
                    {
                        // Extract clean field name (remove duplicate field names from the key)
                        var cleanFieldName = fieldName.Split('_')[0];
                        
                        // Flatten: use "criteria_{field}_{definition}" instead of nested structure
                        criteriaFieldScoresFlat[$"criteria_{cleanFieldName}_{defIndex}"] = GetNumericValue(score);
                    }
                }
            }
        }

        // Add flattened scores directly to result
        foreach (var kvp in definitionScoresFlat)
        {
            scoreDetails[kvp.Key] = kvp.Value;
        }
        
        foreach (var kvp in criteriaFieldScoresFlat)
        {
            scoreDetails[kvp.Key] = kvp.Value;
        }

        scoreDetails["bestDefinitionIndex"] = bestDefinitionIndex;

        return scoreDetails;
    }

    /// <summary>
    /// Flattens any nested values to ensure no nested objects in output
    /// </summary>
    private object? FlattenValue(object? value)
    {
        if (value == null)
            return null;

        // Handle MongoDB BSON date types
        if (value is IDictionary<string, object> dict)
        {
            if (dict.ContainsKey("$date"))
                return dict["$date"]?.ToString();
            if (dict.ContainsKey("$numberLong"))
                return GetNumericValue(dict, "$numberLong");
            if (dict.ContainsKey("$numberInt"))
                return GetNumericValue(dict, "$numberInt");
            if (dict.ContainsKey("$numberDouble"))
                return GetNumericValue(dict, "$numberDouble");
            if (dict.ContainsKey("$oid"))
                return dict["$oid"]?.ToString();
            
            // For any other dictionary, convert to JSON string to avoid nested objects
            try
            {
                return System.Text.Json.JsonSerializer.Serialize(dict);
            }
            catch
            {
                return dict.ToString();
            }
        }

        // Handle arrays/lists - convert to comma-separated string
        if (value is IEnumerable<object> enumerable && !(value is string))
        {
            try
            {
                return string.Join(", ", enumerable.Select(x => x?.ToString() ?? ""));
            }
            catch
            {
                return value.ToString();
            }
        }

        return value;
    }
}