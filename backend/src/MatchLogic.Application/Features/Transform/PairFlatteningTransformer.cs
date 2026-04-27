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
/// Flattens complex nested Pair documents into individual record rows.
/// Each pair produces two rows (Record1 and Record2) with scoring and comparison information.
/// Multi-row transformer: 1 pair input → 2 record outputs
/// 
/// Configuration Example:
///   Settings: { 
///     "fieldMappings": List<MappedFieldRow>,
///     "dataSourceDict": Dictionary<Guid, string>
///   }
/// 
/// Transforms Pair document structure to match frontend expectations.
/// Each record in the pair becomes a separate row with:
/// - Completely flattened record data (no nested objects)
/// - Pair comparison scores by definition
/// - Data source information
/// - Pair relationship indicators
/// </summary>
[HandlesDataTransformer("pairs")]
internal class PairFlatteningTransformer : BaseDataTransformer
{
    private readonly List<MappedFieldRow>? _fieldMappings;
    private readonly Dictionary<Guid, string>? _dataSourceDict;

    public override string Name => "pair_flatten";

    public PairFlatteningTransformer(TransformerConfiguration configuration, ILogger? logger = null)
        : base(configuration, logger)
    {
        _fieldMappings = ExtractFieldMappings(configuration.Settings);
        _dataSourceDict = ExtractDataSourceDict(configuration.Settings);

        Logger?.LogInformation(
            "PairFlatteningTransformer initialized with {FieldMappingCount} field mappings and {DataSourceCount} data sources",
            _fieldMappings?.Count ?? 0,
            _dataSourceDict?.Count ?? 0);
    }

    /// <summary>
    /// Multi-row transformation: 1 pair → 2 record rows (Record1 and Record2)
    /// </summary>
    protected override async Task<IEnumerable<IDictionary<string, object>>> TransformRowInternalAsync(
        IDictionary<string, object> row,
        CancellationToken cancellationToken)
    {
        return await Task.FromResult(TransformPairToRows(row, _fieldMappings, _dataSourceDict ?? new Dictionary<Guid, string>()));
    }

    public IEnumerable<IDictionary<string, object>> TransformPairToRows(
        IDictionary<string, object> pair,
        List<MappedFieldRow>? fieldMappings,
        Dictionary<Guid, string> dataSourceDict)
    {
        if (pair == null)
            return Enumerable.Empty<IDictionary<string, object>>();

        var fieldMappingDict = BuildFieldMappingDictionary(fieldMappings);
        var transformedRows = new List<IDictionary<string, object>>();

        // Transform Record1
        if (pair.TryGetValue("Record1", out var record1Obj) && record1Obj is IDictionary<string, object> record1)
        {
            var row1 = TransformPairRecordToRow(
                record1,
                pair,
                "Record1",
                0,
                fieldMappingDict,
                dataSourceDict);
            
            transformedRows.Add(row1);
        }

        // Transform Record2
        if (pair.TryGetValue("Record2", out var record2Obj) && record2Obj is IDictionary<string, object> record2)
        {
            var row2 = TransformPairRecordToRow(
                record2,
                pair,
                "Record2",
                1,
                fieldMappingDict,
                dataSourceDict);
            
            transformedRows.Add(row2);
        }

        return transformedRows;
    }

    

    private IDictionary<string, object> TransformPairRecordToRow(
        IDictionary<string, object> record,
        IDictionary<string, object> pair,
        string recordType,
        int recordIndex,
        Dictionary<string, Dictionary<string, string>> fieldMappingDict,
        Dictionary<Guid, string> dataSourceDict)
    {
        var transformedRow = new Dictionary<string, object>();

        // Extract basic pair information - FLATTENED
        transformedRow["pairId"] = GetLongValue(pair, "PairId");
        transformedRow["recordType"] = recordType;
        transformedRow["recordIndex"] = recordIndex;
        transformedRow["isFirstInPair"] = recordIndex == 0;
        transformedRow["isSecondInPair"] = recordIndex == 1;

        // Extract data source information - FLATTENED
        var dataSourceIdKey = recordType == "Record1" ? "DataSource1Id" : "DataSource2Id";
        var rowNumberKey = recordType == "Record1" ? "Row1Number" : "Row2Number";
        
        var dataSourceId = GetStringValue(pair, dataSourceIdKey) ?? string.Empty;
        transformedRow["dataSourceId"] = dataSourceId;
        transformedRow["dataSourceName"] = GetDataSourceName(dataSourceId, dataSourceDict);
        transformedRow["rowNumber"] = GetNumericValue(pair, rowNumberKey);

        // Extract match information - FLATTENED
        var matchDefinitionIndices = GetMatchDefinitionIndices(pair);
        transformedRow["mds"] = string.Join(" ", matchDefinitionIndices);
        transformedRow["maxScore"] = GetDoubleValue(pair, "MaxScore");

        // Extract metadata - FLATTENED
        if (record.TryGetValue("_metadata", out var metadataObj) && metadataObj is IDictionary<string, object> metadata)
        {
            transformedRow["recordRowNumber"] = GetNumericValue(metadata, "RowNumber");
            transformedRow["recordHash"] = GetStringValue(metadata, "Hash");
            transformedRow["recordSourceFile"] = GetStringValue(metadata, "SourceFile");
        }

        // Map record data using field mappings - COMPLETELY FLATTENED
        var mappedRecordData = MapRecordData(record, dataSourceId, fieldMappingDict);
        foreach (var kvp in mappedRecordData)
        {
            transformedRow[kvp.Key] = kvp.Value;
        }

        // Add score details - COMPLETELY FLATTENED
        var scoreDetails = ExtractScoreDetails(pair);
        foreach (var kvp in scoreDetails)
        {
            transformedRow[kvp.Key] = kvp.Value;
        }

        return transformedRow;
    }

    private List<int> GetMatchDefinitionIndices(IDictionary<string, object> pair)
    {
        var indices = new List<int>();
        
        if (pair.TryGetValue("MatchDefinitionIndices", out var indicesObj) &&
            indicesObj is IEnumerable<object> indicesList)
        {
            indices.AddRange(indicesList.Select(i => Convert.ToInt32(GetNumericValue(i))));
        }

        return indices;
    }

    private string GetDataSourceName(string dataSourceId, Dictionary<Guid, string> dataSourceDict)
    {
        if (string.IsNullOrEmpty(dataSourceId) || !Guid.TryParse(dataSourceId, out var guid))
            return string.Empty;

        return dataSourceDict.TryGetValue(guid, out var name) ? name : string.Empty;
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

    private Dictionary<string, object> ExtractScoreDetails(IDictionary<string, object> pair)
    {
        var scoreDetails = new Dictionary<string, object>();

        if (!pair.TryGetValue("ScoresByDefinition", out var scoresByDefObj) || 
            scoresByDefObj is not IDictionary<string, object> scoresByDefinition)
        {
            return scoreDetails;
        }

        // FLATTEN definition scores - no nested objects
        double bestScore = 0;
        int bestDefinitionIndex = 1;

        foreach (var (defIndex, scoreDetailObj) in scoresByDefinition)
        {
            if (scoreDetailObj is IDictionary<string, object> scoreDetail)
            {
                var finalScore = GetDoubleValue(scoreDetail, "FinalScore");
                var weightedScore = GetDoubleValue(scoreDetail, "WeightedScore");
                
                // Flatten: use "definitionScore_{index}" instead of nested dictionary
                scoreDetails[$"definitionScore_{defIndex}"] = finalScore;
                scoreDetails[$"definitionWeightedScore_{defIndex}"] = weightedScore;

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
                        scoreDetails[$"criteria_{cleanFieldName}_{defIndex}"] = GetNumericValue(score);
                    }
                }

                // Extract field weights and flatten them
                if (scoreDetail.TryGetValue("FieldWeights", out var fieldWeightsObj) &&
                    fieldWeightsObj is IDictionary<string, object> fieldWeights)
                {
                    foreach (var (fieldName, weight) in fieldWeights)
                    {
                        var cleanFieldName = fieldName.Split('_')[0];
                        scoreDetails[$"weight_{cleanFieldName}_{defIndex}"] = GetNumericValue(weight);
                    }
                }
            }
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
            if (dict.ContainsKey("$guid"))
                return dict["$guid"]?.ToString();
            
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