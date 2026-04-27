using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using System;
using System.Collections.Generic;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Represents a candidate record during master record determination
/// </summary>
public class RecordCandidate
{
    /// <summary>
    /// Unique identifier for this record
    /// </summary>
    public RecordKey RecordKey { get; set; }

    /// <summary>
    /// The actual record data
    /// </summary>
    public IDictionary<string, object> RecordData { get; set; }

    /// <summary>
    /// Data source this record belongs to
    /// </summary>
    public Guid DataSourceId { get; set; }

    /// <summary>
    /// Index of the data source (for ordering/tiebreaking)
    /// </summary>
    public int DataSourceIndex { get; set; }

    /// <summary>
    /// Row number in the original data source
    /// </summary>
    public int RowNumber { get; set; }

    /// <summary>
    /// Was this record previously marked as master?
    /// </summary>
    public bool WasPreviouslyMaster { get; set; }

    /// <summary>
    /// Score or rank (optional, for custom sorting)
    /// </summary>
    public double Score { get; set; }

    public RecordCandidate()
    {
        RecordData = new Dictionary<string, object>();
    }

    public RecordCandidate(
        RecordKey recordKey,
        IDictionary<string, object> recordData,
        Guid dataSourceId,
        int dataSourceIndex)
    {
        RecordKey = recordKey;
        RecordData = recordData ?? new Dictionary<string, object>();
        DataSourceId = dataSourceId;
        DataSourceIndex = dataSourceIndex;
        RowNumber = recordKey.RowNumber;
    }

    /// <summary>
    /// Gets a field value from the record data
    /// </summary>
    public object GetFieldValue(string fieldName)
    {
        if (RecordData != null && RecordData.TryGetValue(fieldName, out var value))
            return value;
        
        return null;
    }

    /// <summary>
    /// Checks if a field has a non-null, non-empty value
    /// </summary>
    public bool HasValue(string fieldName)
    {
        var value = GetFieldValue(fieldName);
        
        if (value == null)
            return false;
        
        if (value is string str)
            return !string.IsNullOrWhiteSpace(str);
        
        return true;
    }

    public override string ToString()
    {
        return $"{RecordKey} (DS Index: {DataSourceIndex})";
    }
}
