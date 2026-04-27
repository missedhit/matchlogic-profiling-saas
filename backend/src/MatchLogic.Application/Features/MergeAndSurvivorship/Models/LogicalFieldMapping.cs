using System;
using System.Collections.Generic;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Maps a logical field name to physical field names across data sources
/// Example: "CompanyName" (logical) -> { DS1: "CompanyName", DS2: "BusinessName" }
/// </summary>
public class LogicalFieldMapping
{
    /// <summary>
    /// Logical field name used in rules (from smallest DataSourceIndex)
    /// </summary>
    public string LogicalFieldName { get; set; }

    /// <summary>
    /// Maps DataSourceId to physical field name in that source
    /// </summary>
    public Dictionary<Guid, string> PhysicalFields { get; set; }

    /// <summary>
    /// Index of this field for ordering in output
    /// </summary>
    public int FieldIndex { get; set; }

    /// <summary>
    /// Indicates if this field is mapped across multiple sources
    /// </summary>
    public bool IsMapped { get; set; }

    public LogicalFieldMapping()
    {
        PhysicalFields = new Dictionary<Guid, string>();
    }

    public LogicalFieldMapping(string logicalFieldName) : this()
    {
        LogicalFieldName = logicalFieldName;
    }

    /// <summary>
    /// Gets the physical field name for a specific data source
    /// </summary>
    public string GetPhysicalFieldName(Guid dataSourceId)
    {
        if (PhysicalFields.TryGetValue(dataSourceId, out var physicalName))
            return physicalName;
        
        return null;
    }

    /// <summary>
    /// Checks if this logical field exists in a specific data source
    /// </summary>
    public bool ExistsInDataSource(Guid dataSourceId)
    {
        return PhysicalFields.ContainsKey(dataSourceId);
    }

    /// <summary>
    /// Gets all data sources that have this field
    /// </summary>
    public IEnumerable<Guid> GetDataSourceIds()
    {
        return PhysicalFields.Keys;
    }

    public override string ToString()
    {
        return $"{LogicalFieldName} (mapped to {PhysicalFields.Count} sources)";
    }
}
