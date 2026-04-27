using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Domain.MergeAndSurvivorship;

/// <summary>
/// Represents a single rule for field overwriting
/// Matches the UI screenshot structure with data source checkboxes per field
/// </summary>
public class FieldOverwriteRule : IEntity
{       
    public Guid RuleSetId { get; set; }
    
    /// <summary>
    /// Order of execution (rules are executed sequentially by Order)
    /// </summary>
    public int Order { get; set; }
    
    /// <summary>
    /// Logical field name (e.g., "CompanyName", "Email")
    /// </summary>
    public string LogicalFieldName { get; set; }
    
    /// <summary>
    /// Operation to apply (Longest, Shortest, Max, Min, etc.)
    /// </summary>
    public OverwriteOperation Operation { get; set; }
    
    /// <summary>
    /// Data source filters - corresponds to checkboxes in UI
    /// "Take data from: Sample_data_1" ☑️ "Take data from: Sample_data_2" ☑️
    /// Only records from these data sources will be considered for this field
    /// If null or empty, all data sources are considered
    /// </summary>
    public List<Guid> DataSourceFilters { get; set; }
    
    /// <summary>
    /// Condition that must be true to overwrite
    /// "Overwrite If" column in UI
    /// </summary>
    public OverwriteCondition OverwriteIf { get; set; }
    
    /// <summary>
    /// Condition that prevents overwriting
    /// "Do Not Overwrite If" column in UI
    /// </summary>
    public OverwriteCondition DoNotOverwriteIf { get; set; }
    
    /// <summary>
    /// Whether this rule is active
    /// "Activated" checkbox in UI
    /// </summary>
    public bool IsActive { get; set; }
    
    /// <summary>
    /// Additional configuration for specific operations
    /// Example: MergeAllValues might have {"Separator": ", "}
    /// </summary>
    public Dictionary<string, object> Configuration { get; set; }

    public FieldOverwriteRule()
    {
        IsActive = true;
        DataSourceFilters = new List<Guid>();
        Configuration = new Dictionary<string, object>();
        OverwriteIf = OverwriteCondition.NoCondition;
        DoNotOverwriteIf = OverwriteCondition.NoCondition;
    }

    public FieldOverwriteRule(
        Guid ruleSetId,
        int order,
        string logicalFieldName,
        OverwriteOperation operation) : this()
    {
        RuleSetId = ruleSetId;
        Order = order;
        LogicalFieldName = logicalFieldName;
        Operation = operation;
    }

    /// <summary>
    /// Validates the rule configuration
    /// </summary>
    public bool IsValid(out List<string> errors)
    {
        errors = new List<string>();

        if (RuleSetId == Guid.Empty)
            errors.Add("RuleSetId is required");

        if (Order < 0)
            errors.Add("Order must be non-negative");

        if (string.IsNullOrWhiteSpace(LogicalFieldName))
            errors.Add("LogicalFieldName is required");

        // Validate operation-specific requirements
        if (Operation == OverwriteOperation.MergeAllValues)
        {
            // MergeAllValues can have optional separator configuration
            // No validation error if not present, will use default
        }

        return errors.Count == 0;
    }

    /// <summary>
    /// Gets a configuration value
    /// </summary>
    public T GetConfigValue<T>(string key, T defaultValue = default)
    {
        if (Configuration != null && Configuration.TryGetValue(key, out var value))
        {
            try
            {
                if (value is T typedValue)
                    return typedValue;
                return (T)Convert.ChangeType(value, typeof(T));
            }
            catch
            {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    /// <summary>
    /// Sets a configuration value
    /// </summary>
    public void SetConfigValue(string key, object value)
    {
        if (Configuration == null)
            Configuration = new Dictionary<string, object>();

        Configuration[key] = value;
    }

    /// <summary>
    /// Checks if a data source should be included based on filters
    /// </summary>
    public bool ShouldIncludeDataSource(Guid dataSourceId)
    {
        // If no filters specified, include all data sources
        if (DataSourceFilters == null || !DataSourceFilters.Any())
            return true;

        // Otherwise, only include if in the filter list
        return DataSourceFilters.Contains(dataSourceId);
    }
}
