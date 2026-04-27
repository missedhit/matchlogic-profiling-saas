using MatchLogic.Domain.CleansingAndStandaradization;
using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Cleansing;

/// <summary>
/// Base request model for cleansing rule operations
/// </summary>
public abstract class BaseCleansingRule
{
    /// <summary>
    /// ID of the project containing the data source to clean
    /// </summary>
    public Guid ProjectId { get; set; }

    /// <summary>
    /// ID of the data source to clean
    /// </summary>
    public Guid DataSourceId { get; set; }

    /// <summary>
    /// Collection of column operations to perform
    /// </summary>
    public List<ColumnOperationModel> ColumnOperations { get; set; } = new();

    public bool isPreview { get; set; }
}

/// <summary>
/// Model representing operations to perform on a column
/// </summary>
public class ColumnOperationModel
{
    /// <summary>
    /// Name of the column to operate on
    /// </summary>
    public string ColumnName { get; set; }

    /// <summary>
    /// Collection of operations to apply to this column
    /// </summary>
    public List<OperationModel> Operations { get; set; } = new();

    /// <summary>
    /// Set to true to automatically create a copy of the original field
    /// The copy will be named [ColumnName]_Original
    /// </summary>
    public bool CopyField { get; set; }
}

/// <summary>
/// Model representing a single operation to apply to a column
/// </summary>
public class OperationModel
{
    /// <summary>
    /// Type of operation (Standard or Mapping)
    /// </summary>
    public OperationType Type { get; set; }

    /// <summary>
    /// Type of cleaning rule to apply (for Standard operations)
    /// </summary>
    public CleaningRuleType? CleaningType { get; set; }

    /// <summary>
    /// Type of mapping operation to apply (for Mapping operations)
    /// </summary>
    public MappingOperationType? MappingType { get; set; }

    /// <summary>
    /// Parameters for the operation
    /// </summary>
    public Dictionary<string, string> Parameters { get; set; } = new();

    /// <summary>
    /// Source columns for mapping operations (if different from the main column)
    /// </summary>
    public List<string> SourceColumns { get; set; } = new();

    /// <summary>
    /// Output columns for mapping operations
    /// </summary>
    public List<string> OutputColumns { get; set; } = new();
}

/// <summary>
/// Defines a required parameter for an operation
/// </summary>
public class OperationParameter
{
    /// <summary>
    /// Parameter name (used in request)
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Parameter label (shown in UI)
    /// </summary>
    public string Label { get; set; }

    /// <summary>
    /// Parameter type (string, number, boolean)
    /// </summary>
    public string Type { get; set; }

    /// <summary>
    /// Whether the parameter is required
    /// </summary>
    public bool Required { get; set; }

    /// <summary>
    /// Default value if any
    /// </summary>
    public string DefaultValue { get; set; }

    public string Description { get; set; }
}

/// <summary>
/// Defines mapping operation requirements
/// </summary>
public class MappingRequirements
{
    /// <summary>
    /// Whether source columns are required
    /// </summary>
    public bool RequiresSourceColumns { get; set; }

    /// <summary>
    /// Whether output columns are required
    /// </summary>
    public bool RequiresOutputColumns { get; set; }
}