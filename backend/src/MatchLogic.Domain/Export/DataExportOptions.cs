using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.Export;
public class DataExportOptions : IEntity
{
    public Guid ProjectId { get; set; }
    public DateTime CreatedDate { get; set; }
    // Required properties
    public required string TableName { get; set; }
    public bool ExportOnlyColumnsAndRows { get; set; } = false;
    public bool BulkCopy { get; set; } = false;
    public bool ForceDBDataTypeFromInput { get; set; } = false;
    public string? SchemaName { get; set; } = null;
    //public Dictionary<string, ColumnMapping> ColumnMappings { get; set; } = new();
    //public List<ColumnMapping> ColumnMappings { get; set; } = new();
    public List<string> ColumnMappings { get; set; } = new();
    //public ExportStep StepType { get; set; }
    public string CollectionName { get; set; }
    /// <summary>
    /// View Name like Final Export, Match Result Pairs,Groups , etc.
    /// View type key: "finalExport", "pairs", "groups", or custom.
    /// </summary>
    public string ViewType { get; set; }
    public BaseConnectionInfo ConnectionConfig { get; set; }

    /// <summary>
    /// Transformer type key: "none", "flatten", "projection", "aggregation", or custom.
    /// If null/"none", no transformation is applied (identity).
    /// 
    /// Configuration (settings and column projections) belongs to the transformation itself,
    /// not here. Pass TransformerConfiguration to IDataTransformerFactory.GetTransformer().
    /// </summary>
    //public string? TransformerType { get; set; } = "none";

}
