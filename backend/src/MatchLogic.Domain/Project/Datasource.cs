using MatchLogic.Domain.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Project;
public class DataSource : AuditableEntity
{
    public Guid ProjectId { get; set; }
    public string Name { get; set; }
    public DataSourceType Type { get; set; }
    public BaseConnectionInfo ConnectionDetails { get; set; }
    public DataSourceConfiguration Configuration { get; set; }
    public string[] ErrorMessages { get; set; }

    public long RecordCount { get; set; } = 0;
    public long ColumnsCount { get; set; } = 0;

    public Guid? ActiveSnapshotId { get; set; }
    public string? SchemaSignature { get; set; }
    public SchemaPolicy SchemaPolicy { get; set; } = SchemaPolicy.ReorderInsensitive_NameSensitive;

    // For file sources convenience (latest linked upload)
    public Guid? LatestFileImportId { get; set; }

    // Metadata from the remote file at last import (for change detection)
    public StoredFileMetadata? LastImportedFileMetadata { get; set; }

    public void ConfigureColumns(List<string> columns)
    {
        Configuration ??= new DataSourceConfiguration();
        Configuration.ColumnMappings = columns.ToDictionary(
        col => col,
            col => new ColumnMapping
            {
                SourceColumn = col,
                TargetColumn = col,
                Include = true
            });
    }
    public void UpdateColumnMapping(string sourceColumn, bool include, string newName = null)
    {
        Configuration ??= new DataSourceConfiguration();
        if (Configuration.ColumnMappings.ContainsKey(sourceColumn))
        {
            var mapping = Configuration.ColumnMappings[sourceColumn];
            mapping.Include = include;
            mapping.TargetColumn = newName ?? mapping.TargetColumn;
        }
    }
}

public class DataSourceConfiguration
{
    public string Name { get; set; }
    public string TableOrSheet { get; set; }
    public Dictionary<string, ColumnMapping> ColumnMappings { get; set; } = new();
    public string Query { get; set; }
}

public class ColumnMapping
{
    public string SourceColumn { get; set; }
    public string TargetColumn { get; set; }
    public bool Include { get; set; } = true;    
}

public class TableInfo
{
    public string Schema { get; set; }
    public string Name { get; set; }
    public string Type { get; set; }

    public List<ColumnInfo> Columns { get; set; }
}

public class ColumnInfo
{
    public string Name { get; set; }
    public string DataType { get; set; }
    public int? Length { get; set; }
    public bool IsNullable { get; set; }
    public int? Ordinal { get; set; }
}
public class TableSchema
{
    public List<ColumnInfo> Columns { get; set; }
}
public class QueryOptions
{
    public string TableName { get; set; }
    public List<string> SelectedColumns { get; set; }
    public string WhereClause { get; set; }
    public string OrderBy { get; set; }
}
public class ConnectionTestResult
{
    public bool Success { get; }
    public string Message { get; }

    public ConnectionTestResult(bool success, string message)
    {
        Success = success;
        Message = message;
    }
}