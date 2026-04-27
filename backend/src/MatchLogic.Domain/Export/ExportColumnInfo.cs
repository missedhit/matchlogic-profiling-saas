using System;

namespace MatchLogic.Domain.Export;

/// <summary>
/// Column information with data type for export.
/// Provides database-specific type mappings for SQL Server, MySQL, and PostgreSQL.
/// Uses FieldMappingEx.DataType captured during import for type fidelity.
/// </summary>
public class ExportColumnInfo
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = "nvarchar";
    public int? Length { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
    public int Index { get; set; }
    public bool IsNullable { get; set; } = true;
}