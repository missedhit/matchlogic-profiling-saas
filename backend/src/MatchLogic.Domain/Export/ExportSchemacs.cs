using System.Collections.Generic;
using System;
using System.Linq;

namespace MatchLogic.Domain.Export;

/// <summary>
/// Schema for export with column type information.
/// Decoupled from import structures for clean separation.
/// Supports both typed columns (from FieldMappingEx) and simple header-only mode.
/// </summary>
public class ExportSchema
{
    public IReadOnlyList<ExportColumnInfo> Columns { get; init; } = Array.Empty<ExportColumnInfo>();
    public IReadOnlyList<string> ColumnNames => Columns.Select(c => c.Name).ToList();
    public IReadOnlyDictionary<string, int> ColumnIndexMap { get; init; } = new Dictionary<string, int>();
    public string? TableName { get; init; }
    public string? SchemaName { get; init; }

    /// <summary>
    /// Create schema from columns with type info
    /// </summary>
    public static ExportSchema FromColumns(
        IEnumerable<ExportColumnInfo> columns,
        string? tableName = null,
        string? schemaName = null)
    {
        var columnList = columns.ToList();
        for (int i = 0; i < columnList.Count; i++)
        {
            columnList[i].Index = i;
        }

        return new ExportSchema
        {
            Columns = columnList,
            ColumnIndexMap = columnList
                .Select((c, i) => (c.Name, i))
                .ToDictionary(x => x.Name, x => x.i, StringComparer.OrdinalIgnoreCase),
            TableName = tableName,
            SchemaName = schemaName
        };
    }

    /// <summary>
    /// Create schema from headers only (defaults to string types)
    /// Used when type information is not available.
    /// </summary>
    public static ExportSchema FromHeaders(
        IEnumerable<string> headers,
        string? tableName = null,
        string? schemaName = null)
    {
        var columns = headers.Select((h, i) => new ExportColumnInfo
        {
            Name = h,
            DataType = "nvarchar",
            Length = null, // MAX
            Index = i
        });

        return FromColumns(columns, tableName, schemaName);
    }

    /// <summary>
    /// Get column index with O(1) lookup
    /// </summary>
    public int GetColumnIndex(string columnName)
    {
        return ColumnIndexMap.TryGetValue(columnName, out var index) ? index : -1;
    }

    /// <summary>
    /// Get column info by name
    /// </summary>
    public ExportColumnInfo? GetColumn(string columnName)
    {
        var index = GetColumnIndex(columnName);
        return index >= 0 ? Columns[index] : null;
    }

    /// <summary>
    /// Check if column exists
    /// </summary>
    public bool HasColumn(string columnName)
    {
        return ColumnIndexMap.ContainsKey(columnName);
    }
}