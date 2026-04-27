using MatchLogic.Domain.Export;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Infrastructure.Export;

/// <summary>
/// Immutable context for export operations containing all necessary metadata.
/// Optimizes performance by pre-computing column mappings and schema information.
/// </summary>
public sealed class ExportContext
{
    public DataExportOptions Options { get; }
    public IReadOnlyList<FieldMappingEx> ExportableFields { get; }
    public IReadOnlyList<string> OrderedColumnNames { get; }
    public IReadOnlyDictionary<string, FieldMappingEx> FieldLookup { get; }
    public IReadOnlyDictionary<string, int> ColumnIndexMap { get; }
    
    public ExportContext(
        DataExportOptions options,
        IReadOnlyList<FieldMappingEx> exportableFields)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        ExportableFields = exportableFields ?? throw new ArgumentNullException(nameof(exportableFields));
        
        // Pre-compute ordered columns based on ColumnMappings order
        OrderedColumnNames = ComputeOrderedColumns(options.ColumnMappings, exportableFields);
        
        // Pre-compute lookup dictionaries for O(1) access
        FieldLookup = exportableFields.ToDictionary(f => f.FieldName, StringComparer.OrdinalIgnoreCase);
        ColumnIndexMap = OrderedColumnNames
            .Select((name, index) => new { name, index })
            .ToDictionary(x => x.name, x => x.index, StringComparer.OrdinalIgnoreCase);
    }

    private static IReadOnlyList<string> ComputeOrderedColumns(
        IReadOnlyList<string> columnMappings,
        IReadOnlyList<FieldMappingEx> exportableFields)
    {
        if (columnMappings?.Count > 0)
        {
            // Use explicit column order from ColumnMappings
            var fieldSet = new HashSet<string>(
                exportableFields.Select(f => f.FieldName), 
                StringComparer.OrdinalIgnoreCase);
            
            return columnMappings
                .Where(fieldSet.Contains)
                .ToList();
        }
        
        // Fall back to field order
        return exportableFields
            .OrderBy(f => f.FieldIndex)
            .Select(f => f.FieldName)
            .ToList();
    }

    /// <summary>
    /// Gets field mapping for a column name with O(1) lookup.
    /// </summary>
    public FieldMappingEx? GetFieldMapping(string columnName)
    {
        return FieldLookup.TryGetValue(columnName, out var field) ? field : null;
    }

    /// <summary>
    /// Gets column index for ordered export with O(1) lookup.
    /// </summary>
    public int GetColumnIndex(string columnName)
    {
        return ColumnIndexMap.TryGetValue(columnName, out var index) ? index : -1;
    }
}