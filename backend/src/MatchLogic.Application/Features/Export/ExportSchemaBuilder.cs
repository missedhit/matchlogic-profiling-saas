using MatchLogic.Application.Common;
using MatchLogic.Application.Features.FinalExport;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Export
{
    /// <summary>
    /// Builds export schema with proper data types using ExportTypeResolver.
    /// Merges field types across multiple data sources for lossless export.
    /// Leverages FieldMappingEx.DataType captured during import.
    /// </summary>
    public class ExportSchemaBuilder
    {
        /// <summary>
        /// Build export schema with type information
        /// </summary>
        public static ExportSchema BuildSchema(
            FinalExportSettings settings,
            List<ScoreColumn> scoreColumns,
            MappedFieldsRow? mappedFieldsRow,
            List<DataSource> dataSources,
            string? tableName = null,
            string? schemaName = null)
        {
            var columns = new List<ExportColumnInfo>();

            // 1. System fields with known types
            if (settings.IncludeSystemFields)
            {
                columns.AddRange(GetSystemFieldColumns());
            }

            // 2. Score fields (all double)
            if (settings.IncludeScoreFields && scoreColumns.Count > 0)
            {
                columns.AddRange(scoreColumns.Select(sc => new ExportColumnInfo
                {
                    Name = sc.ColumnName,
                    DataType = sc.ColumnType switch
                    {
                        ScoreColumnType.MaxScore => "float",
                        ScoreColumnType.ThresholdIndicator => "bit",
                        _ => "float"
                    }
                }));
            }

            // 3. Data fields with resolved types from MappedFieldsRow
            var dataColumns = BuildDataFieldColumns(
                mappedFieldsRow,
                dataSources,
                settings.DataSetsToInclude);
            columns.AddRange(dataColumns);

            return ExportSchema.FromColumns(columns, tableName, schemaName);
        }

        /// <summary>
        /// System fields with predefined types
        /// </summary>
        private static IEnumerable<ExportColumnInfo> GetSystemFieldColumns()
        {
            return new List<ExportColumnInfo>
        {
            new() { Name = ExportFieldNames.GroupId, DataType = "int" },            
            new() { Name = ExportFieldNames.DataSourceName, DataType = "nvarchar", Length = 255 },
            new() { Name = ExportFieldNames.Record, DataType = "bigint" },
            new() { Name = ExportFieldNames.Master, DataType = "bit" },
            new() { Name = ExportFieldNames.Selected, DataType = "bit" },
            new() { Name = ExportFieldNames.NotDuplicate, DataType = "bit" },
            new() { Name = ExportFieldNames.MdHits, DataType = "nvarchar", Length = 50 },            
        };
        }

        /// <summary>
        /// Build data field columns with type resolution across data sources
        /// </summary>
        private static List<ExportColumnInfo> BuildDataFieldColumns(
            MappedFieldsRow? mappedFieldsRow,
            List<DataSource> dataSources,
            Dictionary<Guid, bool> dataSetsToInclude)
        {
            var columns = new List<ExportColumnInfo>();

            if (mappedFieldsRow?.MappedFields == null)
                return columns;

            var includedDataSources = dataSources
                .Where(ds => dataSetsToInclude.GetValueOrDefault(ds.Id, true))
                .ToList();

            foreach (var mappedField in mappedFieldsRow.MappedFields.Where(mf => mf.Include))
            {
                // Collect field mappings from all included data sources
                var fieldMappings = includedDataSources
                    .Select(ds => mappedField[ds.Name])
                    .Where(fm => fm != null)
                    .ToList();

                if (fieldMappings.Count == 0)
                    continue;

                // Determine export type using ExportTypeResolver
                var exportField = ResolveExportType(fieldMappings);

                columns.Add(new ExportColumnInfo
                {
                    Name = exportField.FieldName,
                    DataType = exportField.DataType ?? "nvarchar",
                    Length = exportField.Length
                });
            }

            return columns;
        }

        /// <summary>
        /// Resolve export type from multiple field mappings using ExportTypeResolver
        /// </summary>
        private static FieldMappingEx ResolveExportType(List<FieldMappingEx> fieldMappings)
        {
            if (fieldMappings.Count == 0)
                throw new ArgumentException("No field mappings provided");

            if (fieldMappings.Count == 1)
                return fieldMappings[0];

            // Use ExportTypeResolver to merge types progressively
            var result = fieldMappings[0];

            for (int i = 1; i < fieldMappings.Count; i++)
            {
                result = ExportTypeResolver.DetermineLosslessExportField(result, fieldMappings[i]);
            }

            return result;
        }
    }
}
