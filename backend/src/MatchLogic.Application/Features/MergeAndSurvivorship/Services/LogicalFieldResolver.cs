using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Resolves logical field names to physical field names across data sources
/// </summary>
public class LogicalFieldResolver : ILogicalFieldResolver
{
    private readonly ILogger<LogicalFieldResolver> _logger;
    private readonly IDataSourceIndexMapper _dataSourceMapper;

    public LogicalFieldResolver(
        ILogger<LogicalFieldResolver> logger,
        IDataSourceIndexMapper dataSourceMapper)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dataSourceMapper = dataSourceMapper ?? throw new ArgumentNullException(nameof(dataSourceMapper));
    }

    public Task<List<LogicalFieldMapping>> ResolveLogicalFieldsAsync(
        List<MappedFieldRow> mappedFields,
        List<DataSource> dataSources)
    {
        var logicalMappings = new List<LogicalFieldMapping>();

        if (mappedFields == null || !mappedFields.Any())
        {
            _logger.LogWarning("No mapped fields provided for logical field resolution");
            return Task.FromResult(logicalMappings);
        }

        // Create a dictionary of data source ID to index for ordering
        var dsIndexMap = new Dictionary<Guid, int>();
        for (int i = 0; i < dataSources.Count; i++)
        {
            dsIndexMap[dataSources[i].Id] = i;
        }

        int fieldIndex = 0;

        // Process each mapped field row
        foreach (var mappedRow in mappedFields.Where(mf => mf.Include))
        {
            var physicalFields = new Dictionary<Guid, string>();
            var allFields = mappedRow.GetAllFields().ToList();

            if (!allFields.Any())
                continue;

            // Collect physical field names from each data source
            foreach (var field in allFields)
            {
                if (!string.IsNullOrWhiteSpace(field.FieldName))
                {
                    physicalFields[field.DataSourceId] = field.FieldName;
                }
            }

            if (!physicalFields.Any())
                continue;

            // Determine logical name from the smallest data source index
            var smallestDsId = physicalFields.Keys
                .OrderBy(dsId => dsIndexMap.GetValueOrDefault(dsId, int.MaxValue))
                .First();

            var logicalName = physicalFields[smallestDsId];

            var mapping = new LogicalFieldMapping
            {
                LogicalFieldName = logicalName,
                PhysicalFields = physicalFields,
                FieldIndex = fieldIndex++,
                IsMapped = physicalFields.Count > 1
            };

            logicalMappings.Add(mapping);

            _logger.LogDebug(
                "Resolved logical field '{LogicalName}' to {Count} physical fields",
                logicalName, physicalFields.Count);
        }

        // Add unmapped columns from all data sources
        // These are columns that exist in schema but not in mapped field rows
        foreach (var dataSource in dataSources)
        {
            // This would require access to the data source schema
            // For now, we'll skip this as it requires additional data
            // In a full implementation, you'd query the data source metadata
        }

        _logger.LogInformation(
            "Resolved {Count} logical field mappings from {MappedCount} mapped rows",
            logicalMappings.Count, mappedFields.Count);

        return Task.FromResult(logicalMappings);
    }

    public string GetPhysicalFieldName(
        string logicalFieldName,
        Guid dataSourceId,
        List<LogicalFieldMapping> mappings)
    {
        if (string.IsNullOrWhiteSpace(logicalFieldName))
            return null;

        var mapping = mappings?.FirstOrDefault(m =>
            m.LogicalFieldName.Equals(logicalFieldName, StringComparison.OrdinalIgnoreCase));

        if (mapping == null)
        {
            // If no mapping found, assume physical name = logical name
            return logicalFieldName;
        }

        return mapping.GetPhysicalFieldName(dataSourceId);
    }

    public object GetFieldValue(
        IDictionary<string, object> recordData,
        string logicalFieldName,
        Guid dataSourceId,
        List<LogicalFieldMapping> mappings)
    {
        if (recordData == null || string.IsNullOrWhiteSpace(logicalFieldName))
            return null;

        // Get the physical field name for this data source
        var physicalFieldName = GetPhysicalFieldName(logicalFieldName, dataSourceId, mappings);

        if (string.IsNullOrWhiteSpace(physicalFieldName))
            return null;

        // Try to get the value from record data
        if (recordData.TryGetValue(physicalFieldName, out var value))
            return value;

        // Also try case-insensitive lookup
        var entry = recordData.FirstOrDefault(kvp =>
            kvp.Key.Equals(physicalFieldName, StringComparison.OrdinalIgnoreCase));

        return entry.Value;
    }
}
