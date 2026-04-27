using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Selects the longest value from eligible records
/// Uses ILogicalFieldResolver to get physical field name per record based on data source
/// </summary>
public class LongestValueExecutor : IOverwriteRuleExecutor
{
    private readonly ILogger<LongestValueExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public LongestValueExecutor(
        ILogger<LongestValueExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _fieldResolver = fieldResolver ?? throw new ArgumentNullException(nameof(fieldResolver));
    }

    public object ExecuteRule(
        List<IDictionary<string, object>> records,
        string logicalFieldName,
        FieldOverwriteRule rule,
        List<LogicalFieldMapping> fieldMappings)
    {
        if (records == null || !records.Any())
            return null;

        string longestValue = null;
        int maxLength = -1;

        foreach (var record in records)
        {
            // Extract data source ID from record metadata
            var dataSourceId = ExtractDataSourceId(record);

            // Use field resolver to get the value using logical field name
            // This resolves the correct physical field name for this record's data source
            var value = _fieldResolver.GetFieldValue(
                record,
                logicalFieldName,
                dataSourceId,
                fieldMappings);

            if (value != null && value != DBNull.Value)
            {
                var stringValue = value.ToString();
                if (stringValue.Length > maxLength)
                {
                    maxLength = stringValue.Length;
                    longestValue = stringValue;
                }
            }
        }

        _logger.LogDebug(
            "LongestValue: Selected value with length {Length} from {Count} records",
            maxLength, records.Count);

        return longestValue;
    }

    public bool CanHandle(OverwriteOperation operation)
    {
        return operation == OverwriteOperation.Longest;
    }

    private Guid ExtractDataSourceId(IDictionary<string, object> record)
    {
        // Try to extract from metadata
        if (record.TryGetValue("_metadata", out var metadataObj) &&
            metadataObj is IDictionary<string, object> metadata)
        {
            if (metadata.TryGetValue("DataSourceId", out var dsId))
            {
                if (dsId is Guid guid)
                    return guid;
                if (Guid.TryParse(dsId.ToString(), out var parsedGuid))
                    return parsedGuid;
            }
        }

        // Fallback: try direct field
        if (record.TryGetValue("DataSourceId", out var directDsId))
        {
            if (directDsId is Guid guid)
                return guid;
            if (Guid.TryParse(directDsId.ToString(), out var parsedGuid))
                return parsedGuid;
        }

        return Guid.Empty;
    }
}
