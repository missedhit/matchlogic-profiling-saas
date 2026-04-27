using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class MergeAllValuesExecutor : IOverwriteRuleExecutor
{
    private readonly ILogger<MergeAllValuesExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public MergeAllValuesExecutor(
        ILogger<MergeAllValuesExecutor> logger,
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

        var values = new List<string>();

        foreach (var record in records)
        {
            var dataSourceId = ExtractDataSourceId(record);
            var value = _fieldResolver.GetFieldValue(record, logicalFieldName, dataSourceId, fieldMappings);

            if (value != null && value != DBNull.Value)
            {
                var stringValue = value.ToString();
                if (!string.IsNullOrWhiteSpace(stringValue) && !values.Contains(stringValue))
                {
                    values.Add(stringValue);
                }
            }
        }

        if (!values.Any())
            return null;

        // Use separator from rule if provided, otherwise default to comma-space
        var separator = rule.Configuration != null && rule.Configuration.TryGetValue("Separator", out var sep) && !string.IsNullOrEmpty(sep.ToString())
            ? sep
            : ", ";

        var mergedValue = string.Join(separator.ToString(), values);

        _logger.LogDebug("MergeAllValues: Merged {Count} distinct values from {Total} records",
            values.Count, records.Count);

        return mergedValue;
    }

    public bool CanHandle(OverwriteOperation operation)
    {
        return operation == OverwriteOperation.MergeAllValues;
    }

    private Guid ExtractDataSourceId(IDictionary<string, object> record)
    {
        if (record.TryGetValue("_metadata", out var metadataObj) &&
            metadataObj is IDictionary<string, object> metadata)
        {
            if (metadata.TryGetValue("DataSourceId", out var dsId))
            {
                if (dsId is Guid guid) return guid;
                if (Guid.TryParse(dsId.ToString(), out var parsedGuid)) return parsedGuid;
            }
        }

        if (record.TryGetValue("DataSourceId", out var directDsId))
        {
            if (directDsId is Guid guid) return guid;
            if (Guid.TryParse(directDsId.ToString(), out var parsedGuid)) return parsedGuid;
        }

        return Guid.Empty;
    }
}
