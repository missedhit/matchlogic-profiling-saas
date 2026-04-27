using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class MaxValueExecutor : IOverwriteRuleExecutor
{
    private readonly ILogger<MaxValueExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public MaxValueExecutor(
        ILogger<MaxValueExecutor> logger,
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

        double? maxValue = null;

        foreach (var record in records)
        {
            var dataSourceId = ExtractDataSourceId(record);
            var value = _fieldResolver.GetFieldValue(record, logicalFieldName, dataSourceId, fieldMappings);

            if (value != null && value != DBNull.Value)
            {
                if (double.TryParse(value.ToString(), out var numericValue))
                {
                    if (!maxValue.HasValue || numericValue > maxValue.Value)
                    {
                        maxValue = numericValue;
                    }
                }
            }
        }

        _logger.LogDebug("MaxValue: Selected {Value} from {Count} records", maxValue, records.Count);
        return maxValue;
    }

    public bool CanHandle(OverwriteOperation operation)
    {
        return operation == OverwriteOperation.Max;
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
