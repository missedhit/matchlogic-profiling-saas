using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class MinValueExecutor : IOverwriteRuleExecutor
{
    private readonly ILogger<MinValueExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public MinValueExecutor(
        ILogger<MinValueExecutor> logger,
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

        double? minValue = null;

        foreach (var record in records)
        {
            var dataSourceId = ExtractDataSourceId(record);
            var value = _fieldResolver.GetFieldValue(record, logicalFieldName, dataSourceId, fieldMappings);

            if (value != null && value != DBNull.Value)
            {
                if (double.TryParse(value.ToString(), out var numericValue))
                {
                    if (!minValue.HasValue || numericValue < minValue.Value)
                    {
                        minValue = numericValue;
                    }
                }
            }
        }

        _logger.LogDebug("MinValue: Selected {Value} from {Count} records", minValue, records.Count);
        return minValue;
    }

    public bool CanHandle(OverwriteOperation operation)
    {
        return operation == OverwriteOperation.Min;
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
