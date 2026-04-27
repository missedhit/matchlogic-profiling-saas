using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class MostPopularValueExecutor : IOverwriteRuleExecutor
{
    private readonly ILogger<MostPopularValueExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public MostPopularValueExecutor(
        ILogger<MostPopularValueExecutor> logger,
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

        var valueCounts = new Dictionary<string, int>();

        foreach (var record in records)
        {
            var dataSourceId = ExtractDataSourceId(record);
            var value = _fieldResolver.GetFieldValue(record, logicalFieldName, dataSourceId, fieldMappings);

            if (value != null && value != DBNull.Value)
            {
                var stringValue = value.ToString();
                if (!string.IsNullOrWhiteSpace(stringValue))
                {
                    if (valueCounts.ContainsKey(stringValue))
                        valueCounts[stringValue]++;
                    else
                        valueCounts[stringValue] = 1;
                }
            }
        }

        if (!valueCounts.Any())
            return null;

        var mostPopular = valueCounts.OrderByDescending(kvp => kvp.Value).First();

        _logger.LogDebug("MostPopular: Selected '{Value}' (appeared {Count} times) from {Total} records",
            mostPopular.Key, mostPopular.Value, records.Count);

        return mostPopular.Key;
    }

    public bool CanHandle(OverwriteOperation operation)
    {
        return operation == OverwriteOperation.MostPopular;
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
