using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class FromBestRecordExecutor : IOverwriteRuleExecutor
{
    private readonly ILogger<FromBestRecordExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public FromBestRecordExecutor(
        ILogger<FromBestRecordExecutor> logger,
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

        // Strategy: First try master, then selected, then not duplicate, then first record
        IDictionary<string, object> bestRecord = null;

        // 1. Try to find master record
        bestRecord = records.FirstOrDefault(r =>
            r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
            isMaster is bool masterVal && masterVal);

        // 2. If no master, try selected record
        if (bestRecord == null)
        {
            bestRecord = records.FirstOrDefault(r =>
                r.TryGetValue(RecordSystemFieldNames.Selected, out var isSelected) &&
                isSelected is bool selectedVal && selectedVal);
        }

        // 3. If no selected, try not duplicate
        if (bestRecord == null)
        {
            bestRecord = records.FirstOrDefault(r =>
                r.TryGetValue(RecordSystemFieldNames.NotDuplicate, out var notDup) &&
                notDup is bool notDupVal && notDupVal);
        }

        // 4. Fallback to first record
        if (bestRecord == null)
        {
            bestRecord = records.First();
        }

        var dataSourceId = ExtractDataSourceId(bestRecord);
        var value = _fieldResolver.GetFieldValue(bestRecord, logicalFieldName, dataSourceId, fieldMappings);

        _logger.LogDebug("FromBestRecord: Selected value from best record");
        return value;
    }

    public bool CanHandle(OverwriteOperation operation)
    {
        return operation == OverwriteOperation.FromBestRecord;
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
