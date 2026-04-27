using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class FromMasterExecutor : IOverwriteRuleExecutor
{
    private readonly ILogger<FromMasterExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public FromMasterExecutor(
        ILogger<FromMasterExecutor> logger,
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

        // Find the master record
        var masterRecord = records.FirstOrDefault(r =>
            r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
            isMaster is bool boolVal && boolVal);

        if (masterRecord == null)
        {
            _logger.LogWarning("FromMaster: No master record found in group");
            return null;
        }

        var dataSourceId = ExtractDataSourceId(masterRecord);
        var value = _fieldResolver.GetFieldValue(masterRecord, logicalFieldName, dataSourceId, fieldMappings);

        _logger.LogDebug("FromMaster: Selected value from master record");
        return value;
    }

    public bool CanHandle(OverwriteOperation operation)
    {
        return operation == OverwriteOperation.FromMaster;
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
