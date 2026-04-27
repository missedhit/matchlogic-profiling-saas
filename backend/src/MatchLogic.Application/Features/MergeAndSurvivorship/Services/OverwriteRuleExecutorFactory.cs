using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Factory for creating field overwrite rule executors
/// </summary>
public class OverwriteRuleExecutorFactory
{
    private readonly Dictionary<OverwriteOperation, IOverwriteRuleExecutor> _executors;
    private readonly ILogger<OverwriteRuleExecutorFactory> _logger;

    public OverwriteRuleExecutorFactory(ILogger<OverwriteRuleExecutorFactory> logger,
        LongestValueExecutor longestValueExecutor,
        ShortestValueExecutor shortestValueExecutor,
        MaxValueExecutor maxValueExecutor,
        MinValueExecutor minValueExecutor,
        MostPopularValueExecutor mostPopularValueExecutor,
        FromMasterExecutor fromMasterExecutor,
        FromBestRecordExecutor fromBestRecordExecutor,
        MergeAllValuesExecutor mergeAllValuesExecutor)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Register all 8 executors
        _executors = new Dictionary<OverwriteOperation, IOverwriteRuleExecutor>
        {
            [OverwriteOperation.Longest] = longestValueExecutor,
            [OverwriteOperation.Shortest] = shortestValueExecutor,
            [OverwriteOperation.Max] = maxValueExecutor,
            [OverwriteOperation.Min] = minValueExecutor,
            [OverwriteOperation.MostPopular] = mostPopularValueExecutor,
            [OverwriteOperation.FromMaster] = fromMasterExecutor,
            [OverwriteOperation.FromBestRecord] = fromBestRecordExecutor,
            [OverwriteOperation.MergeAllValues] = mergeAllValuesExecutor
        };
    }

    public IOverwriteRuleExecutor GetExecutor(OverwriteOperation operation)
    {
        if (_executors.TryGetValue(operation, out var executor))
        {
            return executor;
        }

        _logger.LogWarning("No executor registered for operation: {Operation}", operation);
        return null;
    }
}
