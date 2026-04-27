using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Factory for creating appropriate rule executors
/// </summary>
public class RuleExecutorFactory
{
    private readonly ILogger<RuleExecutorFactory> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;
    private readonly List<IRuleExecutor> _executors;

    public RuleExecutorFactory(
        ILogger<RuleExecutorFactory> logger,
        ILogicalFieldResolver fieldResolver,
        LongestValueRuleExecutor longestValueRuleExecutor,
        ShortestValueRuleExecutor shortestValueRuleExecutor,
        MaxValueRuleExecutor maxValueRuleExecutor,
        MinValueRuleExecutor minValueRuleExecutor,
        MostPopularRuleExecutor mostPopularRuleExecutor,
        PreferDataSourceRuleExecutor preferDataSourceRuleExecutor,
        FirstNonNullRuleExecutor firstNonNullRuleExecutor,
        MostRecentRuleExecutor mostRecentRuleExecutor)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _fieldResolver = fieldResolver ?? throw new ArgumentNullException(nameof(fieldResolver));

        // Initialize all available executors
        _executors = new List<IRuleExecutor>
        {
            longestValueRuleExecutor,
            shortestValueRuleExecutor,
            maxValueRuleExecutor,
            minValueRuleExecutor,
            mostPopularRuleExecutor,
            preferDataSourceRuleExecutor,
            firstNonNullRuleExecutor,
            mostRecentRuleExecutor
        };
    }

    /// <summary>
    /// Creates the appropriate executor for the given operation
    /// </summary>
    public IRuleExecutor CreateExecutor(MasterRecordOperation operation)
    {
        var executor = _executors.FirstOrDefault(e => e.CanHandle(operation));

        if (executor == null)
        {
            _logger.LogError("No executor found for operation: {Operation}", operation);
            throw new NotSupportedException($"No executor available for operation: {operation}");
        }

        return executor;
    }

    /// <summary>
    /// Gets all available operations
    /// </summary>
    public IEnumerable<MasterRecordOperation> GetAvailableOperations()
    {
        return _executors
            .SelectMany(e => Enum.GetValues<MasterRecordOperation>().Where(op => e.CanHandle(op)))
            .Distinct();
    }

    /// <summary>
    /// Checks if an operation is supported
    /// </summary>
    public bool IsOperationSupported(MasterRecordOperation operation)
    {
        return _executors.Any(e => e.CanHandle(operation));
    }
}