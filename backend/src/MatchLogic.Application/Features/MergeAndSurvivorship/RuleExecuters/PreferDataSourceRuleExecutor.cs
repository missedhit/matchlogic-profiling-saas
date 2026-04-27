using MatchLogic.Application.Features.MergeAndSurvivorship.Extensions;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Prefers records from a specific data source.
/// This executor uses PreferredDataSourceId for its operation logic,
/// but still respects SelectedDataSourceIds for pre-filtering when enabled.
/// </summary>
public class PreferDataSourceRuleExecutor : IRuleExecutor
{
    private readonly ILogger<PreferDataSourceRuleExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public PreferDataSourceRuleExecutor(
        ILogger<PreferDataSourceRuleExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger;
        _fieldResolver = fieldResolver;
    }

    public bool CanHandle(MasterRecordOperation operation)
    {
        return operation == MasterRecordOperation.PreferDataSource;
    }

    public Task<RuleExecutionResult> ExecuteAsync(
        List<RecordCandidate> candidates,
        MasterRecordRule rule,
        LogicalFieldMapping fieldMapping,
        MasterRecordDeterminationConfig config)
    {
        var stopwatch = Stopwatch.StartNew();

        if (candidates == null || !candidates.Any())
        {
            return Task.FromResult(RuleExecutionResult.NoChange(candidates, "No candidates provided"));
        }

        if (candidates.Count == 1)
        {
            return Task.FromResult(RuleExecutionResult.NoChange(candidates, "Only one candidate"));
        }

        // Apply data source filtering (pre-filter based on SelectedDataSourceIds)
        var eligibleCandidates = candidates.FilterBySelectedDataSources(rule, config);

        if (eligibleCandidates.Count == 1)
        {
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - 1,
                Decision = "Single candidate after data source filtering",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        // Get preferred data source from rule (operation-specific logic)
        var preferredDataSourceId = rule.PreferredDataSourceId;

        if (preferredDataSourceId == null || preferredDataSourceId == Guid.Empty)
        {
            _logger.LogWarning("PreferDataSource rule requires PreferredDataSourceId");
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - eligibleCandidates.Count,
                Decision = "No preferred data source specified",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        // Filter eligible candidates to preferred data source
        var survivors = eligibleCandidates
            .Where(c => c.DataSourceId == preferredDataSourceId)
            .ToList();

        // If no records from preferred source among eligible candidates, keep all eligible
        if (!survivors.Any())
        {
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - eligibleCandidates.Count,
                Decision = $"No records from preferred data source {preferredDataSourceId}, kept eligible candidates",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        stopwatch.Stop();

        var result = new RuleExecutionResult
        {
            SurvivingCandidates = survivors,
            EliminatedCount = candidates.Count - survivors.Count,
            Decision = $"Preferred data source {preferredDataSourceId}, selected {survivors.Count} record(s)",
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };

        _logger.LogDebug(
            "PreferDataSource rule: {Before} -> {After} candidates (preferred DS: {PreferredDS})",
            candidates.Count, survivors.Count, preferredDataSourceId);

        return Task.FromResult(result);
    }
}