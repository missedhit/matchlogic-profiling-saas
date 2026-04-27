using MatchLogic.Application.Features.MergeAndSurvivorship.Extensions;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Selects records with first non-null/non-empty value
/// </summary>
public class FirstNonNullRuleExecutor : IRuleExecutor
{
    private readonly ILogger<FirstNonNullRuleExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public FirstNonNullRuleExecutor(
        ILogger<FirstNonNullRuleExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger;
        _fieldResolver = fieldResolver;
    }

    public bool CanHandle(MasterRecordOperation operation)
    {
        return operation == MasterRecordOperation.FirstNonNull;
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

        // Apply data source filtering
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

        // Find all eligible candidates with non-null values
        var candidatesWithValues = new List<RecordCandidate>();

        foreach (var candidate in eligibleCandidates)
        {
            var fieldValue = _fieldResolver.GetFieldValue(
                candidate.RecordData,
                rule.LogicalFieldName,
                candidate.DataSourceId,
                new List<LogicalFieldMapping> { fieldMapping });

            if (HasValue(fieldValue))
            {
                candidatesWithValues.Add(candidate);
            }
        }

        // If no candidates have values, keep all eligible candidates
        if (!candidatesWithValues.Any())
        {
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - eligibleCandidates.Count,
                Decision = "All values are null/empty, kept all eligible candidates",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        // Keep all candidates with non-null values
        var survivors = candidatesWithValues;

        stopwatch.Stop();

        var result = new RuleExecutionResult
        {
            SurvivingCandidates = survivors,
            EliminatedCount = candidates.Count - survivors.Count,
            Decision = $"Selected {survivors.Count} record(s) with non-null values",
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };

        _logger.LogDebug(
            "FirstNonNull rule: {Before} -> {After} candidates",
            candidates.Count, survivors.Count);

        return Task.FromResult(result);
    }

    private bool HasValue(object value)
    {
        if (value == null)
            return false;

        if (value is string str)
            return !string.IsNullOrWhiteSpace(str);

        return true;
    }
}