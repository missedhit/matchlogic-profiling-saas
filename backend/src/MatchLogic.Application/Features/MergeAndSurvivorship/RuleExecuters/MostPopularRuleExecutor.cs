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
/// Selects records with the most frequently occurring value (mode)
/// </summary>
public class MostPopularRuleExecutor : IRuleExecutor
{
    private readonly ILogger<MostPopularRuleExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public MostPopularRuleExecutor(
        ILogger<MostPopularRuleExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger;
        _fieldResolver = fieldResolver;
    }

    public bool CanHandle(MasterRecordOperation operation)
    {
        return operation == MasterRecordOperation.MostPopular;
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

        // Build frequency map from eligible candidates only
        var valueFrequency = new Dictionary<string, List<RecordCandidate>>();

        foreach (var candidate in eligibleCandidates)
        {
            var fieldValue = _fieldResolver.GetFieldValue(
                candidate.RecordData,
                rule.LogicalFieldName,
                candidate.DataSourceId,
                new List<LogicalFieldMapping> { fieldMapping });

            var valueKey = fieldValue?.ToString() ?? "<null>";

            if (!valueFrequency.ContainsKey(valueKey))
            {
                valueFrequency[valueKey] = new List<RecordCandidate>();
            }

            valueFrequency[valueKey].Add(candidate);
        }

        // Find the maximum frequency
        var maxFrequency = valueFrequency.Max(kvp => kvp.Value.Count);

        // Get all values with maximum frequency
        var mostPopularValues = valueFrequency
            .Where(kvp => kvp.Value.Count == maxFrequency)
            .ToList();

        // If all values have same frequency, no reduction within eligible candidates
        if (mostPopularValues.Count == valueFrequency.Count)
        {
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - eligibleCandidates.Count,
                Decision = "All values have equal frequency",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        // Collect all candidates with most popular values
        var survivors = mostPopularValues
            .SelectMany(kvp => kvp.Value)
            .ToList();

        stopwatch.Stop();

        var mostPopularValuesList = mostPopularValues
            .Select(kvp => $"{kvp.Key} (x{kvp.Value.Count})")
            .Take(3);

        var result = new RuleExecutionResult
        {
            SurvivingCandidates = survivors,
            EliminatedCount = candidates.Count - survivors.Count,
            Decision = $"Selected {survivors.Count} record(s) with most popular value(s): {string.Join(", ", mostPopularValuesList)}",
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };

        _logger.LogDebug(
            "MostPopular rule: {Before} -> {After} candidates (max frequency: {MaxFreq})",
            candidates.Count, survivors.Count, maxFrequency);

        return Task.FromResult(result);
    }
}