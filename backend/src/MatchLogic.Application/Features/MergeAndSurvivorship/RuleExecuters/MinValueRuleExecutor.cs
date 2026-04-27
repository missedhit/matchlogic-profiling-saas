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
/// Selects records with the minimum numeric value for a field
/// </summary>
public class MinValueRuleExecutor : IRuleExecutor
{
    private readonly ILogger<MinValueRuleExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public MinValueRuleExecutor(
        ILogger<MinValueRuleExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger;
        _fieldResolver = fieldResolver;
    }

    public bool CanHandle(MasterRecordOperation operation)
    {
        return operation == MasterRecordOperation.Min;
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

        var scoredCandidates = new List<(RecordCandidate candidate, double value, bool hasValue)>();

        // Score only eligible candidates by numeric value
        foreach (var candidate in eligibleCandidates)
        {
            var fieldValue = _fieldResolver.GetFieldValue(
                candidate.RecordData,
                rule.LogicalFieldName,
                candidate.DataSourceId,
                new List<LogicalFieldMapping> { fieldMapping });

            if (TryConvertToDouble(fieldValue, out var numericValue))
            {
                scoredCandidates.Add((candidate, numericValue, true));
            }
            else
            {
                scoredCandidates.Add((candidate, double.MaxValue, false));
            }
        }

        // Check if any candidate has a valid numeric value
        if (!scoredCandidates.Any(sc => sc.hasValue))
        {
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - eligibleCandidates.Count,
                Decision = "No valid numeric values found",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        // Find the minimum value
        var minValue = scoredCandidates.Where(sc => sc.hasValue).Min(sc => sc.value);

        // Keep only candidates with minimum value
        var survivors = scoredCandidates
            .Where(sc => sc.hasValue && sc.value == minValue)
            .Select(sc => sc.candidate)
            .ToList();

        stopwatch.Stop();

        var result = new RuleExecutionResult
        {
            SurvivingCandidates = survivors,
            EliminatedCount = candidates.Count - survivors.Count,
            Decision = $"Selected {survivors.Count} record(s) with minimum value ({minValue:N2})",
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };

        _logger.LogDebug(
            "MinValue rule: {Before} -> {After} candidates (min: {MinValue})",
            candidates.Count, survivors.Count, minValue);

        return Task.FromResult(result);
    }

    private bool TryConvertToDouble(object value, out double result)
    {
        result = 0;

        if (value == null)
            return false;

        if (value is double d)
        {
            result = d;
            return true;
        }

        if (value is int i)
        {
            result = i;
            return true;
        }

        if (value is long l)
        {
            result = l;
            return true;
        }

        if (value is decimal dec)
        {
            result = (double)dec;
            return true;
        }

        if (value is float f)
        {
            result = f;
            return true;
        }

        // Try parsing string
        if (value is string str)
        {
            return double.TryParse(str, out result);
        }

        return false;
    }
}