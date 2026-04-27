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
/// Selects records with the shortest value for a field
/// </summary>
public class ShortestValueRuleExecutor : IRuleExecutor
{
    private readonly ILogger<ShortestValueRuleExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public ShortestValueRuleExecutor(
        ILogger<ShortestValueRuleExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger;
        _fieldResolver = fieldResolver;
    }

    public bool CanHandle(MasterRecordOperation operation)
    {
        return operation == MasterRecordOperation.Shortest;
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

        var scoredCandidates = new List<(RecordCandidate candidate, int length)>();

        // Score only eligible candidates by string length
        foreach (var candidate in eligibleCandidates)
        {
            var value = _fieldResolver.GetFieldValue(
                candidate.RecordData,
                rule.LogicalFieldName,
                candidate.DataSourceId,
                new List<LogicalFieldMapping> { fieldMapping });

            // Treat null/empty as infinite length (so they're not selected as "shortest")
            var length = value?.ToString()?.Length ?? int.MaxValue;

            if (string.IsNullOrWhiteSpace(value?.ToString()))
                length = int.MaxValue;

            scoredCandidates.Add((candidate, length));
        }

        // Find the minimum length (excluding nulls)
        var minLength = scoredCandidates.Min(sc => sc.length);

        // If all are null, keep all eligible candidates
        if (minLength == int.MaxValue)
        {
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - eligibleCandidates.Count,
                Decision = "All values are null or empty",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        // Keep only candidates with minimum length
        var survivors = scoredCandidates
            .Where(sc => sc.length == minLength)
            .Select(sc => sc.candidate)
            .ToList();

        stopwatch.Stop();

        var result = new RuleExecutionResult
        {
            SurvivingCandidates = survivors,
            EliminatedCount = candidates.Count - survivors.Count,
            Decision = $"Selected {survivors.Count} record(s) with shortest value (length={minLength})",
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };

        _logger.LogDebug(
            "ShortestValue rule: {Before} -> {After} candidates (min length: {MinLength})",
            candidates.Count, survivors.Count, minLength);

        return Task.FromResult(result);
    }
}