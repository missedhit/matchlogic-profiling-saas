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
/// Selects records with the longest value for a field
/// </summary>
public class LongestValueRuleExecutor : IRuleExecutor
{
    private readonly ILogger<LongestValueRuleExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public LongestValueRuleExecutor(
        ILogger<LongestValueRuleExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger;
        _fieldResolver = fieldResolver;
    }

    public bool CanHandle(Domain.MergeAndSurvivorship.MasterRecordOperation operation)
    {
        return operation == MasterRecordOperation.Longest;
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

        // Score each candidate by string length
        foreach (var candidate in eligibleCandidates)
        {
            var value = _fieldResolver.GetFieldValue(
                candidate.RecordData,
                rule.LogicalFieldName,
                candidate.DataSourceId,
                new List<LogicalFieldMapping> { fieldMapping });

            var length = value?.ToString()?.Length ?? 0;
            scoredCandidates.Add((candidate, length));
        }

        // Find the maximum length
        var maxLength = scoredCandidates.Max(sc => sc.length);

        // Keep only candidates with maximum length
        var survivors = scoredCandidates
            .Where(sc => sc.length == maxLength)
            .Select(sc => sc.candidate)
            .ToList();

        stopwatch.Stop();

        var result = new RuleExecutionResult
        {
            SurvivingCandidates = survivors,
            EliminatedCount = candidates.Count - survivors.Count,
            Decision = $"Selected {survivors.Count} record(s) with longest value (length={maxLength})",
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };

        _logger.LogDebug(
            "LongestValue rule: {Before} -> {After} candidates (max length: {MaxLength})",
            candidates.Count, survivors.Count, maxLength);

        return Task.FromResult(result);
    }
}
