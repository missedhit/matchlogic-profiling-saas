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
/// Selects records with the most recent timestamp
/// </summary>
public class MostRecentRuleExecutor : IRuleExecutor
{
    private readonly ILogger<MostRecentRuleExecutor> _logger;
    private readonly ILogicalFieldResolver _fieldResolver;

    public MostRecentRuleExecutor(
        ILogger<MostRecentRuleExecutor> logger,
        ILogicalFieldResolver fieldResolver)
    {
        _logger = logger;
        _fieldResolver = fieldResolver;
    }

    public bool CanHandle(MasterRecordOperation operation)
    {
        return operation == MasterRecordOperation.MostRecent;
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

        var scoredCandidates = new List<(RecordCandidate candidate, DateTime? timestamp)>();

        // Score only eligible candidates by timestamp
        foreach (var candidate in eligibleCandidates)
        {
            var fieldValue = _fieldResolver.GetFieldValue(
                candidate.RecordData,
                rule.LogicalFieldName,
                candidate.DataSourceId,
                new List<LogicalFieldMapping> { fieldMapping });

            var timestamp = TryParseDateTime(fieldValue);
            scoredCandidates.Add((candidate, timestamp));
        }

        // Check if any candidate has a valid timestamp
        if (!scoredCandidates.Any(sc => sc.timestamp.HasValue))
        {
            stopwatch.Stop();
            return Task.FromResult(new RuleExecutionResult
            {
                SurvivingCandidates = eligibleCandidates,
                EliminatedCount = candidates.Count - eligibleCandidates.Count,
                Decision = "No valid timestamps found",
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }

        // Find the most recent timestamp
        var mostRecent = scoredCandidates
            .Where(sc => sc.timestamp.HasValue)
            .Max(sc => sc.timestamp.Value);

        // Keep only candidates with most recent timestamp
        var survivors = scoredCandidates
            .Where(sc => sc.timestamp.HasValue && sc.timestamp.Value == mostRecent)
            .Select(sc => sc.candidate)
            .ToList();

        stopwatch.Stop();

        var result = new RuleExecutionResult
        {
            SurvivingCandidates = survivors,
            EliminatedCount = candidates.Count - survivors.Count,
            Decision = $"Selected {survivors.Count} record(s) with most recent timestamp ({mostRecent:yyyy-MM-dd HH:mm:ss})",
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds
        };

        _logger.LogDebug(
            "MostRecent rule: {Before} -> {After} candidates (most recent: {Timestamp})",
            candidates.Count, survivors.Count, mostRecent);

        return Task.FromResult(result);
    }

    private DateTime? TryParseDateTime(object value)
    {
        if (value == null)
            return null;

        if (value is DateTime dt)
            return dt;

        if (value is DateTimeOffset dto)
            return dto.DateTime;

        if (value is string str)
        {
            if (DateTime.TryParse(str, out var parsed))
                return parsed;
        }

        return null;
    }
}