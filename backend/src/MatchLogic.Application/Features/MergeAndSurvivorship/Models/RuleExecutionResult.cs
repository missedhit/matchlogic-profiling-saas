using System.Collections.Generic;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Result of applying a single rule to a set of candidates
/// </summary>
public class RuleExecutionResult
{
    /// <summary>
    /// Candidates that survived after applying the rule
    /// </summary>
    public List<RecordCandidate> SurvivingCandidates { get; set; }

    /// <summary>
    /// Number of candidates eliminated by this rule
    /// </summary>
    public int EliminatedCount { get; set; }

    /// <summary>
    /// Human-readable description of what the rule decided
    /// </summary>
    public string Decision { get; set; }

    /// <summary>
    /// Time taken to execute this rule in milliseconds
    /// </summary>
    public long ExecutionTimeMs { get; set; }

    /// <summary>
    /// Indicates if the rule successfully reduced the candidate set
    /// </summary>
    public bool ReducedCandidates => EliminatedCount > 0;

    /// <summary>
    /// Indicates if only one candidate remains
    /// </summary>
    public bool SingleCandidateRemaining => SurvivingCandidates?.Count == 1;

    public RuleExecutionResult()
    {
        SurvivingCandidates = new List<RecordCandidate>();
    }

    public static RuleExecutionResult NoChange(List<RecordCandidate> candidates, string reason)
    {
        return new RuleExecutionResult
        {
            SurvivingCandidates = candidates,
            EliminatedCount = 0,
            Decision = reason,
            ExecutionTimeMs = 0
        };
    }

    public override string ToString()
    {
        return $"{SurvivingCandidates.Count} candidates remaining ({EliminatedCount} eliminated) - {Decision}";
    }
}
