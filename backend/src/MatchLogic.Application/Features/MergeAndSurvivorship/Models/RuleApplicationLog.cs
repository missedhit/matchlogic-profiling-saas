using System;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Log entry for a single rule application (for auditability)
/// </summary>
public class RuleApplicationLog
{
    public Guid RuleId { get; set; }
    public int Order { get; set; }
    public string LogicalFieldName { get; set; }
    public string Operation { get; set; }
    public int CandidatesBeforeRule { get; set; }
    public int CandidatesAfterRule { get; set; }
    public string RuleDecision { get; set; }
    public long ExecutionTimeMs { get; set; }
    public DateTime AppliedAt { get; set; }

    public RuleApplicationLog()
    {
        AppliedAt = DateTime.UtcNow;
    }

    public override string ToString()
    {
        return $"Rule {Order}: {Operation} on {LogicalFieldName} " +
               $"({CandidatesBeforeRule} -> {CandidatesAfterRule} candidates) " +
               $"in {ExecutionTimeMs}ms";
    }
}
