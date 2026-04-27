using MatchLogic.Domain.MergeAndSurvivorship;
using System.Collections.Generic;
using System.Threading.Tasks;
using MatchLogic.Application.Features.MergeAndSurvivorship;

namespace MatchLogic.Application.Interfaces.MergeAndSurvivorship;

/// <summary>
/// Interface for rule executors (Strategy pattern)
/// </summary>
public interface IRuleExecutor
{
    /// <summary>
    /// Checks if this executor can handle the given operation
    /// </summary>
    bool CanHandle(MasterRecordOperation operation);

    /// <summary>
    /// Executes the rule on the given candidates
    /// </summary>
    Task<RuleExecutionResult> ExecuteAsync(
        List<RecordCandidate> candidates,
        MasterRecordRule rule,
        LogicalFieldMapping fieldMapping,
        MasterRecordDeterminationConfig config);
}
