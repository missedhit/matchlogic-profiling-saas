using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Domain.MergeAndSurvivorship;
using System.Collections.Generic;

namespace MatchLogic.Application.Interfaces.MergeAndSurvivorship;

/// <summary>
/// Interface for field overwrite rule executors
/// Follows the same pattern as IRuleExecutor (Master Record)
/// </summary>
public interface IOverwriteRuleExecutor
{
    /// <summary>
    /// Executes the overwrite rule and returns the chosen value
    /// </summary>
    /// <param name="records">Records to evaluate</param>
    /// <param name="logicalFieldName">Logical field name (not physical)</param>
    /// <param name="rule">The rule being executed</param>
    /// <param name="fieldMappings">Field mappings for resolving physical names per data source</param>
    /// <returns>The chosen value to overwrite</returns>
    object ExecuteRule(
        List<IDictionary<string, object>> records,
        string logicalFieldName,
        FieldOverwriteRule rule,
        List<LogicalFieldMapping> fieldMappings);

    /// <summary>
    /// Checks if this executor can handle the given operation
    /// </summary>
    bool CanHandle(OverwriteOperation operation);
}
