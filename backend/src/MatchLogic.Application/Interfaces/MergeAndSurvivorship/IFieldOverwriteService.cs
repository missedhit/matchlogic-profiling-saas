using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MergeAndSurvivorship;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.MergeAndSurvivorship;

/// <summary>
/// Service for overwriting fields in match groups based on rules
/// Follows the same pattern as IMasterRecordDeterminationService
/// </summary>
public interface IFieldOverwriteService
{
    /// <summary>
    /// Processes groups asynchronously and overwrites fields based on rules
    /// </summary>
    IAsyncEnumerable<MatchGroup> OverwriteAsync(
        IAsyncEnumerable<MatchGroup> groups,
        FieldOverwriteRuleSet ruleSet,
        Guid projectId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default);

    /// <summary>
    /// Overwrites fields for a single group
    /// </summary>
    Task<MatchGroup> OverwriteForSingleGroupAsync(
        MatchGroup group,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates a rule set before applying it
    /// </summary>
    Task<ValidationResult> ValidateRuleSetAsync(
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings);
}
