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
/// Service for determining master records in match groups
/// </summary>
public interface IMasterRecordDeterminationService
{
    /// <summary>
    /// Processes groups asynchronously and determines master records
    /// </summary>
    IAsyncEnumerable<MatchGroup> DetermineAsync(
        IAsyncEnumerable<MatchGroup> groups,
        MasterRecordRuleSet ruleSet,
        Guid projectId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default);

    /// <summary>
    /// Determines master record for a single group
    /// </summary>
    Task<MatchGroup> DetermineForSingleGroupAsync(
        MatchGroup group,
        MasterRecordRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates a rule set before applying it
    /// </summary>
    Task<ValidationResult> ValidateRuleSetAsync(
        MasterRecordRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings);
}

/// <summary>
/// Result of rule set validation
/// </summary>
public class ValidationResult
{
    public bool IsValid { get; set; }
    public List<string> Errors { get; set; }
    public List<string> Warnings { get; set; }

    public ValidationResult()
    {
        Errors = new List<string>();
        Warnings = new List<string>();
    }

    public static ValidationResult Success()
    {
        return new ValidationResult { IsValid = true };
    }

    public static ValidationResult Failure(params string[] errors)
    {
        return new ValidationResult
        {
            IsValid = false,
            Errors = new List<string>(errors)
        };
    }
}
