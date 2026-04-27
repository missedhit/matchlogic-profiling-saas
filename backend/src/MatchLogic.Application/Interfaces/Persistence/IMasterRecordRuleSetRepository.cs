using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.MergeAndSurvivorship;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Persistence;

/// <summary>
/// Repository interface for MasterRecordRuleSet
/// </summary>
public interface IMasterRecordRuleSetRepository : IGenericRepository<MasterRecordRuleSet, Guid>
{
    /// <summary>
    /// Gets the rule set for a specific project (most recent)
    /// </summary>
    Task<MasterRecordRuleSet> GetByProjectIdAsync(Guid projectId);

    /// <summary>
    /// Gets the active rule set for a project
    /// </summary>
    Task<MasterRecordRuleSet> GetActiveRuleSetAsync(Guid projectId);

    /// <summary>
    /// Saves a rule set with all its rules
    /// </summary>
    Task SaveWithRulesAsync(MasterRecordRuleSet ruleSet);

    /// <summary>
    /// Sets a rule set as active and deactivates all others for the project
    /// </summary>
    Task SetActiveRuleSetAsync(Guid projectId, Guid ruleSetId);

    /// <summary>
    /// Gets all rule sets for a project
    /// </summary>
    Task<List<MasterRecordRuleSet>> GetAllByProjectIdAsync(Guid projectId);

    /// <summary>
    /// Deletes a rule set
    /// </summary>
    Task DeleteRuleSetAsync(Guid ruleSetId);

    /// <summary>
    /// Checks if a rule set exists
    /// </summary>
    Task<bool> ExistsAsync(Guid ruleSetId);

    /// <summary>
    /// Updates an existing rule set
    /// </summary>
    Task UpdateAsync(MasterRecordRuleSet ruleSet);
}