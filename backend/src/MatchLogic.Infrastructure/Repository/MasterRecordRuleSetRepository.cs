using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Auth.Interfaces;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Repository;

/// <summary>
/// Repository implementation for MasterRecordRuleSet following MatchLogic repository pattern
/// </summary>
public class MasterRecordRuleSetRepository : GenericRepository<MasterRecordRuleSet, Guid>, IMasterRecordRuleSetRepository
{
    private const string COLLECTION_NAME = "MasterRecordRuleSets";
    private readonly ILogger<MasterRecordRuleSetRepository> _logger;

    public MasterRecordRuleSetRepository(
        Func<StoreType, IDataStore> storeFactory,
        IStoreTypeResolver storeTypeResolver,
        ICurrentUser currentUser,
        ILogger<MasterRecordRuleSetRepository> logger)
        : base(storeFactory, storeTypeResolver, currentUser)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Gets rule set by project ID (most recently updated)
    /// </summary>
    public async Task<MasterRecordRuleSet> GetByProjectIdAsync(Guid projectId)
    {
        try
        {
            var ruleSets = await _dataStore.QueryAsync<MasterRecordRuleSet>(
                rs => rs.ProjectId == projectId,
                COLLECTION_NAME);

            // Return the most recently updated one
            return ruleSets?.FirstOrDefault();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting rule set for project {ProjectId}", projectId);
            throw;
        }
    }

    /// <summary>
    /// Gets the active rule set for a project
    /// </summary>
    public async Task<MasterRecordRuleSet> GetActiveRuleSetAsync(Guid projectId)
    {
        try
        {
            var ruleSets = await _dataStore.QueryAsync<MasterRecordRuleSet>(
                rs => rs.ProjectId == projectId && rs.IsActive,
                COLLECTION_NAME);

            if (ruleSets.Count() > 1)
            {
                _logger.LogWarning(
                    "Multiple active rule sets found for project {ProjectId}, returning most recent",
                    projectId);
            }

            return ruleSets.FirstOrDefault();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting active rule set for project {ProjectId}", projectId);
            throw;
        }
    }

    /// <summary>
    /// Saves a rule set with all its rules
    /// </summary>
    public async Task SaveWithRulesAsync(MasterRecordRuleSet ruleSet)
    {
        try
        {
            if (ruleSet == null)
                throw new ArgumentNullException(nameof(ruleSet));

            // Validate before saving
            if (!ruleSet.IsValid(out var errors))
            {
                var errorMsg = string.Join("; ", errors);
                _logger.LogError("Cannot save invalid rule set: {Errors}", errorMsg);
                throw new InvalidOperationException($"Invalid rule set: {errorMsg}");
            }

            // Check if rule set already exists
            var existing = await _dataStore.GetByIdAsync<MasterRecordRuleSet, Guid>(
                ruleSet.Id,
                COLLECTION_NAME);

            if (existing != null)
            {
                // Update existing
                await _dataStore.UpdateAsync(ruleSet, COLLECTION_NAME);
            }
            else
            {
                // Insert new
                await _dataStore.InsertAsync(ruleSet, COLLECTION_NAME);
            }

            _logger.LogInformation(
                "Saved rule set {RuleSetId} for project {ProjectId} with {RuleCount} rules",
                ruleSet.Id, ruleSet.ProjectId, ruleSet.Rules?.Count ?? 0);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error saving rule set {RuleSetId}", ruleSet?.Id);
            throw;
        }
    }

    /// <summary>
    /// Sets a rule set as active and deactivates all others for the project
    /// </summary>
    public async Task SetActiveRuleSetAsync(Guid projectId, Guid ruleSetId)
    {
        try
        {
            // Get all rule sets for the project
            var allRuleSets = await _dataStore.QueryAsync<MasterRecordRuleSet>(
                rs => rs.ProjectId == projectId,
                COLLECTION_NAME);

            var ruleSetsList = allRuleSets.ToList();

            if (!ruleSetsList.Any(rs => rs.Id == ruleSetId))
            {
                _logger.LogWarning(
                    "Rule set {RuleSetId} not found when trying to set as active",
                    ruleSetId);
                throw new InvalidOperationException($"Rule set {ruleSetId} not found");
            }

            // Update all rule sets
            foreach (var ruleSet in ruleSetsList)
            {
                var wasActive = ruleSet.IsActive;
                ruleSet.IsActive = (ruleSet.Id == ruleSetId);

                if (wasActive != ruleSet.IsActive)
                {
                    await _dataStore.UpdateAsync(ruleSet, COLLECTION_NAME);
                }
            }

            _logger.LogInformation(
                "Set rule set {RuleSetId} as active for project {ProjectId}",
                ruleSetId, projectId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error setting active rule set {RuleSetId} for project {ProjectId}",
                ruleSetId, projectId);
            throw;
        }
    }

    /// <summary>
    /// Gets all rule sets for a project
    /// </summary>
    public async Task<System.Collections.Generic.List<MasterRecordRuleSet>> GetAllByProjectIdAsync(Guid projectId)
    {
        try
        {
            var ruleSets = await _dataStore.QueryAsync<MasterRecordRuleSet>(
                rs => rs.ProjectId == projectId,
                COLLECTION_NAME);

            return ruleSets
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting all rule sets for project {ProjectId}", projectId);
            throw;
        }
    }

    /// <summary>
    /// Deletes a rule set
    /// </summary>
    public async Task DeleteRuleSetAsync(Guid ruleSetId)
    {
        try
        {
            // Check if it's the active rule set
            var ruleSet = await _dataStore.GetByIdAsync<MasterRecordRuleSet, Guid>(
                ruleSetId,
                COLLECTION_NAME);

            if (ruleSet != null && ruleSet.IsActive)
            {
                _logger.LogWarning(
                    "Attempted to delete active rule set {RuleSetId}. Deactivate first.",
                    ruleSetId);
                throw new InvalidOperationException(
                    "Cannot delete active rule set. Activate another rule set first.");
            }

            await _dataStore.DeleteAsync<MasterRecordRuleSet>(ruleSet, COLLECTION_NAME);

            _logger.LogInformation("Deleted rule set {RuleSetId}", ruleSetId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting rule set {RuleSetId}", ruleSetId);
            throw;
        }
    }

    /// <summary>
    /// Checks if a rule set exists
    /// </summary>
    public async Task<bool> ExistsAsync(Guid ruleSetId)
    {
        try
        {
            var ruleSet = await _dataStore.GetByIdAsync<MasterRecordRuleSet, Guid>(
                ruleSetId,
                COLLECTION_NAME);

            return ruleSet != null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if rule set {RuleSetId} exists", ruleSetId);
            throw;
        }
    }

    /// <summary>
    /// Gets rule set by ID
    /// </summary>
    public async Task<MasterRecordRuleSet> GetByIdAsync(Guid ruleSetId)
    {
        try
        {
            return await _dataStore.GetByIdAsync<MasterRecordRuleSet, Guid>(
                ruleSetId,
                COLLECTION_NAME);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting rule set {RuleSetId}", ruleSetId);
            throw;
        }
    }

    /// <summary>
    /// Updates an existing rule set
    /// </summary>
    public async Task UpdateAsync(MasterRecordRuleSet ruleSet)
    {
        try
        {
            if (ruleSet == null)
                throw new ArgumentNullException(nameof(ruleSet));

            // Validate before updating
            if (!ruleSet.IsValid(out var errors))
            {
                var errorMsg = string.Join("; ", errors);
                _logger.LogError("Cannot update invalid rule set: {Errors}", errorMsg);
                throw new InvalidOperationException($"Invalid rule set: {errorMsg}");
            }

            await _dataStore.UpdateAsync(ruleSet, COLLECTION_NAME);

            _logger.LogInformation("Updated rule set {RuleSetId}", ruleSet.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating rule set {RuleSetId}", ruleSet?.Id);
            throw;
        }
    }
}