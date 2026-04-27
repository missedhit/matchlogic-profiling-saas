using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization;

/// <summary>
/// Manager for handling transformation rules
/// </summary>
public class RulesManager : IRulesManager<TransformationRule>
{
    private readonly IRuleRegistry _ruleRegistry;
    private readonly IRuleScheduler _ruleScheduler;
    private readonly ILogger<RulesManager> _logger;

    /// <summary>
    /// Gets the number of registered rules
    /// </summary>
    public int RuleCount => _ruleRegistry.RuleCount;

    /// <summary>
    /// Creates a new rules manager
    /// </summary>
    public RulesManager(
        IRuleRegistry ruleRegistry,
        IRuleScheduler ruleScheduler,
        ILogger<RulesManager> logger)
    {
        _ruleRegistry = ruleRegistry ?? throw new ArgumentNullException(nameof(ruleRegistry));
        _ruleScheduler = ruleScheduler ?? throw new ArgumentNullException(nameof(ruleScheduler));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Gets all registered rules
    /// </summary>
    public IEnumerable<TransformationRule> GetAllRules()
    {
        return _ruleRegistry.GetAllRules();
    }

    /// <summary>
    /// Gets a rule by ID
    /// </summary>
    public TransformationRule GetRuleById(Guid ruleId)
    {
        return _ruleRegistry.GetRuleById(ruleId);
    }

    /// <summary>
    /// Loads rules from a configuration
    /// </summary>
    public Task<bool> LoadRulesFromConfigAsync(EnhancedCleaningRules configuration)
    {
        try
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            // Clear existing rules
            _ruleRegistry.ClearRules();

            // Register rules from configuration
            _ruleRegistry.RegisterRulesFromConfig(configuration);

            _logger.LogInformation("Loaded {RuleCount} rules from configuration", _ruleRegistry.RuleCount);

            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading rules from configuration");
            return Task.FromResult(false);
        }
    }

    /// <summary>
    /// Applies the rule execution plan to a record
    /// </summary>
    public void ApplyRules(Record record)
    {
        if (record == null)
            throw new ArgumentNullException(nameof(record));

        try
        {
            // Get rules to apply in the correct order
            var rulesToApply = _ruleScheduler.GetRulesToApply(record);

            // Apply each rule
            foreach (var rule in rulesToApply)
            {
                rule.Apply(record);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying rules to record");
            throw;
        }
    }

    /// <summary>
    /// Applies the rule execution plan to a batch of records
    /// </summary>
    public void ApplyRules(RecordBatch batch)
    {
        if (batch == null)
            throw new ArgumentNullException(nameof(batch));

        try
        {
            // Optimize rule scheduler for this batch schema
            if (batch.Count > 0)
            {
                var firstRecord = batch[0];
                _ruleScheduler.OptimizeForSchema(firstRecord.ColumnNames);
            }

            // Apply rules to each record
            foreach (var record in batch.Records)
            {
                ApplyRules(record);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying rules to batch");
            throw;
        }
    }

    /// <summary>
    /// Clears all rules
    /// </summary>
    public void ClearRules()
    {
        _ruleRegistry.ClearRules();
    }

    /// <summary>
    /// Gets output schema without executing rules
    /// </summary>
    public async Task<SchemaInfo> GetOutputSchemaAsync(EnhancedCleaningRules configuration)
    {
        // Load rules into registry
        await LoadRulesFromConfigAsync(configuration);

        // Get schema from registry
        var schema = _ruleRegistry.GetOutputSchema();
        schema.TotalRules = _ruleRegistry.RuleCount;

        return schema;
    }

    public Task<SchemaInfo> GetOutputSchemaAsync(IEnumerable<string> inputColumns)
    {
        throw new NotImplementedException();
    }

    public Task<List<string>> GetMergeableColumnsAsync(IEnumerable<string> inputColumns)
    {
        throw new NotImplementedException();
    }

    public Task<ValidationResult> ValidateConfigurationAsync(IEnumerable<string> inputColumns)
    {
        throw new NotImplementedException();
    }
}
