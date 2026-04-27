using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing;

/// <summary>
/// Interface for managing transformation rules
/// </summary>
public interface IRulesManager<T> where T : TransformationRule
{
    /// <summary>
    /// Gets all registered rules
    /// </summary>
    IEnumerable<T> GetAllRules();

    /// <summary>
    /// Gets a rule by ID
    /// </summary>
    T GetRuleById(Guid ruleId);

    /// <summary>
    /// Loads rules from a configuration
    /// </summary>
    Task<bool> LoadRulesFromConfigAsync(EnhancedCleaningRules configuration);

    /// <summary>
    /// Applies the rule execution plan to a record
    /// </summary>
    void ApplyRules(Record record);

    /// <summary>
    /// Applies the rule execution plan to a batch of records
    /// </summary>
    void ApplyRules(RecordBatch batch);

    /// <summary>
    /// Clears all rules
    /// </summary>
    void ClearRules();

    /// <summary>
    /// Gets the number of registered rules
    /// </summary>
    int RuleCount { get; }

    /// <summary>
    /// Gets the output schema that would result from applying the configured rules
    /// </summary>
    /// <param name="inputColumns">Initial input column names</param>
    /// <returns>Schema information</returns>
    Task<SchemaInfo> GetOutputSchemaAsync(IEnumerable<string> inputColumns);

    /// <summary>
    /// Gets columns available for merging at a specific point in execution
    /// </summary>
    /// <param name="inputColumns">Initial input column names</param>
    /// <returns>List of column names available for merge operations</returns>
    Task<List<string>> GetMergeableColumnsAsync(IEnumerable<string> inputColumns);

    /// <summary>
    /// Validates that a proposed rule configuration would work with the given input columns
    /// </summary>
    /// <param name="inputColumns">Initial input column names</param>
    /// <returns>Validation result with any errors or warnings</returns>
    Task<ValidationResult> ValidateConfigurationAsync(IEnumerable<string> inputColumns);
    Task<SchemaInfo> GetOutputSchemaAsync(EnhancedCleaningRules configuration);
}
