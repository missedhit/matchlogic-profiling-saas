using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing
{
    /// <summary>
    /// Registry for transformation rules
    /// </summary>
    public interface IRuleRegistry
    {
        /// <summary>
        /// Registers a transformation rule
        /// </summary>
        void RegisterRule(TransformationRule rule);

        /// <summary>
        /// Registers multiple transformation rules
        /// </summary>
        void RegisterRules(IEnumerable<TransformationRule> rules);

        /// <summary>
        /// Creates and registers rules from cleaning rule configuration
        /// </summary>
        void RegisterRulesFromConfig(EnhancedCleaningRules configuration);

        /// <summary>
        /// Gets all registered rules
        /// </summary>
        IEnumerable<TransformationRule> GetAllRules();

        /// <summary>
        /// Gets rules applicable to a specific column
        /// </summary>
        IEnumerable<TransformationRule> GetRulesForColumn(string columnName);

        /// <summary>
        /// Gets rules applicable to a specific record
        /// </summary>
        IEnumerable<TransformationRule> GetRulesForRecord(Record record);

        /// <summary>
        /// Gets a rule by ID
        /// </summary>
        TransformationRule GetRuleById(Guid ruleId);

        /// <summary>
        /// Gets the dependency graph for all rules
        /// </summary>
        Dictionary<Guid, List<Guid>> GetDependencyGraph();

        /// <summary>
        /// Gets an ordered execution plan for the given record
        /// </summary>
        List<TransformationRule> GetExecutionPlan(Record record);

        /// <summary>
        /// Clears all registered rules
        /// </summary>
        void ClearRules();

        /// <summary>
        /// Gets the total number of registered rules
        /// </summary>
        int RuleCount { get; }

        SchemaInfo GetOutputSchema();
    }
}
