using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization;

/// <summary>
/// Types of operations that can be performed
/// </summary>
public enum OperationType : byte
{
    /// <summary>
    /// Standard cleaning operation on a single column
    /// </summary>
    Standard = 1,

    /// <summary>
    /// Maps a column to one or more output columns
    /// </summary>
    Mapping = 2,

    /// <summary>
    /// Combines multiple input columns
    /// </summary>
    Composite = 3
}

/// <summary>
/// Represents a mapping between source and target columns
/// </summary>
public class DataCleansingColumnMapping
{
    /// <summary>
    /// Source column name
    /// </summary>
    public string SourceColumn { get; set; }

    /// <summary>
    /// Target column name
    /// </summary>
    public string TargetColumn { get; set; }

    /// <summary>
    /// Additional output columns (for complex mappings)
    /// </summary>
    public List<string> OutputColumns { get; set; } = new List<string>();

    /// <summary>
    /// Creates a new instance of the ColumnMapping class
    /// </summary>
    public DataCleansingColumnMapping()
    {
    }

    /// <summary>
    /// Creates a new instance of the ColumnMapping class with the specified source and target columns
    /// </summary>
    public DataCleansingColumnMapping(string sourceColumn, string targetColumn)
    {
        SourceColumn = sourceColumn;
        TargetColumn = targetColumn;
    }

    /// <summary>
    /// Creates a clone of this ColumnMapping
    /// </summary>
    public DataCleansingColumnMapping Clone()
    {
        var clone = new DataCleansingColumnMapping
        {
            SourceColumn = SourceColumn,
            TargetColumn = TargetColumn
        };

        clone.OutputColumns.AddRange(OutputColumns);

        return clone;
    }
}

/// <summary>
/// Extended version of CleaningRule with additional features
/// </summary>
public class ExtendedCleaningRule : CleaningRule
{
    /// <summary>
    /// Type of operation to perform
    /// </summary>
    public OperationType OperationType { get; set; } = OperationType.Standard;

    /// <summary>
    /// Column mappings for this rule
    /// </summary>
    public List<DataCleansingColumnMapping> ColumnMappings { get; set; } = new List<DataCleansingColumnMapping>();

    /// <summary>
    /// IDs of rules this rule depends on
    /// </summary>
    public List<Guid> DependsOnRules { get; set; } = new List<Guid>();

    /// <summary>
    /// Order in which this rule should be executed (lower numbers execute first)
    /// </summary>
    public int ExecutionOrder { get; set; } = 0;

    /// <summary>
    /// Creates a new instance of the ExtendedCleaningRule class
    /// </summary>
    public ExtendedCleaningRule()
    {
    }

    /// <summary>
    /// Creates a new instance of the ExtendedCleaningRule class from a base CleaningRule
    /// </summary>
    public ExtendedCleaningRule(CleaningRule baseRule)
    {
        Id = baseRule.Id;
        ColumnName = baseRule.ColumnName;
        RuleType = baseRule.RuleType;            
        Arguments = new Dictionary<string, string>(baseRule.Arguments);
    }

    /// <summary>
    /// Creates a clone of this ExtendedCleaningRule
    /// </summary>
    public new ExtendedCleaningRule Clone()
    {
        var clone = new ExtendedCleaningRule
        {
            Id = Guid.NewGuid(), // Generate a new ID for the clone
            ColumnName = ColumnName,
            RuleType = RuleType,
            OperationType = OperationType,
            ExecutionOrder = ExecutionOrder
        };

        // Clone the arguments
        foreach (var kvp in Arguments)
        {
            clone.Arguments[kvp.Key] = kvp.Value;
        }

        // Clone the column mappings
        foreach (var mapping in ColumnMappings)
        {
            clone.ColumnMappings.Add(mapping.Clone());
        }

        // Clone the dependencies
        clone.DependsOnRules.AddRange(DependsOnRules);

        return clone;
    }

    /// <summary>
    /// Returns a string representation of this ExtendedCleaningRule
    /// </summary>
    public override string ToString()
    {
        return $"ExtendedCleaningRule: {RuleType} on {ColumnName} (ID: {Id})";
    }
}

/// <summary>
/// Enhanced version of CleaningRules with additional features
/// </summary>
public class EnhancedCleaningRules : CleaningRules
{
    /// <summary>
    /// Extended rules with additional features
    /// </summary>
    public List<ExtendedCleaningRule> ExtendedRules { get; set; } = new List<ExtendedCleaningRule>();

    /// <summary>
    /// Mapping rules for complex transformations
    /// </summary>
    public List<MappingRule> MappingRules { get; set; } = new List<MappingRule>();

    /// <summary>
    /// Rule dependencies for ordering execution
    /// </summary>
    public Dictionary<Guid, List<Guid>> RuleDependencies { get; set; } = new Dictionary<Guid, List<Guid>>();


    /// <summary>
    /// Creates a new instance of the EnhancedCleaningRules class
    /// </summary>
    public EnhancedCleaningRules()
    {
    }
   
    /// <summary>
    /// Adds a standard cleaning rule
    /// </summary>
    public void AddRule(CleaningRule rule)
    {
        if (rule == null)
            throw new ArgumentNullException(nameof(rule));

        Rules.Add(rule);
    }

    /// <summary>
    /// Adds an extended cleaning rule
    /// </summary>
    public void AddExtendedRule(ExtendedCleaningRule rule)
    {
        if (rule == null)
            throw new ArgumentNullException(nameof(rule));

        ExtendedRules.Add(rule);

        // Add dependencies
        foreach (var depId in rule.DependsOnRules)
        {
            AddDependency(rule.Id, depId);
        }
    }

    /// <summary>
    /// Adds a mapping rule
    /// </summary>
    public void AddMappingRule(MappingRule rule)
    {
        if (rule == null)
            throw new ArgumentNullException(nameof(rule));

        MappingRules.Add(rule);
    }

    /// <summary>
    /// Adds a dependency between rules
    /// </summary>
    public void AddDependency(Guid fromRuleId, Guid toRuleId)
    {
        if (!RuleDependencies.TryGetValue(fromRuleId, out var deps))
        {
            deps = new List<Guid>();
            RuleDependencies[fromRuleId] = deps;
        }

        if (!deps.Contains(toRuleId))
        {
            deps.Add(toRuleId);
        }
    }

    /// <summary>
    /// Gets the total number of rules
    /// </summary>
    public int GetTotalRuleCount()
    {
        return Rules.Count + ExtendedRules.Count + MappingRules.Count;
    }        

    /// <summary>
    /// Creates a clone of this EnhancedCleaningRules
    /// </summary>
    public EnhancedCleaningRules Clone()
    {
        var clone = new EnhancedCleaningRules
        {
            Id = Guid.NewGuid(),
            ProjectId = ProjectId,
            ProjectRunId = ProjectRunId,
            DataSourceId = DataSourceId,
        };

        // Map old rule IDs to new rule IDs
        var ruleIdMap = new Dictionary<Guid, Guid>();

        // Clone basic rules
        foreach (var rule in Rules)
        {
            var clonedRule = rule.Clone();
            ruleIdMap[rule.Id] = clonedRule.Id;
            clone.Rules.Add(clonedRule);
        }

        // Clone extended rules
        foreach (var rule in ExtendedRules)
        {
            var clonedRule = rule.Clone();
            ruleIdMap[rule.Id] = clonedRule.Id;

            // Update dependencies to use new rule IDs
            clonedRule.DependsOnRules = clonedRule.DependsOnRules
                .Select(depId => ruleIdMap.ContainsKey(depId) ? ruleIdMap[depId] : depId)
                .ToList();

            clone.ExtendedRules.Add(clonedRule);
        }

        // Clone mapping rules
        foreach (var rule in MappingRules)
        {
            var clonedRule = rule.Clone();
            ruleIdMap[rule.Id] = clonedRule.Id;
            clone.MappingRules.Add(clonedRule);
        }

        // Clone dependencies with updated rule IDs
        foreach (var kvp in RuleDependencies)
        {
            if (ruleIdMap.TryGetValue(kvp.Key, out var newFromRuleId))
            {
                var newDeps = new List<Guid>();

                foreach (var toRuleId in kvp.Value)
                {
                    if (ruleIdMap.TryGetValue(toRuleId, out var newToRuleId))
                    {
                        newDeps.Add(newToRuleId);
                    }
                    else
                    {
                        newDeps.Add(toRuleId);
                    }
                }

                clone.RuleDependencies[newFromRuleId] = newDeps;
            }
        }

        return clone;
    }
}
