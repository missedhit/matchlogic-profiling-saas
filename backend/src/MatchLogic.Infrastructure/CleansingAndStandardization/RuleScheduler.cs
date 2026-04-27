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
/// Implementation of IRuleScheduler that determines execution order of rules
/// </summary>
public class RuleScheduler : IRuleScheduler
{
    private readonly IRuleRegistry _ruleRegistry;
    private readonly IRuleDependencyResolver _dependencyResolver;
    private readonly ILogger<RuleScheduler> _logger;

    // Cache for optimized rule execution plans by schema signature
    private readonly Dictionary<string, List<TransformationRule>> _executionPlanCache = new Dictionary<string, List<TransformationRule>>();

    /// <summary>
    /// Creates a new rule scheduler
    /// </summary>
    public RuleScheduler(
        IRuleRegistry ruleRegistry,
        IRuleDependencyResolver dependencyResolver,
        ILogger<RuleScheduler> logger)
    {
        _ruleRegistry = ruleRegistry ?? throw new ArgumentNullException(nameof(ruleRegistry));
        _dependencyResolver = dependencyResolver ?? throw new ArgumentNullException(nameof(dependencyResolver));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Gets a list of rules to apply to a record in the correct order
    /// </summary>
    public List<TransformationRule> GetRulesToApply(Record record)
    {
        if (record == null)
            throw new ArgumentNullException(nameof(record));

        // Check if we have a cached execution plan for this record schema
        var schemaSignature = GetSchemaSignature(record.ColumnNames);

        if (_executionPlanCache.TryGetValue(schemaSignature, out var cachedPlan))
        {
            // Use cached plan but filter rules that can't be applied to this specific record
            return cachedPlan
                .Where(rule => rule.CanApply(record))
                .ToList();
        }

        // Get execution plan from registry
        var executionPlan = _ruleRegistry.GetExecutionPlan(record);

        // Cache the execution plan for future records with the same schema
        _executionPlanCache[schemaSignature] = executionPlan;

        return executionPlan;
    }

    /// <summary>
    /// Optimizes the rule execution order based on record schema
    /// </summary>
    public void OptimizeForSchema(IEnumerable<string> columnNames)
    {
        if (columnNames == null)
            throw new ArgumentNullException(nameof(columnNames));

        var schemaSignature = GetSchemaSignature(columnNames);

        // Skip if we already have a cached plan for this schema
        if (_executionPlanCache.ContainsKey(schemaSignature))
            return;

        // Create and cache an execution plan for this schema
        var executionPlan = CreateExecutionPlan(columnNames);
        _executionPlanCache[schemaSignature] = executionPlan;

        _logger.LogInformation("Optimized rule execution plan for schema with {ColumnCount} columns", columnNames.Count());
    }

    /// <summary>
    /// Creates a rule execution plan for a specific set of columns
    /// </summary>
    public List<TransformationRule> CreateExecutionPlan(IEnumerable<string> columnNames)
    {
        if (columnNames == null)
            throw new ArgumentNullException(nameof(columnNames));

        var allRules = _ruleRegistry.GetAllRules().ToList();

        if (!allRules.Any())
        {
            _logger.LogWarning("No rules found in registry");
            return new List<TransformationRule>();
        }

        _logger.LogDebug("Creating execution plan from {TotalRules} total rules for columns: [{Columns}]",
            allRules.Count, string.Join(", ", columnNames));

        // NEW: Find relevant rules considering column flow
        var relevantRules = FindRelevantRulesWithColumnFlow(allRules, columnNames.ToList());

        if (!relevantRules.Any())
        {
            _logger.LogDebug("No relevant rules found for the given column set");
            return new List<TransformationRule>();
        }

        _logger.LogInformation("Found {RelevantCount} relevant rules from {TotalCount} total",
            relevantRules.Count, allRules.Count);

        // Create execution plan with proper dependency resolution and priority ordering
        return CreateExecutionPlanWithDependencies(relevantRules);
    }

    /// <summary>
    /// NEW: Finds rules that are relevant considering column flow
    /// A rule is relevant if its input columns will be available (either initially or created by other rules)
    /// </summary>
    private List<TransformationRule> FindRelevantRulesWithColumnFlow(
        List<TransformationRule> allRules,
        List<string> initialColumns)
    {
        // Step 1: Build the complete set of columns that WILL be available
        // This includes initial columns + all columns that ANY rule could create
        var allPossibleColumns = new HashSet<string>(initialColumns, StringComparer.OrdinalIgnoreCase);

        _logger.LogDebug("Initial columns: [{Columns}]", string.Join(", ", initialColumns));

        // Collect ALL output columns from ALL rules
        foreach (var rule in allRules)
        {
            foreach (var outputCol in rule.OutputColumns)
            {
                if (allPossibleColumns.Add(outputCol))
                {
                    _logger.LogTrace("Rule {RuleType} can create column: {Column}",
                        rule.GetType().Name, outputCol);
                }
            }
        }

        _logger.LogDebug("All possible columns (initial + created): [{Columns}]",
            string.Join(", ", allPossibleColumns));

        // Step 2: A rule is relevant if ALL its input columns will eventually be available
        var relevantRules = new List<TransformationRule>();

        foreach (var rule in allRules)
        {
            bool allInputsAvailable = rule.AffectedColumns.All(col =>
                allPossibleColumns.Contains(col));

            if (allInputsAvailable)
            {
                relevantRules.Add(rule);

                _logger.LogDebug("Rule {RuleType} is relevant - Inputs: [{Inputs}] Outputs: [{Outputs}] Priority: {Priority}",
                    rule.GetType().Name,
                    string.Join(", ", rule.AffectedColumns),
                    string.Join(", ", rule.OutputColumns),
                    rule.Priority);
            }
            else
            {
                var missingInputs = rule.AffectedColumns.Where(col => !allPossibleColumns.Contains(col)).ToList();

                _logger.LogTrace("Rule {RuleType} is NOT relevant - Missing inputs: [{Missing}]",
                    rule.GetType().Name,
                    string.Join(", ", missingInputs));
            }
        }

        return relevantRules;
    }

    /// <summary>
    /// Creates execution plan with dependency resolution and priority ordering
    /// </summary>
    private List<TransformationRule> CreateExecutionPlanWithDependencies(List<TransformationRule> applicableRules)
    {
        if (!applicableRules.Any())
            return new List<TransformationRule>();

        // Build dependency graph including only applicable rules
        var dependencies = new Dictionary<Guid, List<Guid>>();

        foreach (var rule in applicableRules)
        {
            dependencies[rule.Id] = rule.DependsOn
                .Where(depId => applicableRules.Any(r => r.Id == depId))
                .ToList();
        }

        // Resolve implicit dependencies (e.g., priority-based)
        var resolvedDependencies = _dependencyResolver.ResolveImplicitDependencies(applicableRules);

        // Merge explicit and implicit dependencies
        foreach (var kvp in resolvedDependencies)
        {
            if (!dependencies.TryGetValue(kvp.Key, out var deps))
            {
                deps = new List<Guid>();
                dependencies[kvp.Key] = deps;
            }

            foreach (var depId in kvp.Value)
            {
                if (!deps.Contains(depId) && applicableRules.Any(r => r.Id == depId))
                {
                    deps.Add(depId);
                }
            }
        }

        // Create topologically sorted execution plan
        var executionPlanIds = _dependencyResolver.CreateExecutionPlan(dependencies);

        // Convert IDs back to rules and apply final priority ordering
        var executionPlan = executionPlanIds
            .Select(id => applicableRules.FirstOrDefault(r => r.Id == id))
            .Where(rule => rule != null)
            .OrderBy(rule => rule.Priority)
            .ToList();

        _logger.LogInformation("Created execution plan with {RuleCount} rules", executionPlan.Count);

        // Log the execution order
        for (int i = 0; i < executionPlan.Count; i++)
        {
            var rule = executionPlan[i];
            _logger.LogDebug("  {Step}. {RuleType} (Priority: {Priority})",
                i + 1, rule.GetType().Name, rule.Priority);
        }

        return executionPlan;
    }

    /// <summary>
    /// Gets a signature that represents the schema of a record
    /// </summary>
    private string GetSchemaSignature(IEnumerable<string> columnNames)
    {
        // Create a normalized signature of column names (sorted and lowercased)
        return string.Join("|", columnNames
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Select(name => name.ToLowerInvariant()));
    }
}
