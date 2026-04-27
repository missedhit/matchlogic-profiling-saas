using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;
/// <summary>
/// Represents a column dependency between rules
/// </summary>
public class ColumnDependency
{
    public string SourceColumn { get; set; }
    public string TargetColumn { get; set; }
    public Guid ProducerRuleId { get; set; }
    public Guid ConsumerRuleId { get; set; }
    public DependencyType Type { get; set; }
}

public enum DependencyType
{
    Explicit,      // Manually specified
    Implicit,      // Auto-detected from input/output analysis
    ColumnFlow     // Output of one rule becomes input of another
}

/// <summary>
    /// Enhanced dependency resolver with column flow analysis
    /// </summary>
public interface IEnhancedDependencyResolver : IRuleDependencyResolver
    {
        /// <summary>
        /// Analyzes column dependencies between rules
        /// </summary>
        List<ColumnDependency> AnalyzeColumnDependencies(IEnumerable<EnhancedTransformationRule> rules);

        /// <summary>
        /// Creates execution plan considering column flow dependencies
        /// </summary>
        List<Guid> CreateExecutionPlanWithColumnFlow(
            Dictionary<Guid, List<Guid>> explicitDependencies,
            List<ColumnDependency> columnDependencies);

        /// <summary>
        /// Validates that all required input columns will be available when rules execute
        /// </summary>
        ValidationResult ValidateColumnAvailability(
            IEnumerable<EnhancedTransformationRule> rules,
            IEnumerable<string> initialColumns);
    }

/// <summary>
/// Enhanced rule scheduler with column dependency support
/// </summary>
public interface IEnhancedRuleScheduler : IRuleScheduler
{
    /// <summary>
    /// Gets execution plan considering column dependencies
    /// </summary>
    List<EnhancedTransformationRule> GetEnhancedRulesToApply(Record record);

    /// <summary>
    /// Pre-validates that the record schema supports the rule execution plan
    /// </summary>
    ValidationResult ValidateRecordSchema(Record record);

    /// <summary>
    /// Gets the column flow graph for a set of rules
    /// </summary>
    Dictionary<string, List<string>> GetColumnFlowGraph(IEnumerable<EnhancedTransformationRule> rules);
}

/// <summary>
/// Enhanced dependency resolver that respects both column flow dependencies and rule priorities
/// Priority Logic: Lower numbers execute first UNLESS overridden by column flow dependencies
/// </summary>
public class EnhancedDependencyResolver : IEnhancedDependencyResolver
{
    private readonly ILogger<EnhancedDependencyResolver> _logger;
    private Dictionary<Guid, EnhancedTransformationRule> _ruleCache = new();

    public EnhancedDependencyResolver(ILogger<EnhancedDependencyResolver> logger)
    {
        _logger = logger;
    }

    public List<ColumnDependency> AnalyzeColumnDependencies(IEnumerable<EnhancedTransformationRule> rules)
    {
        var dependencies = new List<ColumnDependency>();
        var rulesList = rules.ToList();

        // Cache rules for priority lookup
        _ruleCache = rulesList.ToDictionary(r => r.Id, r => r);

        // Create mappings of which rules produce/consume which columns
        var columnProducers = new Dictionary<string, List<EnhancedTransformationRule>>(StringComparer.OrdinalIgnoreCase);
        var columnConsumers = new Dictionary<string, List<EnhancedTransformationRule>>(StringComparer.OrdinalIgnoreCase);

        // Build producer/consumer maps
        foreach (var rule in rulesList)
        {
            // Track output columns (producers) - ONLY if rule creates NEW columns
            foreach (var outputCol in rule.OutputColumns)
            {
                // Skip self-transformations (input == output) to avoid false dependencies
                if (!rule.InputColumns.Contains(outputCol, StringComparer.OrdinalIgnoreCase))
                {
                    if (!columnProducers.TryGetValue(outputCol, out var producers))
                    {
                        producers = new List<EnhancedTransformationRule>();
                        columnProducers[outputCol] = producers;
                    }
                    producers.Add(rule);
                }
            }

            // Track input columns (consumers)
            foreach (var inputCol in rule.InputColumns)
            {
                if (!columnConsumers.TryGetValue(inputCol, out var consumers))
                {
                    consumers = new List<EnhancedTransformationRule>();
                    columnConsumers[inputCol] = consumers;
                }
                consumers.Add(rule);
            }
        }

        // Detect column flow dependencies - ONLY for rules that create new columns
        foreach (var rule in rulesList)
        {
            foreach (var inputCol in rule.InputColumns)
            {
                // Find rules that CREATE this input column (not transform it in place)
                if (columnProducers.TryGetValue(inputCol, out var producers))
                {
                    foreach (var producer in producers)
                    {
                        if (producer.Id != rule.Id) // Don't depend on self
                        {
                            dependencies.Add(new ColumnDependency
                            {
                                SourceColumn = inputCol,
                                TargetColumn = inputCol,
                                ProducerRuleId = producer.Id,
                                ConsumerRuleId = rule.Id,
                                Type = DependencyType.ColumnFlow
                            });

                            _logger.LogDebug("Column flow dependency detected: {ProducerType}(P{ProducerPriority}) → {ConsumerType}(P{ConsumerPriority}) via column '{Column}'",
                                producer.GetType().Name, producer.Priority,
                                rule.GetType().Name, rule.Priority,
                                inputCol);
                        }
                    }
                }
            }
        }

        _logger.LogInformation("Analyzed {RuleCount} rules and found {DependencyCount} column dependencies",
            rulesList.Count, dependencies.Count);

        return dependencies;
    }

    public List<Guid> CreateExecutionPlanWithColumnFlow(
        Dictionary<Guid, List<Guid>> explicitDependencies,
        List<ColumnDependency> columnDependencies)
    {
        // Merge explicit and column-flow dependencies
        var allDependencies = new Dictionary<Guid, List<Guid>>();

        // Initialize with all rules (including those with no dependencies)
        foreach (var ruleId in _ruleCache.Keys)
        {
            allDependencies[ruleId] = new List<Guid>();
        }

        // Add explicit dependencies
        foreach (var kvp in explicitDependencies)
        {
            if (allDependencies.ContainsKey(kvp.Key))
            {
                allDependencies[kvp.Key].AddRange(kvp.Value);
            }
            else
            {
                allDependencies[kvp.Key] = new List<Guid>(kvp.Value);
            }
        }

        // Add column flow dependencies (these OVERRIDE priority)
        foreach (var colDep in columnDependencies)
        {
            if (!allDependencies.TryGetValue(colDep.ConsumerRuleId, out var deps))
            {
                deps = new List<Guid>();
                allDependencies[colDep.ConsumerRuleId] = deps;
            }

            if (!deps.Contains(colDep.ProducerRuleId))
            {
                deps.Add(colDep.ProducerRuleId); // Producer MUST execute before consumer

                if (_ruleCache.TryGetValue(colDep.ProducerRuleId, out var producer) &&
                    _ruleCache.TryGetValue(colDep.ConsumerRuleId, out var consumer))
                {
                    _logger.LogDebug("Column flow dependency overrides priority: {ProducerType}(P{ProducerPriority}) → {ConsumerType}(P{ConsumerPriority})",
                        producer.GetType().Name, producer.Priority,
                        consumer.GetType().Name, consumer.Priority);
                }
            }
        }

        // Create execution plan with priority consideration
        return CreateExecutionPlanWithPriority(allDependencies);
    }

    private List<Guid> CreateExecutionPlanWithPriority(Dictionary<Guid, List<Guid>> dependencies)
    {
        var result = new List<Guid>();
        var visited = new HashSet<Guid>();
        var visiting = new HashSet<Guid>(); // Changed from 'temporary' for clarity

        // Get all rule IDs and categorize them
        var allRuleIds = dependencies.Keys.ToList();

        // Start with rules that have no dependencies, ordered by priority (lower = earlier)
        var independentRules = allRuleIds
            .Where(id => !dependencies[id].Any())
            .OrderBy(id => GetRulePriority(id))
            .ThenBy(id => GetRuleTypePriority(id))
            .ToList();

        _logger.LogDebug("Found {IndependentCount} independent rules (no dependencies)", independentRules.Count);

        // Process independent rules first (respecting priority order)
        foreach (var node in independentRules)
        {
            if (!visited.Contains(node))
            {
                VisitWithPriority(node, dependencies, visited, visiting, result);
            }
        }

        // Process remaining rules (those with dependencies) ordered by priority
        var dependentRules = allRuleIds
            .Where(id => !visited.Contains(id))
            .OrderBy(id => GetRulePriority(id))
            .ThenBy(id => GetRuleTypePriority(id))
            .ToList();

        _logger.LogDebug("Processing {DependentCount} dependent rules", dependentRules.Count);

        foreach (var node in dependentRules)
        {
            if (!visited.Contains(node))
            {
                VisitWithPriority(node, dependencies, visited, visiting, result);
            }
        }

        LogExecutionPlan(result);
        return result;
    }

    private void VisitWithPriority(
        Guid node,
        Dictionary<Guid, List<Guid>> dependencies,
        HashSet<Guid> visited,
        HashSet<Guid> visiting,
        List<Guid> result)
    {
        if (visiting.Contains(node))
        {
            var ruleName = _ruleCache.TryGetValue(node, out var rule) ? rule.GetType().Name : node.ToString();
            var chain = GetDependencyChain(node, dependencies, visiting);
            throw new InvalidOperationException($"Circular dependency detected involving rule {ruleName} ({node}). Dependency chain: {string.Join(" → ", chain)}");
        }

        if (!visited.Contains(node))
        {
            visiting.Add(node);

            if (dependencies.TryGetValue(node, out var deps))
            {
                var sortedDeps = deps
                    .Where(depId => _ruleCache.ContainsKey(depId))
                    .OrderBy(id => GetRulePriority(id))
                    .ThenBy(id => GetRuleTypePriority(id))
                    .ToList();

                foreach (var dependency in sortedDeps)
                {
                    VisitWithPriority(dependency, dependencies, visited, visiting, result);
                }
            }

            visiting.Remove(node);
            visited.Add(node);
            result.Add(node);
        }
    }

    private List<string> GetDependencyChain(Guid startNode, Dictionary<Guid, List<Guid>> dependencies, HashSet<Guid> visiting)
    {
        var chain = new List<string>();
        var current = startNode;
        var visited = new HashSet<Guid>(); // Add this to prevent infinite loops

        while (current != Guid.Empty && visiting.Contains(current))
        {
            // Break if we've already processed this node in the chain
            if (visited.Contains(current))
            {
                break;
            }

            visited.Add(current);

            var ruleName = _ruleCache.TryGetValue(current, out var rule) ? rule.GetType().Name : current.ToString();
            chain.Add(ruleName);

            if (dependencies.TryGetValue(current, out var deps))
            {
                current = deps.FirstOrDefault(d => visiting.Contains(d));
            }
            else
            {
                break;
            }

            // Additional safety check - limit chain length
            if (chain.Count > 100) // Reasonable limit
            {
                chain.Add("... (chain truncated)");
                break;
            }
        }

        return chain;
    }

    private int GetRulePriority(Guid ruleId)
    {
        if (_ruleCache.TryGetValue(ruleId, out var rule))
        {
            return rule.Priority;
        }
        return int.MaxValue;
    }

    private int GetRuleTypePriority(Guid ruleId)
    {
        if (_ruleCache.TryGetValue(ruleId, out var rule))
        {
            return rule.GetType().Name switch
            {
                "TestNameParserRule" or "NameParserRule" => 1,
                "TestAddressParserRule" or "AddressParserRule" => 2,
                "EnhancedTextTransformationRule" or "TextTransformationRule" => 3,
                "EnhancedCopyFieldRule" or "CopyFieldRule" => 4,
                "TestConcatenateRule" or "ConcatenateRule" => 5,
                "TestWordSmithRule" or "WordSmithRule" => 6,
                _ => 10
            };
        }
        return 10;
    }

    private void LogExecutionPlan(List<Guid> executionPlan)
    {
        _logger.LogInformation("Execution plan created with {RuleCount} rules:", executionPlan.Count);

        for (int i = 0; i < executionPlan.Count; i++)
        {
            var ruleId = executionPlan[i];
            if (_ruleCache.TryGetValue(ruleId, out var rule))
            {
                _logger.LogDebug("  {Step}. {RuleType} (Priority: {Priority}) - Inputs: [{Inputs}] Outputs: [{Outputs}]",
                    i + 1, rule.GetType().Name, rule.Priority,
                    string.Join(", ", rule.InputColumns),
                    string.Join(", ", rule.OutputColumns));
            }
        }
    }

    public ValidationResult ValidateColumnAvailability(
        IEnumerable<EnhancedTransformationRule> rules,
        IEnumerable<string> initialColumns)
    {
        var result = new ValidationResult { IsValid = true };
        var availableColumns = new HashSet<string>(initialColumns, StringComparer.OrdinalIgnoreCase);
        var rulesList = rules.ToList();

        _ruleCache = rulesList.ToDictionary(r => r.Id, r => r);

        var columnDependencies = AnalyzeColumnDependencies(rulesList);
        var explicitDependencies = rulesList.ToDictionary(r => r.Id, r => r.DependsOn.ToList());

        try
        {
            var executionPlan = CreateExecutionPlanWithColumnFlow(explicitDependencies, columnDependencies);

            foreach (var ruleId in executionPlan)
            {
                if (_ruleCache.TryGetValue(ruleId, out var rule))
                {
                    var missingInputs = rule.InputColumns.Where(col => !availableColumns.Contains(col)).ToList();

                    if (missingInputs.Any())
                    {
                        result.IsValid = false;
                        result.Errors.Add($"Rule {rule.GetType().Name} (Priority: {rule.Priority}) requires columns [{string.Join(", ", missingInputs)}] which are not available");
                    }

                    foreach (var outputCol in rule.OutputColumns)
                    {
                        availableColumns.Add(outputCol);
                    }
                }
            }
        }
        catch (InvalidOperationException ex)
        {
            result.IsValid = false;
            result.Errors.Add($"Dependency resolution failed: {ex.Message}");
        }

        return result;
    }

    public bool ValidateDependencies(Dictionary<Guid, List<Guid>> dependencies)
    {
        try
        {
            CreateExecutionPlan(dependencies);
            return true;
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogError(ex, "Circular dependency detected in rules");
            return false;
        }
    }

    public List<Guid> CreateExecutionPlan(Dictionary<Guid, List<Guid>> dependencies)
    {
        return CreateExecutionPlanWithPriority(dependencies);
    }

    public Dictionary<Guid, List<Guid>> ResolveImplicitDependencies(IEnumerable<TransformationRule> rules)
    {
        var enhancedRules = rules.OfType<EnhancedTransformationRule>().ToList();
        var columnDependencies = AnalyzeColumnDependencies(enhancedRules);

        var result = new Dictionary<Guid, List<Guid>>();

        foreach (var dep in columnDependencies)
        {
            if (!result.TryGetValue(dep.ConsumerRuleId, out var deps))
            {
                deps = new List<Guid>();
                result[dep.ConsumerRuleId] = deps;
            }

            if (!deps.Contains(dep.ProducerRuleId))
            {
                deps.Add(dep.ProducerRuleId);
            }
        }

        return result;
    }
}

/// <summary>
/// Enhanced rule scheduler that extends your existing RuleScheduler with column flow dependency support
/// </summary>
public class EnhancedRuleScheduler : IEnhancedRuleScheduler
{
    private readonly IEnhancedRuleRegistry _ruleRegistry;
    private readonly IEnhancedDependencyResolver _dependencyResolver;
    private readonly ILogger<EnhancedRuleScheduler> _logger;

    private readonly Dictionary<string, List<TransformationRule>> _executionPlanCache = new Dictionary<string, List<TransformationRule>>();
    private readonly Dictionary<string, List<EnhancedTransformationRule>> _enhancedExecutionPlanCache = new Dictionary<string, List<EnhancedTransformationRule>>();

    public EnhancedRuleScheduler(
        IEnhancedRuleRegistry ruleRegistry,
        IEnhancedDependencyResolver dependencyResolver,
        ILogger<EnhancedRuleScheduler> logger)
    {
        _ruleRegistry = ruleRegistry ?? throw new ArgumentNullException(nameof(ruleRegistry));
        _dependencyResolver = dependencyResolver ?? throw new ArgumentNullException(nameof(dependencyResolver));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public List<EnhancedTransformationRule> GetEnhancedRulesToApply(Record record)
    {
        if (record == null)
            throw new ArgumentNullException(nameof(record));

        var schemaSignature = GetSchemaSignature(record.ColumnNames);

        if (_enhancedExecutionPlanCache.TryGetValue(schemaSignature, out var cachedPlan))
        {
            //var applicableRules = cachedPlan.Where(rule => rule.CanApply(record)).ToList();

            _logger.LogDebug("Using cached enhanced execution plan for schema {Schema}: {RuleCount} rules",
          schemaSignature, cachedPlan.Count);

            return cachedPlan;
        }

        var executionPlan = CreateEnhancedExecutionPlan(record.ColumnNames);
        _enhancedExecutionPlanCache[schemaSignature] = executionPlan;

        _logger.LogInformation("Created new enhanced execution plan for schema {Schema}: {TotalRules} total rules",
            schemaSignature, executionPlan.Count);

        // FIXED: Return the full execution plan without filtering
        // Rules will be applied in order, and each rule will check CanApply at execution time
        return executionPlan;
    }

    public ValidationResult ValidateRecordSchema(Record record)
    {
        if (record == null)
            throw new ArgumentNullException(nameof(record));

        var result = new ValidationResult { IsValid = true };

        try
        {
            var allRules = _ruleRegistry.GetAllEnhancedRules().ToList();

            if (!allRules.Any())
            {
                result.Warnings.Add("No enhanced rules found in registry");
                return result;
            }

            return _dependencyResolver.ValidateColumnAvailability(allRules, record.ColumnNames);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating record schema");
            result.IsValid = false;
            result.Errors.Add($"Schema validation error: {ex.Message}");
            return result;
        }
    }

    public Dictionary<string, List<string>> GetColumnFlowGraph(IEnumerable<EnhancedTransformationRule> rules)
    {
        var flowGraph = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var rule in rules)
        {
            foreach (var inputCol in rule.InputColumns)
            {
                if (!flowGraph.TryGetValue(inputCol, out var outputs))
                {
                    outputs = new List<string>();
                    flowGraph[inputCol] = outputs;
                }

                foreach (var outputCol in rule.OutputColumns.Where(o => !outputs.Contains(o, StringComparer.OrdinalIgnoreCase)))
                {
                    outputs.Add(outputCol);
                }
            }

            foreach (var outputCol in rule.OutputColumns.Where(o => !rule.InputColumns.Contains(o, StringComparer.OrdinalIgnoreCase)))
            {
                if (!flowGraph.ContainsKey(outputCol))
                {
                    flowGraph[outputCol] = new List<string>();
                }
            }
        }

        _logger.LogDebug("Built column flow graph with {ColumnCount} columns", flowGraph.Count);
        return flowGraph;
    }

    /// <summary>
    /// FIXED: Creates an enhanced execution plan for a specific set of columns
    /// Now includes rules that will operate on columns created by other rules
    /// </summary>
    public List<EnhancedTransformationRule> CreateEnhancedExecutionPlan(IEnumerable<string> columnNames)
    {
        if (columnNames == null)
            throw new ArgumentNullException(nameof(columnNames));

        try
        {
            var allRules = _ruleRegistry.GetAllEnhancedRules().ToList();

            if (!allRules.Any())
            {
                _logger.LogWarning("No enhanced rules found in registry");
                return new List<EnhancedTransformationRule>();
            }

            _logger.LogDebug("Analyzing {TotalRules} rules for execution plan", allRules.Count);

            var relevantRules = FindRelevantRules(allRules, columnNames.ToList());

            if (!relevantRules.Any())
            {
                _logger.LogDebug("No relevant enhanced rules found for columns: {Columns}",
                    string.Join(", ", columnNames));
                return new List<EnhancedTransformationRule>();
            }

            _logger.LogDebug("Found {RelevantRules} relevant rules from {TotalRules} total rules",
                relevantRules.Count, allRules.Count);

            return CreateExecutionPlanWithEnhancedResolver(relevantRules, _dependencyResolver);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating enhanced execution plan");
            throw;
        }
    }

    /// <summary>
    /// FIXED: Finds all rules that are relevant for the given initial columns,
    /// including rules that operate on columns created by other rules
    /// </summary>
    private List<EnhancedTransformationRule> FindRelevantRules(
        List<EnhancedTransformationRule> allRules,
        List<string> initialColumns)
    {
        var relevantRules = new List<EnhancedTransformationRule>();
        var availableColumns = new HashSet<string>(initialColumns, StringComparer.OrdinalIgnoreCase);
        var processedRules = new HashSet<Guid>();

        bool foundNewRules;
        do
        {
            foundNewRules = false;

            foreach (var rule in allRules)
            {
                if (processedRules.Contains(rule.Id))
                    continue;

                var canProcess = rule.InputColumns.All(col =>
                    availableColumns.Contains(col));

                if (canProcess)
                {
                    relevantRules.Add(rule);
                    processedRules.Add(rule.Id);
                    foundNewRules = true;

                    foreach (var outputCol in rule.OutputColumns)
                    {
                        availableColumns.Add(outputCol);
                    }

                    _logger.LogTrace("Added rule {RuleType} - Inputs: [{Inputs}] Outputs: [{Outputs}]",
                        rule.GetType().Name,
                        string.Join(", ", rule.InputColumns),
                        string.Join(", ", rule.OutputColumns));
                }
            }
        } while (foundNewRules);

        _logger.LogDebug("Found {RelevantCount} relevant rules from initial columns [{InitialColumns}]. Available columns after analysis: [{FinalColumns}]",
            relevantRules.Count,
            string.Join(", ", initialColumns),
            string.Join(", ", availableColumns));

        return relevantRules;
    }

    public List<TransformationRule> GetRulesToApply(Record record)
    {
        return GetEnhancedRulesToApply(record).Cast<TransformationRule>().ToList();
    }

    public void OptimizeForSchema(IEnumerable<string> columnNames)
    {
        if (columnNames == null)
            throw new ArgumentNullException(nameof(columnNames));

        var schemaSignature = GetSchemaSignature(columnNames);

        if (_executionPlanCache.ContainsKey(schemaSignature) && _enhancedExecutionPlanCache.ContainsKey(schemaSignature))
            return;

        try
        {
            var enhancedPlan = CreateEnhancedExecutionPlan(columnNames);
            var regularPlan = enhancedPlan.Cast<TransformationRule>().ToList();

            _enhancedExecutionPlanCache[schemaSignature] = enhancedPlan;
            _executionPlanCache[schemaSignature] = regularPlan;

            _logger.LogInformation("Optimized rule execution plan for schema with {ColumnCount} columns: {RuleCount} rules",
                columnNames.Count(), enhancedPlan.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error optimizing schema {Schema}", schemaSignature);
        }
    }

    public List<TransformationRule> CreateExecutionPlan(IEnumerable<string> columnNames)
    {
        return CreateEnhancedExecutionPlan(columnNames).Cast<TransformationRule>().ToList();
    }

    private List<EnhancedTransformationRule> CreateExecutionPlanWithEnhancedResolver(
        List<EnhancedTransformationRule> applicableRules,
        IEnhancedDependencyResolver enhancedResolver)
    {
        _logger.LogDebug("Creating execution plan with enhanced dependency resolver for {RuleCount} rules", applicableRules.Count);

        var columnDependencies = enhancedResolver.AnalyzeColumnDependencies(applicableRules);

        var explicitDependencies = new Dictionary<Guid, List<Guid>>();
        foreach (var rule in applicableRules)
        {
            explicitDependencies[rule.Id] = rule.DependsOn.ToList();
        }

        var executionPlanIds = enhancedResolver.CreateExecutionPlanWithColumnFlow(explicitDependencies, columnDependencies);

        var executionPlan = executionPlanIds
            .Select(id => applicableRules.FirstOrDefault(r => r.Id == id))
            .Where(rule => rule != null)
            .ToList();

        _logger.LogInformation("Enhanced execution plan created: {PlanCount} rules with {DependencyCount} column dependencies",
            executionPlan.Count, columnDependencies.Count);

        return executionPlan;
    }

    private string GetSchemaSignature(IEnumerable<string> columnNames)
    {
        return string.Join("|", columnNames
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Select(name => name.ToLowerInvariant()));
    }
}