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
/// Implementation of IRuleRegistry for managing transformation rules
/// </summary>
public class RuleRegistry : IRuleRegistry
{
    private readonly Dictionary<string, List<TransformationRule>> _columnRules = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<Guid, TransformationRule> _rulesById = new();
    private readonly Dictionary<Guid, List<Guid>> _dependencies = new();
    private readonly ILogger<RuleRegistry> _logger;
    private readonly IRuleFactory _ruleFactory;

    /// <summary>
    /// Creates a new rule registry
    /// </summary>
    public RuleRegistry(ILogger<RuleRegistry> logger, IRuleFactory ruleFactory)
    {
        _logger = logger;
        _ruleFactory = ruleFactory;
    }

    /// <summary>
    /// Gets the total number of registered rules
    /// </summary>
    public int RuleCount => _rulesById.Count;

    /// <summary>
    /// Registers a transformation rule
    /// </summary>
    public void RegisterRule(TransformationRule rule)
    {
        // Register rule by ID
        _rulesById[rule.Id] = rule;

        // Map rule to affected columns
        foreach (var column in rule.AffectedColumns)
        {
            if (!_columnRules.TryGetValue(column, out var rules))
            {
                rules = new List<TransformationRule>();
                _columnRules[column] = rules;
            }

            rules.Add(rule);
        }

        // Register dependencies
        _dependencies[rule.Id] = rule.DependsOn.ToList();

        _logger.LogDebug("Registered rule {RuleId} of type {RuleType}", rule.Id, rule.GetType().Name);
    }

    /// <summary>
    /// Registers multiple transformation rules
    /// </summary>
    public void RegisterRules(IEnumerable<TransformationRule> rules)
    {
        foreach (var rule in rules)
        {
            RegisterRule(rule);
        }
    }

    /// <summary>
    /// Creates and registers rules from cleaning rule configuration
    /// </summary>
    public void RegisterRulesFromConfig(EnhancedCleaningRules configuration)
    {
        var rules = new List<TransformationRule>();

        // Process standard cleaning rules
        foreach (var rule in configuration.Rules)
        {
            var transformationRule = _ruleFactory.CreateFromCleaningRule(rule);
            if (transformationRule != null)
            {
                rules.Add(transformationRule);
            }
        }

        // Process extended cleaning rules
        foreach (var rule in configuration.ExtendedRules)
        {
            var transformationRules = _ruleFactory.CreateFromExtendedCleaningRule(rule);
            rules.AddRange(transformationRules);
        }

        // Process mapping rules
        foreach (var rule in configuration.MappingRules)
        {
            var transformationRule = _ruleFactory.CreateFromMappingRule(rule);
            if (transformationRule != null)
            {
                rules.Add(transformationRule);
            }
        }

        // Register all rules
        RegisterRules(rules);

        _logger.LogInformation("Registered {RuleCount} rules from configuration", rules.Count);
    }

    /// <summary>
    /// Gets all registered rules
    /// </summary>
    public IEnumerable<TransformationRule> GetAllRules()
    {
        return _rulesById.Values;
    }

    /// <summary>
    /// Gets rules applicable to a specific column
    /// </summary>
    public IEnumerable<TransformationRule> GetRulesForColumn(string columnName)
    {
        if (_columnRules.TryGetValue(columnName, out var rules))
        {
            return rules;
        }

        return Enumerable.Empty<TransformationRule>();
    }

    /// <summary>
    /// Gets rules applicable to a specific record
    /// </summary>
    public IEnumerable<TransformationRule> GetRulesForRecord(Record record)
    {
        var applicableRules = new HashSet<TransformationRule>();

        // Find all applicable rules based on record's columns
        foreach (var column in record.ColumnNames)
        {
            foreach (var rule in GetRulesForColumn(column))
            {
                if (rule.CanApply(record))
                {
                    applicableRules.Add(rule);
                }
            }
        }

        return applicableRules;
    }

    /// <summary>
    /// Gets a rule by ID
    /// </summary>
    public TransformationRule GetRuleById(Guid ruleId)
    {
        if (_rulesById.TryGetValue(ruleId, out var rule))
        {
            return rule;
        }

        return null;
    }

    /// <summary>
    /// Gets the dependency graph for all rules
    /// </summary>
    public Dictionary<Guid, List<Guid>> GetDependencyGraph()
    {
        // Return a copy to prevent modification
        var result = new Dictionary<Guid, List<Guid>>();

        foreach (var kvp in _dependencies)
        {
            result[kvp.Key] = new List<Guid>(kvp.Value);
        }

        return result;
    }

    /// <summary>
    /// Gets an ordered execution plan for the given record
    /// </summary>
    public List<TransformationRule> GetExecutionPlan(Record record)
    {
        // Get applicable rules
        var applicableRules = GetRulesForRecord(record).ToList();

        // Build a reduced dependency graph with only applicable rules
        var graph = new Dictionary<Guid, List<Guid>>();
        var ruleIds = applicableRules.Select(r => r.Id).ToHashSet();

        foreach (var rule in applicableRules)
        {
            graph[rule.Id] = rule.DependsOn
                .Where(id => ruleIds.Contains(id)) // Only include dependencies on applicable rules
                .ToList();
        }

        // Sort rules by dependencies (topological sort)
        var sortedIds = TopologicalSort(graph);

        // Convert IDs back to rules, preserving the sorted order
        return sortedIds
            .Select(id => _rulesById[id])
            .ToList();
    }

    private List<Guid> TopologicalSort(Dictionary<Guid, List<Guid>> graph)
    {
        var result = new List<Guid>();
        var visited = new HashSet<Guid>();
        var temporary = new HashSet<Guid>();

        // Start with nodes with no dependencies or lowest priority
        var startNodes = graph.Keys
            .Where(id => !graph[id].Any() || !_rulesById.ContainsKey(id))
            .OrderBy(id => _rulesById.TryGetValue(id, out var rule) ? rule.Priority : int.MaxValue)
            .ToList();

        // If no start nodes, use all nodes
        if (!startNodes.Any())
        {
            startNodes = graph.Keys.ToList();
        }

        foreach (var node in startNodes)
        {
            if (!visited.Contains(node))
            {
                TopologicalSortVisit(node, graph, visited, temporary, result);
            }
        }

        return result;
    }

    private void TopologicalSortVisit(
        Guid node,
        Dictionary<Guid, List<Guid>> graph,
        HashSet<Guid> visited,
        HashSet<Guid> temporary,
        List<Guid> result)
    {
        if (temporary.Contains(node))
        {
            throw new InvalidOperationException($"Circular dependency detected involving rule {node}");
        }

        if (!visited.Contains(node))
        {
            temporary.Add(node);

            if (graph.TryGetValue(node, out var dependencies))
            {
                // Sort dependencies by priority
                var sortedDependencies = dependencies
                    .OrderBy(id => _rulesById.TryGetValue(id, out var rule) ? rule.Priority : int.MaxValue)
                    .ToList();

                foreach (var dependency in sortedDependencies)
                {
                    TopologicalSortVisit(dependency, graph, visited, temporary, result);
                }
            }

            temporary.Remove(node);
            visited.Add(node);
            result.Add(node);
        }
    }
    /// <summary>
    /// Clears all registered rules
    /// </summary>
    public void ClearRules()
    {
        _columnRules.Clear();
        _rulesById.Clear();
        _dependencies.Clear();

        _logger.LogInformation("Cleared all rules");
    }

    /// <summary>
    /// Gets all output columns that will be produced by rules
    /// </summary>
    public SchemaInfo GetOutputSchema()
    {
        var allRules = GetAllRules().ToList();

        var schema = new SchemaInfo
        {
            OutputColumns = new List<CleansingColumnInfo>(),
            ColumnFlow = new Dictionary<string, ColumnFlowInfo>()
        };

        foreach (var rule in allRules)
        {
            // Collect all output columns
            foreach (var outputCol in rule.OutputColumns)
            {
                if (!schema.OutputColumns.Any(c => c.Name.Equals(outputCol, StringComparison.OrdinalIgnoreCase)))
                {
                    schema.OutputColumns.Add(new CleansingColumnInfo
                    {
                        Name = outputCol,
                        ProducedBy = rule.GetType().Name,
                        RuleId = rule.Id,
                        IsNewColumn = !rule.AffectedColumns.Contains(outputCol, StringComparer.OrdinalIgnoreCase)
                    });
                }
            }

            // Build column flow graph
            foreach (var outputCol in rule.OutputColumns)
            {
                if (!schema.ColumnFlow.ContainsKey(outputCol))
                {
                    schema.ColumnFlow[outputCol] = new ColumnFlowInfo
                    {
                        ColumnName = outputCol,
                        ProducedBy = new List<string>(),
                        ConsumedBy = new List<string>()
                    };
                }
                schema.ColumnFlow[outputCol].ProducedBy.Add(rule.GetType().Name);
            }

            foreach (var inputCol in rule.AffectedColumns)
            {
                if (!schema.ColumnFlow.ContainsKey(inputCol))
                {
                    schema.ColumnFlow[inputCol] = new ColumnFlowInfo
                    {
                        ColumnName = inputCol,
                        ProducedBy = new List<string>(),
                        ConsumedBy = new List<string>()
                    };
                }
                schema.ColumnFlow[inputCol].ConsumedBy.Add(rule.GetType().Name);
            }
        }

        return schema;
    }
}
