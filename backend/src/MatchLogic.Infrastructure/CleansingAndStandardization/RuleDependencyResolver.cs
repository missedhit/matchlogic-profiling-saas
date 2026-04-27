using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization
{
    /// <summary>
    /// Implementation of IRuleDependencyResolver for analyzing rule dependencies
    /// </summary>
    public class RuleDependencyResolver : IRuleDependencyResolver
    {
        private readonly ILogger<RuleDependencyResolver> _logger;

        /// <summary>
        /// Creates a new rule dependency resolver
        /// </summary>
        public RuleDependencyResolver(ILogger<RuleDependencyResolver> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Detects and resolves implicit dependencies between rules
        /// </summary>
        public Dictionary<Guid, List<Guid>> ResolveImplicitDependencies(IEnumerable<TransformationRule> rules)
        {
            if (rules == null)
                throw new ArgumentNullException(nameof(rules));

            var rulesList = rules.ToList();
            var dependencies = new Dictionary<Guid, List<Guid>>();

            // Initialize with explicit dependencies
            foreach (var rule in rulesList)
            {
                dependencies[rule.Id] = rule.DependsOn.ToList();
            }

            // Detect column dependencies (rules that write to columns that other rules read from)
            var columnWriters = new Dictionary<string, List<TransformationRule>>(StringComparer.OrdinalIgnoreCase);
            var columnReaders = new Dictionary<string, List<TransformationRule>>(StringComparer.OrdinalIgnoreCase);

            // Build column writers and readers dictionaries
            foreach (var rule in rulesList)
            {
                foreach (var column in rule.AffectedColumns)
                {
                    if (!columnWriters.TryGetValue(column, out var writers))
                    {
                        writers = new List<TransformationRule>();
                        columnWriters[column] = writers;
                    }

                    writers.Add(rule);
                }

                // TODO: This is a simplification - we would need to analyze the actual implementation
                // to determine which columns are read by each rule. For now, we assume all affected
                // columns are both read and written.
                foreach (var column in rule.AffectedColumns)
                {
                    if (!columnReaders.TryGetValue(column, out var readers))
                    {
                        readers = new List<TransformationRule>();
                        columnReaders[column] = readers;
                    }

                    readers.Add(rule);
                }
            }

            // Add implicit dependencies
            foreach (var rule in rulesList)
            {
                foreach (var column in rule.AffectedColumns)
                {
                    // Find other rules that write to the same column
                    var otherWriters = columnWriters[column]
                        .Where(r => r.Id != rule.Id)
                        .ToList();

                    // Add dependency from lower priority to higher priority rules
                    foreach (var otherRule in otherWriters)
                    {
                        if (rule.Priority > otherRule.Priority)
                        {
                            // Higher priority rule (executed later) depends on lower priority rule (executed earlier)
                            AddDependency(dependencies, rule.Id, otherRule.Id);
                        }
                    }
                }
            }

            return dependencies;
        }

        /// <summary>
        /// Helper method to add a dependency
        /// </summary>
        private void AddDependency(Dictionary<Guid, List<Guid>> dependencies, Guid fromRuleId, Guid toRuleId)
        {
            if (!dependencies.TryGetValue(fromRuleId, out var deps))
            {
                deps = new List<Guid>();
                dependencies[fromRuleId] = deps;
            }

            if (!deps.Contains(toRuleId))
            {
                deps.Add(toRuleId);
            }
        }

        /// <summary>
        /// Validates that there are no circular dependencies between rules
        /// </summary>
        public bool ValidateDependencies(Dictionary<Guid, List<Guid>> dependencies)
        {
            if (dependencies == null)
                throw new ArgumentNullException(nameof(dependencies));

            try
            {
                // We validate by attempting to create an execution plan
                CreateExecutionPlan(dependencies);
                return true;
            }
            catch (InvalidOperationException ex)
            {
                _logger.LogError(ex, "Circular dependency detected in rules");
                return false;
            }
        }

        /// <summary>
        /// Creates an optimized execution plan based on dependencies
        /// </summary>
        public List<Guid> CreateExecutionPlan(Dictionary<Guid, List<Guid>> dependencies)
        {
            if (dependencies == null)
                throw new ArgumentNullException(nameof(dependencies));

            var result = new List<Guid>();
            var visited = new HashSet<Guid>();
            var temporary = new HashSet<Guid>();

            foreach (var node in dependencies.Keys)
            {
                if (!visited.Contains(node))
                {
                    Visit(node, dependencies, visited, temporary, result);
                }
            }

            // The result of topological sort is in reverse order of execution
            result.Reverse();

            return result;
        }

        /// <summary>
        /// Helper method for topological sort
        /// </summary>
        private void Visit(
            Guid node,
            Dictionary<Guid, List<Guid>> dependencies,
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

                if (dependencies.TryGetValue(node, out var deps))
                {
                    foreach (var dependency in deps)
                    {
                        Visit(dependency, dependencies, visited, temporary, result);
                    }
                }

                temporary.Remove(node);
                visited.Add(node);
                result.Add(node);
            }
        }
    }
}
