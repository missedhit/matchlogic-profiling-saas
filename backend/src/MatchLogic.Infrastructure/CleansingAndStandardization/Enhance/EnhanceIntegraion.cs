using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
using MatchLogic.Infrastructure.Transformation.Parsers;
using MatchLogic.Parsers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;
/// <summary>
/// Enhanced version of your existing RulesManager with column dependency support
/// </summary>
public class EnhancedRulesManager : IRulesManager<EnhancedTransformationRule>
{
    private readonly IEnhancedRuleRegistry _ruleRegistry;
    private readonly IEnhancedRuleScheduler _ruleScheduler;
    private readonly IEnhancedDependencyResolver _dependencyResolver;
    private readonly ILogger<EnhancedRulesManager> _logger;

    public int RuleCount => _ruleRegistry.RuleCount;

    public EnhancedRulesManager(
        IEnhancedRuleRegistry ruleRegistry,
        IEnhancedRuleScheduler ruleScheduler,
        IEnhancedDependencyResolver dependencyResolver,
        ILogger<EnhancedRulesManager> logger)
    {
        _ruleRegistry = ruleRegistry;
        _ruleScheduler = ruleScheduler;
        _dependencyResolver = dependencyResolver;
        _logger = logger;
    }

    public IEnumerable<EnhancedTransformationRule> GetAllRules()
    {
        return _ruleRegistry.GetAllEnhancedRules();
    }

    public EnhancedTransformationRule GetRuleById(Guid ruleId)
    {
        return _ruleRegistry.GetEnhancedRuleById(ruleId);
    }

    public async Task<bool> LoadRulesFromConfigAsync(EnhancedCleaningRules configuration)
    {
        try
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            _ruleRegistry.ClearRules();

            await _ruleRegistry.RegisterRulesFromConfigAsync(configuration);

            var validationResult = await ValidateRuleConfigurationAsync();
            if (!validationResult.IsValid)
            {
                _logger.LogError("Rule configuration validation failed: {Errors}",
                    string.Join(", ", validationResult.Errors));
                return false;
            }

            _logger.LogInformation("Successfully loaded {RuleCount} rules with {DependencyCount} dependencies",
                _ruleRegistry.RuleCount, _ruleRegistry.GetColumnDependencies().Count);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading rules from configuration");
            return false;
        }
    }

    public void ApplyRules(Record record)
    {
        if (record == null)
            throw new ArgumentNullException(nameof(record));

        try
        {
            var schemaValidation = _ruleScheduler.ValidateRecordSchema(record);
            if (!schemaValidation.IsValid)
            {
                _logger.LogWarning("Record schema validation warnings: {Warnings}",
                    string.Join(", ", schemaValidation.Warnings));

                if (schemaValidation.Errors.Any())
                {
                    _logger.LogError("Record schema validation errors: {Errors}",
                        string.Join(", ", schemaValidation.Errors));
                }
            }

            var rulesToApply = _ruleScheduler.GetEnhancedRulesToApply(record);

            _logger.LogDebug("Applying {RuleCount} rules to record with {ColumnCount} columns",
                rulesToApply.Count, record.ColumnCount);

            foreach (var rule in rulesToApply)
            {
                if (rule.CanApply(record))
                {
                    rule.Apply(record);
                    _logger.LogTrace("Applied rule {RuleType} to record", rule.GetType().Name);
                }
                else
                {
                    _logger.LogDebug("Skipped rule {RuleType} - cannot be applied to current record state",
                        rule.GetType().Name);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying rules to record");
            throw;
        }
    }

    public void ApplyRules(RecordBatch batch)
    {
        if (batch == null)
            throw new ArgumentNullException(nameof(batch));

        try
        {
            if (batch.Count > 0)
            {
                var firstRecord = batch[0];
                _ruleScheduler.OptimizeForSchema(firstRecord.ColumnNames);
            }

            foreach (var record in batch.Records)
            {
                ApplyRules(record);
            }

            _logger.LogDebug("Applied rules to batch of {RecordCount} records", batch.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying rules to batch");
            throw;
        }
    }

    public void ClearRules()
    {
        _ruleRegistry.ClearRules();
    }

    private async Task<ValidationResult> ValidateRuleConfigurationAsync()
    {
        var result = new ValidationResult { IsValid = true };

        try
        {
            var allRules = _ruleRegistry.GetAllEnhancedRules().ToList();

            if (!allRules.Any())
            {
                result.Warnings.Add("No rules loaded");
                return result;
            }

            var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(allRules);
            var explicitDependencies = _ruleRegistry.GetDependencyGraph();

            try
            {
                var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
                    explicitDependencies, columnDependencies);

                _logger.LogInformation("Rule validation successful. Execution plan contains {RuleCount} rules",
                    executionPlan.Count);
            }
            catch (InvalidOperationException ex)
            {
                result.IsValid = false;
                result.Errors.Add($"Circular dependency detected: {ex.Message}");
            }

            var allRuleIds = allRules.Select(r => r.Id).ToHashSet();
            foreach (var rule in allRules)
            {
                var missingDependencies = rule.DependsOn.Where(depId => !allRuleIds.Contains(depId)).ToList();
                if (missingDependencies.Any())
                {
                    result.Warnings.Add($"Rule {rule.GetType().Name} has missing dependencies: {string.Join(", ", missingDependencies)}");
                }
            }

            return result;
        }
        catch (Exception ex)
        {
            result.IsValid = false;
            result.Errors.Add($"Validation error: {ex.Message}");
            return result;
        }
    }
    /// <summary>
    /// Gets the output schema that would result from applying the configured rules
    /// </summary>
    public Task<SchemaInfo> GetOutputSchemaAsync(IEnumerable<string> inputColumns)
    {
        if (inputColumns == null)
            throw new ArgumentNullException(nameof(inputColumns));

        var schema = _ruleRegistry.GetOutputSchema(inputColumns);

        _logger.LogInformation(
            "Generated output schema: {InputCount} input columns -> {OutputCount} output columns " +
            "({NewCount} new), {TotalRules} total rules",
            inputColumns.Count(),
            schema.OutputColumns.Count,
            schema.OutputColumns.Count(c => c.IsNewColumn),
            schema.TotalRules);

        return Task.FromResult(schema);
    }

    /// <summary>
    /// Gets columns available for merging
    /// </summary>
    public Task<List<string>> GetMergeableColumnsAsync(IEnumerable<string> inputColumns)
    {
        if (inputColumns == null)
            throw new ArgumentNullException(nameof(inputColumns));

        var mergeableColumns = _ruleRegistry.GetMergeableColumns(inputColumns);

        _logger.LogDebug("Found {ColumnCount} mergeable columns", mergeableColumns.Count);

        return Task.FromResult(mergeableColumns);
    }

    /// <summary>
    /// Validates that a proposed rule configuration would work
    /// </summary>
    public Task<ValidationResult> ValidateConfigurationAsync(IEnumerable<string> inputColumns)
    {
        if (inputColumns == null)
            throw new ArgumentNullException(nameof(inputColumns));

        var allRules = _ruleRegistry.GetAllEnhancedRules().ToList();
        var result = _dependencyResolver.ValidateColumnAvailability(allRules, inputColumns);

        if (result.IsValid)
        {
            _logger.LogInformation("Rule configuration validated successfully for {ColumnCount} input columns",
                inputColumns.Count());
        }
        else
        {
            _logger.LogWarning("Rule configuration validation failed: {ErrorCount} errors, {WarningCount} warnings",
                result.Errors.Count, result.Warnings.Count);
        }

        return Task.FromResult(result);
    }

    /// <summary>
    /// Gets output schema preview from configuration WITHOUT executing rules.
    /// Used by UI to show what columns will be available after rules execute.
    /// </summary>
    public async Task<SchemaInfo> GetOutputSchemaAsync(EnhancedCleaningRules configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        // Temporarily load rules to analyze schema
        var previousRuleCount = RuleCount;

        try
        {
            _ruleRegistry.ClearRules();
            await _ruleRegistry.RegisterRulesFromConfigAsync(configuration);

            var allRules = _ruleRegistry.GetAllEnhancedRules().ToList();

            // Extract input columns from configuration
            var inputColumns = ExtractInputColumns(configuration);

            var schema = new SchemaInfo
            {
                OutputColumns = new List<CleansingColumnInfo>(),
                ColumnFlow = new Dictionary<string, ColumnFlowInfo>(),
                TotalRules = allRules.Count
            };

            var availableColumns = new HashSet<string>(inputColumns, StringComparer.OrdinalIgnoreCase);

            // Add input columns as original columns
            foreach (var col in inputColumns)
            {
                schema.OutputColumns.Add(new CleansingColumnInfo
                {
                    Name = col,
                    ProducedBy = null,
                    RuleId = Guid.Empty,
                    IsNewColumn = false
                });

                schema.ColumnFlow[col] = new ColumnFlowInfo
                {
                    ColumnName = col,
                    ProducedBy = new List<string>(),
                    ConsumedBy = new List<string>()
                };
            }

            if (!allRules.Any())
                return schema;

            // Get execution plan
            var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(allRules);
            var explicitDependencies = allRules.ToDictionary(r => r.Id, r => r.DependsOn.ToList());

            List<Guid> executionPlan;
            try
            {
                executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(explicitDependencies, columnDependencies);
            }
            catch (InvalidOperationException ex)
            {
                _logger.LogError(ex, "Failed to create execution plan for schema preview");
                return schema;
            }

            // Process rules in order to build schema
            var rulesById = allRules.ToDictionary(r => r.Id);

            foreach (var ruleId in executionPlan)
            {
                if (!rulesById.TryGetValue(ruleId, out var rule))
                    continue;

                // Skip if inputs not available
                if (!rule.InputColumns.All(c => availableColumns.Contains(c)))
                    continue;

                var ruleTypeName = rule.GetType().Name
                    .Replace("Enhanced", "")
                    .Replace("TransformationRule", "");

                // Track what this rule consumes
                foreach (var inputCol in rule.InputColumns)
                {
                    if (schema.ColumnFlow.TryGetValue(inputCol, out var flowInfo))
                    {
                        if (!flowInfo.ConsumedBy.Contains(ruleTypeName))
                            flowInfo.ConsumedBy.Add(ruleTypeName);
                    }
                }

                // Track what this rule produces
                foreach (var outputCol in rule.OutputColumns)
                {
                    if (!availableColumns.Contains(outputCol))
                    {
                        // New column
                        schema.OutputColumns.Add(new CleansingColumnInfo
                        {
                            Name = outputCol,
                            ProducedBy = ruleTypeName,
                            RuleId = rule.Id,
                            IsNewColumn = true
                        });

                        schema.ColumnFlow[outputCol] = new ColumnFlowInfo
                        {
                            ColumnName = outputCol,
                            ProducedBy = new List<string> { ruleTypeName },
                            ConsumedBy = new List<string>()
                        };

                        availableColumns.Add(outputCol);
                    }
                    else
                    {
                        // Modified existing column
                        var existing = schema.OutputColumns.FirstOrDefault(
                            c => c.Name.Equals(outputCol, StringComparison.OrdinalIgnoreCase));

                        if (existing != null && string.IsNullOrEmpty(existing.ProducedBy))
                        {
                            existing.ProducedBy = ruleTypeName;
                            existing.RuleId = rule.Id;
                        }

                        if (schema.ColumnFlow.TryGetValue(outputCol, out var flowInfo))
                        {
                            if (!flowInfo.ProducedBy.Contains(ruleTypeName))
                                flowInfo.ProducedBy.Add(ruleTypeName);
                        }
                    }
                }
            }

            _logger.LogInformation(
                "Schema preview: {InputCount} inputs -> {OutputCount} outputs ({NewCount} new)",
                inputColumns.Count, schema.OutputColumns.Count,
                schema.OutputColumns.Count(c => c.IsNewColumn));

            return schema;
        }
        finally
        {
            // Clear temporary rules
            _ruleRegistry.ClearRules();
        }
    }

    private List<string> ExtractInputColumns(EnhancedCleaningRules configuration)
    {
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var rule in configuration.Rules)
        {
            if (!string.IsNullOrEmpty(rule.ColumnName))
                columns.Add(rule.ColumnName);
        }

        foreach (var rule in configuration.ExtendedRules)
        {
            if (!string.IsNullOrEmpty(rule.ColumnName))
                columns.Add(rule.ColumnName);

            foreach (var mapping in rule.ColumnMappings)
            {
                if (!string.IsNullOrEmpty(mapping.SourceColumn))
                    columns.Add(mapping.SourceColumn);
            }
        }

        foreach (var rule in configuration.MappingRules)
        {
            foreach (var sourceCol in rule.SourceColumn)
            {
                if (!string.IsNullOrEmpty(sourceCol))
                    columns.Add(sourceCol);
            }
        }

        return columns.ToList();
    }
}

/// <summary>
/// Enhanced rule registry interface
/// </summary>
public interface IEnhancedRuleRegistry : IRuleRegistry
{
    /// <summary>
    /// Gets all enhanced transformation rules
    /// </summary>
    IEnumerable<EnhancedTransformationRule> GetAllEnhancedRules();

    /// <summary>
    /// Gets an enhanced rule by ID
    /// </summary>
    EnhancedTransformationRule GetEnhancedRuleById(Guid ruleId);

    /// <summary>
    /// Registers enhanced rules from configuration
    /// </summary>
    Task RegisterRulesFromConfigAsync(EnhancedCleaningRules configuration);

    /// <summary>
    /// Gets column dependencies between rules
    /// </summary>
    List<ColumnDependency> GetColumnDependencies();

    /// <summary>
    /// Gets rules that produce a specific column
    /// </summary>
    IEnumerable<EnhancedTransformationRule> GetRulesProducingColumn(string columnName);

    /// <summary>
    /// Gets rules that consume a specific column
    /// </summary>
    IEnumerable<EnhancedTransformationRule> GetRulesConsumingColumn(string columnName);
    IEnumerable<EnhancedTransformationRule> GetEnhancedRulesForRecord(Record record);

    /// <summary>
    /// Gets the output schema that would result from applying all registered rules
    /// to the given input columns
    /// </summary>
    /// <param name="inputColumns">Initial input column names</param>
    /// <returns>Schema information including all output columns and column flow</returns>
    SchemaInfo GetOutputSchema(IEnumerable<string> inputColumns);

    /// <summary>
    /// Gets columns that can be used as merge sources
    /// (columns that will be available at the merge point in execution)
    /// </summary>
    /// <param name="inputColumns">Initial input column names</param>
    /// <param name="targetMergeRule">Optional: the merge rule to check compatibility for</param>
    /// <returns>List of column names available for merging</returns>
    List<string> GetMergeableColumns(IEnumerable<string> inputColumns, EnhancedTransformationRule targetMergeRule = null);
}

/// <summary>
/// Enhanced rule registry implementation
/// </summary>
public class EnhancedRuleRegistry : IEnhancedRuleRegistry
{
    private readonly Dictionary<string, List<EnhancedTransformationRule>> _columnRules = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<Guid, EnhancedTransformationRule> _rulesById = new();
    private readonly Dictionary<Guid, List<Guid>> _dependencies = new();
    private readonly ILogger<EnhancedRuleRegistry> _logger;
    private readonly IEnhancedRuleFactory _ruleFactory;
    private readonly IEnhancedDependencyResolver _dependencyResolver;
    private List<ColumnDependency> _columnDependencies = new();

    public int RuleCount => _rulesById.Count;

    public EnhancedRuleRegistry(
        ILogger<EnhancedRuleRegistry> logger,
        IEnhancedRuleFactory ruleFactory,
        IEnhancedDependencyResolver dependencyResolver)
    {
        _logger = logger;
        _ruleFactory = ruleFactory;
        _dependencyResolver = dependencyResolver;
    }

    public void RegisterRule(TransformationRule rule)
    {
        if (rule is EnhancedTransformationRule enhancedRule)
        {
            RegisterEnhancedRule(enhancedRule);
        }
        else
        {
            throw new ArgumentException("Rule must be an EnhancedTransformationRule", nameof(rule));
        }
    }

    public void RegisterEnhancedRule(EnhancedTransformationRule rule)
    {
        _rulesById[rule.Id] = rule;

        foreach (var column in rule.AffectedColumns)
        {
            if (!_columnRules.TryGetValue(column, out var rules))
            {
                rules = new List<EnhancedTransformationRule>();
                _columnRules[column] = rules;
            }
            rules.Add(rule);
        }

        _dependencies[rule.Id] = rule.DependsOn.ToList();

        _logger.LogDebug("Registered enhanced rule {RuleId} of type {RuleType}",
            rule.Id, rule.GetType().Name);
    }

    public void RegisterRules(IEnumerable<TransformationRule> rules)
    {
        foreach (var rule in rules)
        {
            RegisterRule(rule);
        }

        RefreshColumnDependencies();
    }

    public async Task RegisterRulesFromConfigAsync(EnhancedCleaningRules configuration)
    {
        var rules = new List<EnhancedTransformationRule>();

        foreach (var rule in configuration.Rules)
        {
            var transformationRule = _ruleFactory.CreateEnhancedFromCleaningRule(rule);
            if (transformationRule != null)
            {
                rules.Add(transformationRule);
            }
        }

        foreach (var rule in configuration.ExtendedRules)
        {
            var transformationRules = _ruleFactory.CreateEnhancedFromExtendedCleaningRule(rule);
            rules.AddRange(transformationRules);
        }

        foreach (var rule in configuration.MappingRules)
        {
            var transformationRule = _ruleFactory.CreateEnhancedFromMappingRule(rule);
            if (transformationRule != null)
            {
                rules.Add(transformationRule);
            }
        }

        foreach (var rule in rules)
        {
            RegisterEnhancedRule(rule);
        }

        RefreshColumnDependencies();

        _logger.LogInformation("Registered {RuleCount} enhanced rules from configuration", rules.Count);
    }

    public void RegisterRulesFromConfig(EnhancedCleaningRules configuration)
    {
        RegisterRulesFromConfigAsync(configuration).GetAwaiter().GetResult();
    }

    public IEnumerable<TransformationRule> GetAllRules()
    {
        return _rulesById.Values.Cast<TransformationRule>();
    }

    public IEnumerable<EnhancedTransformationRule> GetAllEnhancedRules()
    {
        return _rulesById.Values;
    }

    public TransformationRule GetRuleById(Guid ruleId)
    {
        return GetEnhancedRuleById(ruleId);
    }

    public EnhancedTransformationRule GetEnhancedRuleById(Guid ruleId)
    {
        _rulesById.TryGetValue(ruleId, out var rule);
        return rule;
    }

    public IEnumerable<TransformationRule> GetRulesForColumn(string columnName)
    {
        return GetEnhancedRulesForColumn(columnName).Cast<TransformationRule>();
    }

    public IEnumerable<EnhancedTransformationRule> GetEnhancedRulesForColumn(string columnName)
    {
        if (_columnRules.TryGetValue(columnName, out var rules))
        {
            return rules;
        }
        return Enumerable.Empty<EnhancedTransformationRule>();
    }

    public IEnumerable<TransformationRule> GetRulesForRecord(Record record)
    {
        return GetEnhancedRulesForRecord(record).Cast<TransformationRule>();
    }

    public IEnumerable<EnhancedTransformationRule> GetEnhancedRulesForRecord(Record record)
    {
        var applicableRules = new HashSet<EnhancedTransformationRule>();

        foreach (var column in record.ColumnNames)
        {
            foreach (var rule in GetEnhancedRulesForColumn(column))
            {
                if (rule.CanApply(record))
                {
                    applicableRules.Add(rule);
                }
            }
        }

        return applicableRules;
    }

    public Dictionary<Guid, List<Guid>> GetDependencyGraph()
    {
        var result = new Dictionary<Guid, List<Guid>>();
        foreach (var kvp in _dependencies)
        {
            result[kvp.Key] = new List<Guid>(kvp.Value);
        }
        return result;
    }

    public List<TransformationRule> GetExecutionPlan(Record record)
    {
        return GetEnhancedExecutionPlan(record).Cast<TransformationRule>().ToList();
    }

    public List<EnhancedTransformationRule> GetEnhancedExecutionPlan(Record record)
    {
        var applicableRules = GetEnhancedRulesForRecord(record).ToList();
        var explicitDependencies = GetDependencyGraph();

        var executionPlanIds = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            explicitDependencies, _columnDependencies);

        return executionPlanIds
            .Select(id => GetEnhancedRuleById(id))
            .Where(rule => rule != null && applicableRules.Contains(rule))
            .ToList();
    }

    public List<ColumnDependency> GetColumnDependencies()
    {
        return new List<ColumnDependency>(_columnDependencies);
    }

    public IEnumerable<EnhancedTransformationRule> GetRulesProducingColumn(string columnName)
    {
        return _rulesById.Values.Where(rule =>
            rule.OutputColumns.Contains(columnName, StringComparer.OrdinalIgnoreCase));
    }

    public IEnumerable<EnhancedTransformationRule> GetRulesConsumingColumn(string columnName)
    {
        return _rulesById.Values.Where(rule =>
            rule.InputColumns.Contains(columnName, StringComparer.OrdinalIgnoreCase));
    }

    public void ClearRules()
    {
        _columnRules.Clear();
        _rulesById.Clear();
        _dependencies.Clear();
        _columnDependencies.Clear();

        _logger.LogInformation("Cleared all enhanced rules");
    }

    private void RefreshColumnDependencies()
    {
        _columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(_rulesById.Values);
        _logger.LogDebug("Refreshed column dependencies: {DependencyCount} dependencies found",
            _columnDependencies.Count);
    }

    /// <summary>
    /// Gets the output schema that would result from applying all registered rules
    /// </summary>
    public SchemaInfo GetOutputSchema(IEnumerable<string> inputColumns)
    {
        var inputColumnsList = inputColumns.ToList();
        var schema = new SchemaInfo
        {
            OutputColumns = new List<CleansingColumnInfo>(),
            ColumnFlow = new Dictionary<string, ColumnFlowInfo>()
        };

        var availableColumns = new HashSet<string>(inputColumnsList, StringComparer.OrdinalIgnoreCase);
        var allRules = GetAllEnhancedRules().ToList();

        schema.TotalRules = allRules.Count;

        // Initialize with input columns (original columns)
        foreach (var col in inputColumnsList)
        {
            schema.OutputColumns.Add(new CleansingColumnInfo
            {
                Name = col,
                ProducedBy = null,      // Original column, not produced by any rule
                RuleId = Guid.Empty,
                IsNewColumn = false
            });

            schema.ColumnFlow[col] = new ColumnFlowInfo
            {
                ColumnName = col,
                ProducedBy = new List<string>(),    // Not produced by any rule
                ConsumedBy = new List<string>()     // Will be populated as rules are processed
            };
        }

        if (!allRules.Any())
        {
            return schema;
        }

        // Get execution plan to process rules in correct order
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(allRules);
        var explicitDependencies = allRules.ToDictionary(r => r.Id, r => r.DependsOn.ToList());

        List<Guid> executionPlan;
        try
        {
            executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(explicitDependencies, columnDependencies);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogError(ex, "Failed to create execution plan for schema preview");
            return schema;
        }

        // Process rules in execution order to build schema
        var rulesById = allRules.ToDictionary(r => r.Id);
        var processedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var ruleId in executionPlan)
        {
            if (!rulesById.TryGetValue(ruleId, out var rule))
                continue;

            // Check if rule can execute (all inputs available)
            var canExecute = rule.InputColumns.All(col => availableColumns.Contains(col));
            if (!canExecute)
            {
                _logger.LogDebug("Rule {RuleType} skipped in schema preview - missing inputs", rule.GetType().Name);
                continue;
            }

            var ruleTypeName = rule.GetType().Name
                .Replace("Enhanced", "")
                .Replace("TransformationRule", "");

            // Update ConsumedBy for input columns
            foreach (var inputCol in rule.InputColumns)
            {
                if (schema.ColumnFlow.TryGetValue(inputCol, out var inputFlowInfo))
                {
                    if (!inputFlowInfo.ConsumedBy.Contains(ruleTypeName))
                    {
                        inputFlowInfo.ConsumedBy.Add(ruleTypeName);
                    }
                }
            }

            // Process each output column
            foreach (var outputCol in rule.OutputColumns)
            {
                bool isNewColumn = !availableColumns.Contains(outputCol);

                if (isNewColumn)
                {
                    // Add new column to schema
                    schema.OutputColumns.Add(new CleansingColumnInfo
                    {
                        Name = outputCol,
                        ProducedBy = ruleTypeName,
                        RuleId = rule.Id,
                        IsNewColumn = true
                    });

                    // Add column flow for new column
                    schema.ColumnFlow[outputCol] = new ColumnFlowInfo
                    {
                        ColumnName = outputCol,
                        ProducedBy = new List<string> { ruleTypeName },
                        ConsumedBy = new List<string>()
                    };

                    availableColumns.Add(outputCol);
                }
                else if (!processedColumns.Contains(outputCol))
                {
                    // Update existing column info if modified in place
                    var existingCol = schema.OutputColumns.FirstOrDefault(
                        c => c.Name.Equals(outputCol, StringComparison.OrdinalIgnoreCase));

                    if (existingCol != null && string.IsNullOrEmpty(existingCol.ProducedBy))
                    {
                        existingCol.ProducedBy = ruleTypeName;
                        existingCol.RuleId = rule.Id;
                        // IsNewColumn stays false since it was an input column
                    }

                    // Update column flow
                    if (schema.ColumnFlow.TryGetValue(outputCol, out var flowInfo))
                    {
                        if (!flowInfo.ProducedBy.Contains(ruleTypeName))
                        {
                            flowInfo.ProducedBy.Add(ruleTypeName);
                        }
                    }

                    processedColumns.Add(outputCol);
                }
            }
        }

        _logger.LogDebug(
            "Schema preview: {InputCount} inputs -> {OutputCount} outputs ({NewCount} new columns), {TotalRules} rules",
            inputColumnsList.Count,
            schema.OutputColumns.Count,
            schema.OutputColumns.Count(c => c.IsNewColumn),
            schema.TotalRules);

        return schema;
    }

    /// <summary>
    /// Gets columns that can be used as merge sources
    /// </summary>
    public List<string> GetMergeableColumns(IEnumerable<string> inputColumns, EnhancedTransformationRule targetMergeRule = null)
    {
        var availableColumns = new HashSet<string>(inputColumns, StringComparer.OrdinalIgnoreCase);
        var allRules = GetAllEnhancedRules().ToList();

        if (!allRules.Any())
        {
            return availableColumns.OrderBy(c => c).ToList();
        }

        // Get execution plan
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(allRules);
        var explicitDependencies = allRules.ToDictionary(r => r.Id, r => r.DependsOn.ToList());

        List<Guid> executionPlan;
        try
        {
            executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(explicitDependencies, columnDependencies);
        }
        catch (InvalidOperationException)
        {
            return availableColumns.OrderBy(c => c).ToList();
        }

        var rulesById = allRules.ToDictionary(r => r.Id);

        // Find where target merge rule would execute
        int targetIndex = targetMergeRule != null
            ? executionPlan.IndexOf(targetMergeRule.Id)
            : executionPlan.Count; // If no target, return all columns at end

        if (targetIndex < 0)
            targetIndex = executionPlan.Count;

        // Process rules up to (but not including) the target merge rule
        for (int i = 0; i < targetIndex; i++)
        {
            var ruleId = executionPlan[i];
            if (!rulesById.TryGetValue(ruleId, out var rule))
                continue;

            // Check if rule can execute
            var canExecute = rule.InputColumns.All(col => availableColumns.Contains(col));
            if (!canExecute)
                continue;

            // Add output columns
            foreach (var outputCol in rule.OutputColumns)
            {
                availableColumns.Add(outputCol);
            }
        }

        return availableColumns.OrderBy(c => c).ToList();
    }

    public SchemaInfo GetOutputSchema()
    {
        throw new NotImplementedException();
    }
}

/// <summary>
/// Enhanced rule factory implementation based on your current RuleFactory
/// </summary>
public class EnhancedRuleFactory : IEnhancedRuleFactory
{
    private readonly ILogger<EnhancedRuleFactory> _logger;
    private readonly WordSmithDictionaryLoader _wordSmithDictionaryLoader;
    private readonly IWordSmithDictionaryService _dictionaryService;
    private readonly FirstNameParser _firstNameParser;
    private readonly FullNameParserOptimized _fullNameParser;
    private readonly AbbreviationParser _abbreviationParser;
    private readonly ProperCaseOptions _defaultProperCaseOptions;

    /// <summary>
    /// Creates a new enhanced rule factory
    /// </summary>
    public EnhancedRuleFactory(
        ILogger<EnhancedRuleFactory> logger,
        WordSmithDictionaryLoader wordSmithDictionaryLoader,
        IWordSmithDictionaryService dictionaryService,
                FirstNameParser firstNameParser,
        FullNameParserOptimized fullNameParser,
        AbbreviationParser abbreviationParser,
        ProperCaseOptions defaultProperCaseOptions)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _wordSmithDictionaryLoader = wordSmithDictionaryLoader ?? throw new ArgumentNullException(nameof(wordSmithDictionaryLoader));
        _dictionaryService = dictionaryService;
        _firstNameParser = firstNameParser;
        _fullNameParser = fullNameParser;
        _abbreviationParser = abbreviationParser;
        _defaultProperCaseOptions = defaultProperCaseOptions;
    }

    /// <summary>
    /// Creates enhanced transformation rule from cleaning rule
    /// </summary>
    public EnhancedTransformationRule CreateEnhancedFromCleaningRule(CleaningRule rule)
    {
        try
        {
            // Create enhanced text transformation rule for standard cleaning operations
            return new EnhancedTextTransformationRule(
                rule.ColumnName,
                rule.RuleType,
                rule.Arguments,
                _defaultProperCaseOptions,
                    _abbreviationParser);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating enhanced transformation rule from cleaning rule for column {Column}", rule.ColumnName);
            return null;
        }
    }

    /// <summary>
    /// Creates enhanced transformation rules from extended cleaning rule
    /// </summary>
    public IEnumerable<EnhancedTransformationRule> CreateEnhancedFromExtendedCleaningRule(ExtendedCleaningRule rule)
    {
        try
        {
            var rules = new List<EnhancedTransformationRule>();

            // Create the base enhanced text transformation rule
            var baseRule = new EnhancedTextTransformationRule(
                rule.ColumnName,
                rule.RuleType,
                rule.Arguments,
                _defaultProperCaseOptions,
                    _abbreviationParser);
            rules.Add(baseRule);

            // Add enhanced column mappings (copy fields)
            foreach (var mapping in rule.ColumnMappings)
            {
                if (!string.IsNullOrEmpty(mapping.TargetColumn))
                {
                    // Create enhanced copy field rule with dependency on the base rule
                    var copyRule = new EnhancedCopyFieldRule(
                        rule.ColumnName,
                        mapping.TargetColumn
                        //,
                        //new[] { baseRule.Id }
                        ); // Explicit dependency

                    rules.Add(copyRule);
                }
            }

            return rules;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating enhanced transformation rules from extended cleaning rule for column {Column}", rule.ColumnName);
            return Enumerable.Empty<EnhancedTransformationRule>();
        }
    }

    /// <summary>
    /// Creates enhanced transformation rule from mapping rule
    /// </summary>
    public EnhancedTransformationRule CreateEnhancedFromMappingRule(MappingRule rule)
    {
        try
        {
            switch (rule.OperationType)
            {
                case MappingOperationType.AddressParser:
                    string outputPrefix = null;
                    if (rule.MappingConfig != null &&
                        rule.MappingConfig.TryGetValue("outputPrefix", out var prefix))
                    {
                        outputPrefix = prefix;
                    }
                    return new EnhancedAddressTransformationRule(
                        rule.SourceColumn.ToArray(),
                        rule.OutputColumns?.ToDictionary(x => x) ?? new Dictionary<string, string>(),
                        _logger,
                        null, outputPrefix);

                case MappingOperationType.FullNameParser:
                    return CreateFullNameParserRule(rule);

                case MappingOperationType.FirstNameExtractor:
                    return CreateFirstNameExtractorRule(rule);

                case MappingOperationType.Zip:
                    return CreateZipCodeRule(rule);

                case MappingOperationType.MergeFields:
                    return CreateMergeFieldsRule(rule);

                case MappingOperationType.RegexPattern:
                    if (rule.MappingConfig.TryGetValue("pattern", out var pattern))
                    {
                        return new EnhancedRegexTransformationRule(
                            rule.SourceColumn,
                            pattern,
                            _logger,
                            rule.OutputColumns?.ToList() ?? new List<string>(),
                            rule.MappingConfig);
                    }
                    _logger.LogWarning("No pattern specified for regex rule on columns {SourceColumns}",
                        string.Join(", ", rule.SourceColumn));
                    return null;

                case MappingOperationType.WordSmith:
                    return CreateEnhancedWordSmithRule(rule).Result;

                default:
                    _logger.LogWarning("Unsupported mapping operation type: {OperationType}", rule.OperationType);
                    return null;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating enhanced transformation rule from mapping rule for column {Column}",
                string.Join(", ", rule.SourceColumn));
            return null;
        }
    }

    /// <summary>
    /// Creates merge fields rule
    /// </summary>
    private EnhancedTransformationRule CreateMergeFieldsRule(MappingRule rule)
    {
        if (rule.SourceColumn == null || rule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source columns are required for MergeFields rule");
            return null;
        }

        if (rule.OutputColumns == null || rule.OutputColumns.Count == 0)
        {
            _logger.LogWarning("Output column is required for MergeFields rule");
            return null;
        }

        return new EnhancedMergeFieldsTransformationRule(
            rule.SourceColumn,
            rule.OutputColumns.First(),
            rule.MappingConfig ?? new Dictionary<string, string>(),
            _logger);
    }

    /// <summary>
    /// Creates full name parser rule
    /// </summary>
    private EnhancedTransformationRule CreateFullNameParserRule(MappingRule rule)
    {
        if (rule.SourceColumn == null || rule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for FullNameParser rule");
            return null;
        }

        var outputMappings = CreateOutputMappings(rule.OutputColumns);

        return new EnhancedFullNameTransformationRule(
            rule.SourceColumn.First(),
            outputMappings,
            _fullNameParser,
            _firstNameParser,
            _logger);
    }

    /// <summary>
    /// Creates first name extractor rule
    /// </summary>
    private EnhancedTransformationRule CreateFirstNameExtractorRule(MappingRule rule)
    {
        if (rule.SourceColumn == null || rule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for FirstNameExtractor rule");
            return null;
        }

        var outputMappings = CreateOutputMappings(rule.OutputColumns);

        return new EnhancedFirstNameTransformationRule(
            rule.SourceColumn.First(),
            outputMappings,
            _firstNameParser,
            _logger);
    }

    /// <summary>
    /// Creates zip code parser rule
    /// </summary>
    private EnhancedTransformationRule CreateZipCodeRule(MappingRule rule)
    {
        if (rule.SourceColumn == null || rule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for Zip rule");
            return null;
        }

        var outputMappings = CreateOutputMappings(rule.OutputColumns);

        return new EnhancedZipCodeTransformationRule(
            rule.SourceColumn.First(),
            outputMappings,
            _logger);
    }

    /// <summary>
    /// Creates enhanced WordSmith rule based on your current implementation
    /// </summary>
    private async Task<EnhancedTransformationRule> CreateEnhancedWordSmithRule(MappingRule mappingRule)
    {
        if (mappingRule == null)
            throw new ArgumentNullException(nameof(mappingRule));

        if (mappingRule.SourceColumn == null || mappingRule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for WordSmith rule");
            return null;
        }

        try
        {
            var config = mappingRule.MappingConfig ?? new Dictionary<string, string>();

            // Extract configuration values
            string separators = config.GetValueOrDefault("separators", " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`");
            int.TryParse(config.GetValueOrDefault("maxWordCount", "3"), out int maxWordCount);
            bool.TryParse(config.GetValueOrDefault("flagMode", "false"), out bool flagMode);
            bool.TryParse(config.GetValueOrDefault("includeFullText", "true"), out bool includeFullText);

            // Create enhanced WordSmith rule builder with dictionary service
            var builder = new EnhancedWordSmithRuleBuilder(
                mappingRule.SourceColumn.First(),
                _logger,
                _wordSmithDictionaryLoader,
                _dictionaryService) // NEW: Pass dictionary service
                .WithSeparators(separators)
                .WithMaxWordCount(maxWordCount)
                .WithReplacementType(flagMode ? ReplacementType.Flag : ReplacementType.Full)
                .WithIncludeFullText(includeFullText);

            // Load dictionary - check for dictionary ID first, then file path
            // Load dictionary from database using dictionary ID
            if (config.TryGetValue("dictionaryId", out var dictionaryIdStr) &&
                Guid.TryParse(dictionaryIdStr, out var dictionaryId))
            {
                // Load rules from database using dictionary ID
                await builder.WithDictionaryIdAsync(dictionaryId);
            }
            else
            {
                _logger.LogError("No dictionary ID specified for WordSmith rule. Dictionary ID is required.");
                throw new InvalidOperationException("Dictionary ID is required for WordSmith rules. Please specify 'dictionaryId' in the mapping configuration.");
            }

            // Add output columns if specified
            if (mappingRule.OutputColumns != null && mappingRule.OutputColumns.Count > 0)
            {
                foreach (var outputColumn in mappingRule.OutputColumns)
                {
                    if (!string.IsNullOrEmpty(outputColumn))
                    {
                        // Add a placeholder word for new column creation
                        builder.AddNewColumn($"placeholder_{Guid.NewGuid():N}", outputColumn);
                    }
                }
            }

            // Build and return the enhanced rule
            var rule = builder.Build();

            _logger.LogInformation(
                "Created WordSmith rule for column {SourceColumn} using dictionary {DictionaryId}",
                mappingRule.SourceColumn.First(),
                dictionaryId);

            return rule;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating enhanced WordSmith rule for column {Column}",
                string.Join(", ", mappingRule.SourceColumn));
            return null;
        }
    }

    #region Helper Methods

    /// <summary>
    /// Creates output mappings from output columns list
    /// Maps each column name to itself (identity mapping)
    /// </summary>
    private Dictionary<string, string> CreateOutputMappings(List<string> outputColumns)
    {
        if (outputColumns == null || outputColumns.Count == 0)
        {
            return new Dictionary<string, string>();
        }

        return outputColumns
            .Where(c => !string.IsNullOrEmpty(c))
            .ToDictionary(c => c, c => c);
    }

    #endregion
}
/// <summary>
/// Enhanced data cleansing module that integrates with your existing architecture
/// </summary>
public class EnhancedDataCleansingModule : ICleansingModule
{
    private const int BatchSize = 1000;
    private const int MaxDegreeOfParallelism = 4;
    private const int PreviewRecordLimit = 100;
    private readonly IDataStore _dataStore;
    private readonly ILogger<EnhancedDataCleansingModule> _logger;
    private readonly IRulesManager<EnhancedTransformationRule> _rulesManager;
    private readonly IJobEventPublisher _jobEventPublisher;

    public EnhancedDataCleansingModule(
        IDataStore dataStore,
        IRulesManager<EnhancedTransformationRule> rulesManager,
        IJobEventPublisher jobEventPublisher,
        ILogger<EnhancedDataCleansingModule> logger)
    {
        _dataStore = dataStore;
        _logger = logger;
        _rulesManager = rulesManager ?? throw new ArgumentNullException(nameof(rulesManager));
        _jobEventPublisher = jobEventPublisher ?? throw new ArgumentNullException(nameof(jobEventPublisher));
    }

    public async Task<Guid> ProcessDataAsync(
        string inputCollection,
        string outputCollection,
        EnhancedCleaningRules fieldOperations,
        ICommandContext commandContext = null,
        bool isPreview = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _dataStore.DeleteCollection(outputCollection);
            var jobId = await _dataStore.InitializeJobAsync(outputCollection);

            _logger.LogInformation("Starting enhanced cleansing process with job ID: {JobId}", jobId);

            var stepId = commandContext?.StepId ?? Guid.Empty;
            var ruleStep = _jobEventPublisher.CreateStepTracker(stepId, "Applying enhanced transformations", 1, 2);
            var writingStep = _jobEventPublisher.CreateStepTracker(stepId, "Writing data", 2, 2);

            await ruleStep.StartStepAsync(0, cancellationToken);
            await writingStep.StartStepAsync(0, cancellationToken);

            // Load rules into the enhanced rules manager
            var rulesLoaded = await _rulesManager.LoadRulesFromConfigAsync(fieldOperations);
            if (!rulesLoaded)
            {
                throw new InvalidOperationException("Failed to load cleansing rules configuration");
            }

            var dataQueue = new BlockingCollection<List<IDictionary<string, object>>>(MaxDegreeOfParallelism * 2);

            // Start reader task
            var readerTask = Task.Run(
                () => ReadDataFromSourceAsync(jobId, commandContext, dataQueue, ruleStep, inputCollection,isPreview, cancellationToken),
                cancellationToken);

            // Start processor tasks
            var processorTasks = Enumerable.Range(0, MaxDegreeOfParallelism)
                .Select(_ => Task.Run(
                    () => ProcessDataBatchAsync(jobId, commandContext, dataQueue, outputCollection, writingStep, cancellationToken),
                    cancellationToken))
                .ToArray();

            // Wait for completion
            await Task.WhenAll(readerTask.ContinueWith(_ => dataQueue.CompleteAdding()));
            await Task.WhenAll(processorTasks);

            await ruleStep.CompleteStepAsync();
            await writingStep.CompleteStepAsync();

            _logger.LogInformation("Enhanced cleansing process completed successfully for job ID: {JobId}", jobId);

            return jobId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during enhanced cleansing process");
            throw;
        }
    }

    private async Task ReadDataFromSourceAsync(
        Guid jobId,
        ICommandContext flowContext,
        BlockingCollection<List<IDictionary<string, object>>> dataQueue,
        IStepProgressTracker progressTracker,
        string inputCollection,
        bool isPreview, // New parameter
        CancellationToken cancellationToken)
    {
        try
        {
            var batch = new List<IDictionary<string, object>>();
            var totalRecordsRead = 0;
            var recordLimit = isPreview ? PreviewRecordLimit : int.MaxValue;

            _logger.LogInformation(
                "Reading data from source. Preview: {IsPreview}, Record Limit: {Limit}",
                isPreview,
                isPreview ? recordLimit.ToString() : "No limit");

            await foreach (var row in _dataStore.StreamJobDataAsync(jobId, progressTracker, inputCollection, cancellationToken))
            {
                // Check if we've reached the preview limit
                if (totalRecordsRead >= recordLimit)
                {
                    _logger.LogInformation("Preview limit reached: {RecordCount} records read", totalRecordsRead);
                    break;
                }

                batch.Add(row);
                totalRecordsRead++;

                // For preview mode, use smaller batches for better responsiveness
                var currentBatchSize = isPreview ? Math.Min(BatchSize, PreviewRecordLimit) : BatchSize;

                if (batch.Count >= currentBatchSize || totalRecordsRead >= recordLimit)
                {
                    dataQueue.Add(batch, cancellationToken);
                    batch = new List<IDictionary<string, object>>();

                    // Break if we've reached the preview limit
                    if (totalRecordsRead >= recordLimit)
                    {
                        break;
                    }
                }
            }

            // Add any remaining records
            if (batch.Count > 0)
            {
                dataQueue.Add(batch, cancellationToken);
            }

            _logger.LogInformation("Data reading completed. Total records read: {RecordCount}", totalRecordsRead);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading data from source");
            throw;
        }
    }

    private async Task ProcessDataBatchAsync(
        Guid jobId,
        ICommandContext flowContext,
        BlockingCollection<List<IDictionary<string, object>>> dataQueue,
        string outputCollection,
        IStepProgressTracker stepProgressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            foreach (var batch in dataQueue.GetConsumingEnumerable(cancellationToken))
            {
                // Convert to Record batch
                var recordBatch = RecordBatch.FromDictionaries(batch);

                // Apply enhanced transformations with dependency resolution
                _rulesManager.ApplyRules(recordBatch);

                // Update statistics
                if (flowContext?.Statistics != null)
                {
                    lock (flowContext.Statistics)
                    {
                        flowContext.Statistics.RecordsProcessed += recordBatch.Count;
                        flowContext.Statistics.BatchesProcessed++;
                    }
                }

                // Convert back to dictionaries
                var transformedBatch = recordBatch.ToDictionaries();

                await _dataStore.InsertBatchAsync(jobId, transformedBatch, outputCollection);
                await stepProgressTracker.UpdateProgressAsync(transformedBatch.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing data batch");
            throw;
        }
    }

    public Task<SchemaInfo> GetOutputSchemaAsync(EnhancedCleaningRules fieldOperations)
    {
        return _rulesManager.GetOutputSchemaAsync(fieldOperations);
    }
}
