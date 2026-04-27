using MatchLogic.Application.Common;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataProfiling;
using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Service for field overwriting with pipeline pattern
/// CORRECTED: Uses ILogicalFieldResolver like Master Record Determination
/// </summary>
public class FieldOverwriteService : IFieldOverwriteService
{
    private readonly ILogger<FieldOverwriteService> _logger;
    private readonly FieldOverwriteConfig _config;
    private readonly OverwriteRuleExecutorFactory _executorFactory;
    private readonly ILogicalFieldResolver _fieldResolver;
    private readonly IDataSourceIndexMapper _dataSourceMapper;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedFieldRowsRepository;

    private long _totalGroupsProcessed;
    private long _totalFieldsOverwritten;

    public FieldOverwriteService(
        ILogger<FieldOverwriteService> logger,
        IOptions<FieldOverwriteConfig> config,
        OverwriteRuleExecutorFactory executorFactory,
        ILogicalFieldResolver fieldResolver,
        IGenericRepository<DataSource, Guid> dataSourceRepository,
        IGenericRepository<MappedFieldsRow, Guid> mappedFieldRowsRepository,
        IDataSourceIndexMapper dataSourceMapper)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config.Value;
        _executorFactory = executorFactory ?? throw new ArgumentNullException(nameof(executorFactory));
        _fieldResolver = fieldResolver ?? throw new ArgumentNullException(nameof(fieldResolver));
        _dataSourceMapper = dataSourceMapper ?? throw new ArgumentNullException(nameof(dataSourceMapper));
        _dataSourceRepository = dataSourceRepository ?? throw new ArgumentNullException(nameof(dataSourceRepository));
        _mappedFieldRowsRepository = mappedFieldRowsRepository ?? throw new ArgumentNullException(nameof(mappedFieldRowsRepository));

        _totalGroupsProcessed = 0;
        _totalFieldsOverwritten = 0;
    }

    public async IAsyncEnumerable<MatchGroup> OverwriteAsync(
        IAsyncEnumerable<MatchGroup> groups,
        FieldOverwriteRuleSet ruleSet,
        Guid projectId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (groups == null)
            throw new ArgumentNullException(nameof(groups));

        if (ruleSet == null)
            throw new ArgumentNullException(nameof(ruleSet));

        await _dataSourceMapper.InitializeAsync(projectId);

        var mappedFields = await _mappedFieldRowsRepository
            .QueryAsync(mf => mf.ProjectId == projectId, Constants.Collections.MappedFieldRows);

        var dataSources = await _dataSourceRepository
            .QueryAsync(d => d.ProjectId == projectId, Constants.Collections.DataSources);

        var fieldMappings = await _fieldResolver.ResolveLogicalFieldsAsync(
            mappedFields?.First().MappedFields,
            dataSources.ToList());

        if (fieldMappings == null || !fieldMappings.Any())
        {
            _logger.LogWarning("No field mappings provided");
        }

        _logger.LogInformation("Starting field overwriting with {RuleCount} rules", ruleSet.Rules?.Count ?? 0);

        var validation = await ValidateRuleSetAsync(ruleSet, fieldMappings);
        if (!validation.IsValid)
        {
            _logger.LogError("Rule set validation failed: {Errors}", string.Join("; ", validation.Errors));
            throw new InvalidOperationException($"Invalid rule set: {string.Join("; ", validation.Errors)}");
        }

        _totalGroupsProcessed = 0;
        _totalFieldsOverwritten = 0;

        var inputChannel = Channel.CreateBounded<List<MatchGroup>>(
            new BoundedChannelOptions(_config.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        var outputChannel = Channel.CreateBounded<MatchGroup>(
            new BoundedChannelOptions(_config.ChannelCapacity * 2)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        var producerTask = Task.Run(async () =>
        {
            await ProduceBatchesAsync(groups, inputChannel.Writer, cancellationToken);
        }, cancellationToken);

        var consumerTasks = new List<Task>();
        for (int i = 0; i < _config.MaxConcurrentBatches; i++)
        {
            var consumerId = i;
            var task = Task.Run(async () =>
            {
                await ProcessBatchesAsync(inputChannel.Reader, outputChannel.Writer, ruleSet, fieldMappings, consumerId, cancellationToken);
            }, cancellationToken);

            consumerTasks.Add(task);
        }

        var completionTask = Task.Run(async () =>
        {
            await Task.WhenAll(consumerTasks);
            outputChannel.Writer.Complete();
        }, cancellationToken);

        await foreach (var group in outputChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return group;
        }

        await producerTask;
        await completionTask;

        _logger.LogInformation("Field overwriting completed. Groups: {Groups}, Fields: {Fields}",
            _totalGroupsProcessed, _totalFieldsOverwritten);
    }

    public async Task<MatchGroup> OverwriteForSingleGroupAsync(
        MatchGroup group,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        CancellationToken cancellationToken = default)
    {
        if (group == null)
            throw new ArgumentNullException(nameof(group));

        if (ruleSet == null)
            throw new ArgumentNullException(nameof(ruleSet));

        return await ProcessGroupAsync(group, ruleSet, fieldMappings, cancellationToken);
    }

    public Task<ValidationResult> ValidateRuleSetAsync(
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings)
    {
        var result = new ValidationResult();
        result.IsValid = true;

        if (ruleSet == null)
        {
            result.IsValid = false;
            result.Errors.Add("Rule set is null");
            return Task.FromResult(result);
        }

        if (!ruleSet.IsValid(out var errors))
        {
            result.IsValid = false;
            result.Errors.AddRange(errors);
        }

        if (fieldMappings == null || !fieldMappings.Any())
        {
            result.Warnings.Add("No field mappings provided");
        }
        else
        {
            foreach (var rule in ruleSet.GetActiveRulesSorted())
            {
                var mapping = fieldMappings.FirstOrDefault(fm =>
                    fm.LogicalFieldName.Equals(rule.LogicalFieldName, StringComparison.OrdinalIgnoreCase));

                if (mapping == null)
                {
                    result.Warnings.Add($"Logical field '{rule.LogicalFieldName}' in rule {rule.Order} not found");
                }
            }
        }

        result.IsValid = result.IsValid && !result.Errors.Any();
        return Task.FromResult(result);
    }

    private async Task ProduceBatchesAsync(
        IAsyncEnumerable<MatchGroup> groups,
        ChannelWriter<List<MatchGroup>> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            var batch = new List<MatchGroup>();

            await foreach (var group in groups.WithCancellation(cancellationToken))
            {
                batch.Add(group);

                if (batch.Count >= _config.BatchSize)
                {
                    await writer.WriteAsync(batch, cancellationToken);
                    batch = new List<MatchGroup>();
                }
            }

            if (batch.Any())
            {
                await writer.WriteAsync(batch, cancellationToken);
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in producer");
            writer.Complete(ex);
            throw;
        }
    }

    private async Task ProcessBatchesAsync(
        ChannelReader<List<MatchGroup>> reader,
        ChannelWriter<MatchGroup> writer,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        int consumerId,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var batch in reader.ReadAllAsync(cancellationToken))
            {
                foreach (var group in batch)
                {
                    var processedGroup = await ProcessGroupAsync(group, ruleSet, fieldMappings, cancellationToken);
                    await writer.WriteAsync(processedGroup, cancellationToken);
                    Interlocked.Increment(ref _totalGroupsProcessed);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in consumer {ConsumerId}", consumerId);
            throw;
        }
    }

    private async Task<MatchGroup> ProcessGroupAsync(
        MatchGroup group,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        CancellationToken cancellationToken)
    {
        try
        {
            var activeRules = ruleSet.GetActiveRulesSorted();

            foreach (var rule in activeRules)
            {
                // Verify logical field exists in mappings
                var fieldMapping = fieldMappings.FirstOrDefault(fm =>
                    fm.LogicalFieldName.Equals(rule.LogicalFieldName, StringComparison.OrdinalIgnoreCase));

                if (fieldMapping == null)
                {
                    _logger.LogWarning("Logical field '{LogicalField}' not found, skipping", rule.LogicalFieldName);
                    continue;
                }

                var executor = _executorFactory.GetExecutor(rule.Operation);
                if (executor == null)
                {
                    _logger.LogWarning("No executor for operation {Operation}, skipping", rule.Operation);
                    continue;
                }

                // Filter records by data source
                var eligibleRecords = FilterRecordsByDataSource(group.Records, rule);

                if (!eligibleRecords.Any())
                {
                    _logger.LogDebug("No eligible records for rule {Order}", rule.Order);
                    continue;
                }

                // CORRECTED: Pass logical field name and field mappings
                // Executor will use ILogicalFieldResolver to get physical field name per record
                var chosenValue = executor.ExecuteRule(
                    eligibleRecords,
                    rule.LogicalFieldName,  // Logical name, not physical
                    rule,
                    fieldMappings);         // All mappings

                // CORRECTED: Apply value using logical field name
                // Will resolve physical field name per record based on data source
                var overwrittenCount = ApplyValueToRecords(
                    group.Records,
                    rule.LogicalFieldName,  // Logical name
                    chosenValue,
                    rule,
                    fieldMappings);         // All mappings

                Interlocked.Add(ref _totalFieldsOverwritten, overwrittenCount);

                if (_config.EnableDetailedLogging && overwrittenCount > 0)
                {
                    _logger.LogDebug("Group {GroupId}, Rule {Order}: Overwritten {Count} values",
                        group.GroupId, rule.Order, overwrittenCount);
                }
            }

            return group;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing group {GroupId}", group.GroupId);
            throw;
        }
    }

    private List<IDictionary<string, object>> FilterRecordsByDataSource(
        List<IDictionary<string, object>> records,
        FieldOverwriteRule rule)
    {
        if (rule.DataSourceFilters == null || !rule.DataSourceFilters.Any())
            return records;

        return records.Where(r =>
        {
            var dataSourceId = ExtractDataSourceId(r);
            return rule.DataSourceFilters.Contains(dataSourceId);
        }).ToList();
    }

    /// <summary>
    /// CORRECTED: Applies value to records using logical field name
    /// Resolves physical field name per record based on data source
    /// </summary>
    private int ApplyValueToRecords(
        List<IDictionary<string, object>> records,
        string logicalFieldName,
        object chosenValue,
        FieldOverwriteRule rule,
        List<LogicalFieldMapping> fieldMappings)
    {
        int overwrittenCount = 0;

        foreach (var record in records)
        {
            // Check if this record should be overwritten
            if (ShouldOverwriteRecord(record, logicalFieldName, chosenValue, rule, fieldMappings))
            {
                // Extract data source ID for this record
                var dataSourceId = ExtractDataSourceId(record);

                // CORRECTED: Use field resolver to get physical field name for this data source
                var physicalFieldName = GetPhysicalFieldName(
                    logicalFieldName,
                    dataSourceId,
                    fieldMappings);

                if (string.IsNullOrEmpty(physicalFieldName))
                {
                    _logger.LogWarning("Could not resolve physical field name for logical field '{Logical}' in data source {DataSource}",
                        logicalFieldName, dataSourceId);
                    continue;
                }

                // Store old value for audit
                var oldValue = record.TryGetValue(physicalFieldName, out var val) ? val : null;

                // Apply new value to the correct physical field
                record[physicalFieldName] = chosenValue;
                overwrittenCount++;

                if (_config.EnableDetailedLogging)
                {
                    _logger.LogTrace("Overwritten {Physical} (logical: {Logical}): '{Old}' -> '{New}'",
                        physicalFieldName, logicalFieldName,
                        oldValue?.ToString() ?? "(null)",
                        chosenValue?.ToString() ?? "(null)");
                }
            }
        }

        return overwrittenCount;
    }

    /// <summary>
    /// CORRECTED: Checks if record should be overwritten using logical field name
    /// </summary>
    private bool ShouldOverwriteRecord(
        IDictionary<string, object> record,
        string logicalFieldName,
        object chosenValue,
        FieldOverwriteRule rule,
        List<LogicalFieldMapping> fieldMappings)
    {
        if (rule.OverwriteIf != OverwriteCondition.NoCondition)
        {
            if (!MeetsCondition(record, logicalFieldName, chosenValue, rule.OverwriteIf, fieldMappings))
                return false;
        }

        if (rule.DoNotOverwriteIf != OverwriteCondition.NoCondition)
        {
            if (MeetsCondition(record, logicalFieldName, chosenValue, rule.DoNotOverwriteIf, fieldMappings))
                return false;
        }

        return true;
    }

    /// <summary>
    /// CORRECTED: Evaluates condition using logical field name
    /// </summary>
    private bool MeetsCondition(
        IDictionary<string, object> record,
        string logicalFieldName,
        object chosenValue,
        OverwriteCondition condition,
        List<LogicalFieldMapping> fieldMappings)
    {
        switch (condition)
        {
            case OverwriteCondition.NoCondition:
                return true;

            case OverwriteCondition.FieldIsEmpty:
                {
                    // Get the field value using resolver
                    var dataSourceId = ExtractDataSourceId(record);
                    var fieldValue = _fieldResolver.GetFieldValue(
                        record,
                        logicalFieldName,
                        dataSourceId,
                        fieldMappings);

                    return fieldValue == null ||
                           fieldValue == DBNull.Value ||
                           string.IsNullOrWhiteSpace(fieldValue.ToString());
                }

            case OverwriteCondition.OverwriteByEmpty:
                return chosenValue != null &&
                       chosenValue != DBNull.Value &&
                       !string.IsNullOrWhiteSpace(chosenValue?.ToString());

            case OverwriteCondition.RecordIsSelected:
                return record.TryGetValue(RecordSystemFieldNames.Selected, out var selected) &&
                       selected is bool isSelected && isSelected;

            case OverwriteCondition.RecordIsNotDuplicate:
                return record.TryGetValue(RecordSystemFieldNames.NotDuplicate, out var notDup) &&
                       notDup is bool isNotDup && isNotDup;

            case OverwriteCondition.RecordIsMaster:
                return record.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var master) &&
                       master is bool isMaster && isMaster;

            default:
                return true;
        }
    }

    /// <summary>
    /// Extracts data source ID from record metadata
    /// </summary>
    private Guid ExtractDataSourceId(IDictionary<string, object> record)
    {
        if (record.TryGetValue("_metadata", out var metadataObj) &&
            metadataObj is IDictionary<string, object> metadata)
        {
            if (metadata.TryGetValue("DataSourceId", out var dsId))
            {
                if (dsId is Guid guid)
                    return guid;
                if (Guid.TryParse(dsId.ToString(), out var parsedGuid))
                    return parsedGuid;
            }
        }

        if (record.TryGetValue("DataSourceId", out var directDsId))
        {
            if (directDsId is Guid guid)
                return guid;
            if (Guid.TryParse(directDsId.ToString(), out var parsedGuid))
                return parsedGuid;
        }

        return Guid.Empty;
    }

    /// <summary>
    /// Gets physical field name for a data source using field mappings
    /// </summary>
    private string GetPhysicalFieldName(
        string logicalFieldName,
        Guid dataSourceId,
        List<LogicalFieldMapping> fieldMappings)
    {
        return _fieldResolver.GetPhysicalFieldName(logicalFieldName, dataSourceId, fieldMappings);
    }
}
