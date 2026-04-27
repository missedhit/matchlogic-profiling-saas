using MatchLogic.Application.Common;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataProfiling;
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
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Main orchestrator for master record determination with pipeline pattern
/// </summary>
public class MasterRecordDeterminationService : IMasterRecordDeterminationService
{
    private readonly ILogger<MasterRecordDeterminationService> _logger;
    private readonly MasterRecordDeterminationConfig _config;
    private readonly RuleExecutorFactory _executorFactory;
    private readonly ILogicalFieldResolver _fieldResolver;
    private readonly IDataSourceIndexMapper _dataSourceMapper;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedFieldRowsRepository;

    private long _totalGroupsProcessed;
    private long _totalMasterChanges;

    public MasterRecordDeterminationService(
        ILogger<MasterRecordDeterminationService> logger,
        IOptions<MasterRecordDeterminationConfig> config,
        RuleExecutorFactory executorFactory,
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
        _totalMasterChanges = 0;
    }

    public async IAsyncEnumerable<MatchGroup> DetermineAsync(
        IAsyncEnumerable<MatchGroup> groups,
        MasterRecordRuleSet ruleSet,
        Guid projectId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<LogicalFieldMapping> fieldMappings;
        if (groups == null)
            throw new ArgumentNullException(nameof(groups));

        if (ruleSet == null)
            throw new ArgumentNullException(nameof(ruleSet));

        await _dataSourceMapper.InitializeAsync(projectId);

        var mappedFields = await _mappedFieldRowsRepository
            .QueryAsync(mf => mf.ProjectId == projectId, Constants.Collections.MappedFieldRows);

        var dataSources = await _dataSourceRepository.QueryAsync(d => d.ProjectId == projectId, Constants.Collections.DataSources);

        fieldMappings = await _fieldResolver.ResolveLogicalFieldsAsync(
            mappedFields?.First().MappedFields,
            dataSources.ToList());

        if (fieldMappings == null || !fieldMappings.Any())
        {
            _logger.LogWarning("No field mappings provided, master determination may not work correctly");
        }

        _logger.LogInformation(
            "Starting master record determination with {RuleCount} rules",
            ruleSet.Rules?.Count ?? 0);

        // Validate rule set
        var validation = await ValidateRuleSetAsync(ruleSet, fieldMappings);
        if (!validation.IsValid)
        {
            _logger.LogError("Rule set validation failed: {Errors}",
                string.Join("; ", validation.Errors));
            throw new InvalidOperationException(
                $"Invalid rule set: {string.Join("; ", validation.Errors)}");
        }

        // Reset counters
        _totalGroupsProcessed = 0;
        _totalMasterChanges = 0;

        // Create channels
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

        // Start producer task
        var producerTask = Task.Run(async () =>
        {
            await ProduceBatchesAsync(groups, inputChannel.Writer, cancellationToken);
        }, cancellationToken);

        // Start consumer tasks
        var consumerTasks = new List<Task>();
        for (int i = 0; i < _config.MaxConcurrentBatches; i++)
        {
            var consumerId = i;
            var task = Task.Run(async () =>
            {
                await ProcessBatchesAsync(
                    inputChannel.Reader,
                    outputChannel.Writer,
                    ruleSet,
                    fieldMappings,
                    consumerId,
                    cancellationToken);
            }, cancellationToken);

            consumerTasks.Add(task);
        }

        // Start completion task (closes output channel when all consumers done)
        var completionTask = Task.Run(async () =>
        {
            await Task.WhenAll(consumerTasks);
            outputChannel.Writer.Complete();
        }, cancellationToken);

        // Stream results back to caller
        await foreach (var group in outputChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return group;
        }

        // Wait for all tasks to complete
        await producerTask;
        await completionTask;

        _logger.LogInformation(
            "Master record determination completed. Groups: {Groups}, Changes: {Changes}",
            _totalGroupsProcessed, _totalMasterChanges);
    }

    public async Task<MatchGroup> DetermineForSingleGroupAsync(
        MatchGroup group,
        MasterRecordRuleSet ruleSet,
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
        MasterRecordRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings)
    {
        var result = new ValidationResult();

        if (ruleSet == null)
        {
            result.IsValid = false;
            result.Errors.Add("Rule set is null");
            return Task.FromResult(result);
        }

        // Validate the rule set itself
        if (!ruleSet.IsValid(out var errors))
        {
            result.IsValid = false;
            result.Errors.AddRange(errors);
        }

        // Validate that logical field names in rules exist in field mappings
        if (fieldMappings != null && fieldMappings.Any())
        {
            var mappedFieldNames = new HashSet<string>(
                fieldMappings.Select(fm => fm.LogicalFieldName),
                StringComparer.OrdinalIgnoreCase);

            foreach (var rule in ruleSet.Rules ?? new List<MasterRecordRule>())
            {
                if (!mappedFieldNames.Contains(rule.LogicalFieldName))
                {
                    result.Warnings.Add(
                        $"Rule {rule.Order} references field '{rule.LogicalFieldName}' " +
                        $"which is not in the field mappings");
                }
            }
        }

        result.IsValid = !result.Errors.Any();
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

            // Write final batch
            if (batch.Any())
            {
                await writer.WriteAsync(batch, cancellationToken);
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in producer task");
            writer.Complete(ex);
            throw;
        }
    }

    private async Task ProcessBatchesAsync(
        ChannelReader<List<MatchGroup>> reader,
        ChannelWriter<MatchGroup> writer,
        MasterRecordRuleSet ruleSet,
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
                    var processedGroup = await ProcessGroupAsync(
                        group, ruleSet, fieldMappings, cancellationToken);

                    await writer.WriteAsync(processedGroup, cancellationToken);

                    Interlocked.Increment(ref _totalGroupsProcessed);

                    // Report progress periodically
                    if (_totalGroupsProcessed % 100 == 0)
                    {
                        _logger.LogDebug(
                            "Consumer {ConsumerId}: Processed {Total} groups",
                            consumerId, _totalGroupsProcessed);
                    }
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
        MasterRecordRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Initialize candidates from group records
            var candidates = InitializeCandidates(group);

            if (candidates.Count <= 1)
            {
                // Single record or empty group, no need to determine master
                _logger.LogDebug("Group {GroupId}: Single/empty group, skipping", group.GroupId);
                return group;
            }

            var appliedRules = new List<RuleApplicationLog>();
            var activeRules = ruleSet.GetActiveRulesSorted();

            // Apply rules sequentially
            foreach (var rule in activeRules)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var beforeCount = candidates.Count;

                // Find the field mapping for this rule
                var fieldMapping = fieldMappings?.FirstOrDefault(fm =>
                    fm.LogicalFieldName.Equals(rule.LogicalFieldName, StringComparison.OrdinalIgnoreCase));

                if (fieldMapping == null)
                {
                    _logger.LogWarning(
                        "Group {GroupId}, Rule {Order}: Field mapping not found for '{Field}'",
                        group.GroupId, rule.Order, rule.LogicalFieldName);
                    continue;
                }

                // Execute the rule
                var executor = _executorFactory.CreateExecutor(rule.Operation);
                var result = await executor.ExecuteAsync(candidates, rule, fieldMapping, _config);

                // Update candidates
                candidates = result.SurvivingCandidates;

                // Log the rule application
                var log = new RuleApplicationLog
                {
                    RuleId = rule.Id,
                    Order = rule.Order,
                    LogicalFieldName = rule.LogicalFieldName,
                    Operation = rule.Operation.ToString(),
                    CandidatesBeforeRule = beforeCount,
                    CandidatesAfterRule = candidates.Count,
                    RuleDecision = result.Decision,
                    ExecutionTimeMs = result.ExecutionTimeMs
                };

                appliedRules.Add(log);

                if (_config.EnableDetailedLogging)
                {
                    _logger.LogDebug(
                        "Group {GroupId}, Rule {Order}: {Before} -> {After} candidates",
                        group.GroupId, rule.Order, beforeCount, candidates.Count);
                }

                // Early exit if only one candidate remains
                if (candidates.Count == 1)
                {
                    _logger.LogDebug(
                        "Group {GroupId}: Single candidate after rule {Order}, stopping early",
                        group.GroupId, rule.Order);
                    break;
                }
            }

            // Apply tiebreaker if needed
            bool tiebreakerApplied = false;
            if (candidates.Count > 1)
            {
                candidates = ApplyTiebreaker(candidates);
                tiebreakerApplied = true;

                _logger.LogDebug(
                    "Group {GroupId}: Tiebreaker applied, selected {SelectedKey}",
                    group.GroupId, candidates[0].RecordKey);
            }

            // Mark the master record
            var masterCandidate = candidates.Single();
            var changedCount = MarkMasterRecord(group, masterCandidate);

            if (changedCount > 0)
            {
                Interlocked.Add(ref _totalMasterChanges, changedCount);
            }

            // Add audit metadata
            if (_config.EnableAuditTrail)
            {
                AddAuditMetadata(group, masterCandidate, appliedRules, tiebreakerApplied);
            }

            stopwatch.Stop();

            _logger.LogDebug(
                "Group {GroupId}: Master determined in {ElapsedMs}ms, {Changed} records changed",
                group.GroupId, stopwatch.ElapsedMilliseconds, changedCount);

            return group;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing group {GroupId}",
                group.GroupId);
            throw;
        }
    }

    private List<RecordCandidate> InitializeCandidates(MatchGroup group)
    {
        var candidates = new List<RecordCandidate>();

        foreach (var record in group.Records ?? new List<IDictionary<string, object>>())
        {
            var recordKey = ExtractRecordKey(record);
            var wasMaster = record.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var masterVal)
                && masterVal is bool boolVal && boolVal;

            var candidate = new RecordCandidate
            {
                RecordKey = recordKey,
                RecordData = record,
                DataSourceId = recordKey.DataSourceId,
                DataSourceIndex = _dataSourceMapper.GetDataSourceIndex(recordKey.DataSourceId),
                RowNumber = recordKey.RowNumber,
                WasPreviouslyMaster = wasMaster
            };

            candidates.Add(candidate);
        }

        return candidates;
    }

    private List<RecordCandidate> ApplyTiebreaker(List<RecordCandidate> candidates)
    {
        // Tiebreaker: First by DataSourceIndex, then by RowNumber
        var selected = candidates
            .OrderBy(c => c.DataSourceIndex)
            .ThenBy(c => c.RowNumber)
            .First();

        return new List<RecordCandidate> { selected };
    }

    private int MarkMasterRecord(MatchGroup group, RecordCandidate masterCandidate)
    {
        int changedCount = 0;

        foreach (var record in group.Records)
        {
            var recordKey = ExtractRecordKey(record);

            // Capture current state before changes
            bool wasMaster = record.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var masterVal)
                && masterVal is bool boolVal && boolVal;

            // Determine new state
            bool shouldBeMaster = recordKey.Equals(masterCandidate.RecordKey);

            // Update IsMasterRecord
            record[RecordSystemFieldNames.IsMasterRecord] = shouldBeMaster;

            // Update DefaultChanged: true if value changed, false if unchanged
            bool valueChanged = (wasMaster != shouldBeMaster);
            record[RecordSystemFieldNames.IsMasterRecord_DefaultChanged] = valueChanged;

            if (valueChanged)
            {
                changedCount++;
            }

            // Ensure other system fields exist
            if (!record.ContainsKey(RecordSystemFieldNames.Selected))
                record[RecordSystemFieldNames.Selected] = false;
            if (!record.ContainsKey(RecordSystemFieldNames.Selected_DefaultChanged))
                record[RecordSystemFieldNames.Selected_DefaultChanged] = false;
            if (!record.ContainsKey(RecordSystemFieldNames.NotDuplicate))
                record[RecordSystemFieldNames.NotDuplicate] = false;
            if (!record.ContainsKey(RecordSystemFieldNames.NotDuplicate_DefaultChanged))
                record[RecordSystemFieldNames.NotDuplicate_DefaultChanged] = false;
        }

        return changedCount;
    }

    private void AddAuditMetadata(
        MatchGroup group,
        RecordCandidate masterCandidate,
        List<RuleApplicationLog> appliedRules,
        bool tiebreakerApplied)
    {
        if (group.Metadata == null)
            group.Metadata = new Dictionary<string, object>();

        group.Metadata["master_determined_at"] = DateTime.UtcNow;
        group.Metadata["master_record_key"] = masterCandidate.RecordKey.ToString();
        group.Metadata["tiebreaker_applied"] = tiebreakerApplied;
        group.Metadata["rules_applied_count"] = appliedRules.Count;

        if (appliedRules.Any())
        {
            group.Metadata["applied_rules"] = appliedRules.Select(r => new
            {
                r.Order,
                r.LogicalFieldName,
                r.Operation,
                r.CandidatesAfterRule,
                r.RuleDecision
            }).ToList();
        }
    }

    private RecordKey ExtractRecordKey(IDictionary<string, object> record)
    {
        // Try to extract from metadata
        if (record.TryGetValue("_metadata", out var metadataObj) &&
            metadataObj is IDictionary<string, object> metadata)
        {
            var dataSourceId = metadata.TryGetValue("DataSourceId", out var dsId)
                ? (Guid)dsId
                : Guid.Empty;

            var rowNumber = metadata.TryGetValue("RowNumber", out var rowNum)
                ? Convert.ToInt32(rowNum)
                : 0;

            return new RecordKey(dataSourceId, rowNumber);
        }

        // Fallback: try direct fields
        var fallbackDsId = record.TryGetValue("DataSourceId", out var fbDsId)
            ? (Guid)fbDsId
            : Guid.Empty;

        var fallbackRowNum = record.TryGetValue("RowNumber", out var fbRowNum)
            ? Convert.ToInt32(fbRowNum)
            : 0;

        return new RecordKey(fallbackDsId, fallbackRowNum);
    }
}
