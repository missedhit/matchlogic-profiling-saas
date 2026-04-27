using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class FieldOverwriteOrchestrator : IFieldOverwriteOrchestrator, IAsyncDisposable
{
    private readonly IDataStore _dataStore;
    private readonly IFieldOverwriteService _fieldOverwriteService;
    private readonly IFieldOverwriteRuleSetRepository _ruleSetRepository;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly ILogger<FieldOverwriteOrchestrator> _logger;
    private readonly ITelemetry _telemetry;
    private readonly FieldOverwriteConfig _config;
    private readonly SemaphoreSlim _writeSemaphore;
    private bool _disposed;

    public FieldOverwriteOrchestrator(
        IDataStore dataStore,
        IFieldOverwriteService fieldOverwriteService,
        IFieldOverwriteRuleSetRepository ruleSetRepository,
        IJobEventPublisher jobEventPublisher,
        ILogger<FieldOverwriteOrchestrator> logger,
        ITelemetry telemetry,
        IOptions<FieldOverwriteConfig> config)
    {
        _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
        _fieldOverwriteService = fieldOverwriteService ?? throw new ArgumentNullException(nameof(fieldOverwriteService));
        _ruleSetRepository = ruleSetRepository ?? throw new ArgumentNullException(nameof(ruleSetRepository));
        _jobEventPublisher = jobEventPublisher ?? throw new ArgumentNullException(nameof(jobEventPublisher));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _config = config?.Value ?? throw new ArgumentNullException(nameof(config));
        _writeSemaphore = new SemaphoreSlim(4, 4);
    }

    public async Task<FieldOverwriteResult> ExecuteFieldOverwritingAsync(
        Guid projectId,
        FieldOverwriteOptions options = null,
        ICommandContext commandContext = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        options ??= FieldOverwriteOptions.Default();

        using var operation = _telemetry.MeasureOperation("execute_field_overwriting");

        var result = new FieldOverwriteResult
        {
            ProjectId = projectId,
            StartTime = DateTime.UtcNow
        };

        try
        {
            _logger.LogInformation("Starting field overwriting for project {ProjectId}", projectId);

            var stepId = commandContext?.StepId ?? Guid.Empty;

            var ruleSet = await LoadRuleSetAsync(projectId, cancellationToken);

            var collectionNames = GetCollectionNames(projectId);

            await _dataStore.DeleteCollection(collectionNames.OverwrittenGroupsCollection);

            var loadingStep = _jobEventPublisher.CreateStepTracker(stepId, "Loading Groups", 1, 3);
            var processingStep = _jobEventPublisher.CreateStepTracker(stepId, "Overwriting Fields", 2, 3);
            var persistenceStep = _jobEventPublisher.CreateStepTracker(stepId, "Saving Results", 3, 3);

            await loadingStep.StartStepAsync(0, cancellationToken);

            var groupsChannel = Channel.CreateBounded<MatchGroup>(_config.ChannelCapacity);
            var loadingTask = LoadGroupsFromDatabaseAsync(
                collectionNames.InputGroupsCollection,
                groupsChannel.Writer,
                loadingStep,
                cancellationToken);

            await processingStep.StartStepAsync(0, cancellationToken);

            var processedChannel = Channel.CreateBounded<MatchGroup>(_config.ChannelCapacity * 2);
            var processingTask = ProcessGroupsPipelineAsync(
                groupsChannel.Reader,
                processedChannel.Writer,
                ruleSet,
                projectId,
                processingStep,
                cancellationToken);

            await persistenceStep.StartStepAsync(0, cancellationToken);

            var writingTask = WriteGroupsAsync(
                processedChannel.Reader,
                collectionNames.OverwrittenGroupsCollection,
                persistenceStep,
                cancellationToken);

            await loadingTask;
            var processedCount = await processingTask;
            var writtenCount = await writingTask;

            await loadingStep.CompleteStepAsync("Groups loaded");
            await processingStep.CompleteStepAsync("Fields overwritten");
            await persistenceStep.CompleteStepAsync("Results saved");

            result.EndTime = DateTime.UtcNow;
            result.Success = true;
            result.OutputCollections = collectionNames;
            result.TotalGroupsProcessed = processedCount;
            result.TotalFieldsOverwritten = writtenCount;

            await _dataStore.DeleteCollection(collectionNames.InputGroupsCollection);
            await _dataStore.RenameCollection(collectionNames.OverwrittenGroupsCollection, collectionNames.InputGroupsCollection);

            _logger.LogInformation(
                "Completed field overwriting for project {ProjectId}. Groups: {Groups}, Fields: {Fields}",
                projectId, result.TotalGroupsProcessed, result.TotalFieldsOverwritten);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during field overwriting for project {ProjectId}", projectId);

            result.EndTime = DateTime.UtcNow;
            result.Success = false;
            result.ErrorMessage = ex.Message;

            throw;
        }
    }

    private async Task<FieldOverwriteRuleSet> LoadRuleSetAsync(
        Guid projectId,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Loading field overwrite rule set for project {ProjectId}", projectId);

        var ruleSet = await _ruleSetRepository.GetActiveRuleSetAsync(projectId);

        _logger.LogInformation("Loaded rule set '{Id}' with {RuleCount} rules",
            ruleSet.Id, ruleSet.Rules?.Count ?? 0);

        return ruleSet;
    }

    private async Task LoadGroupsFromDatabaseAsync(
        string collectionName,
        ChannelWriter<MatchGroup> writer,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Loading groups from collection {Collection}", collectionName);

            var totalCount = 0;

            await foreach (var doc in _dataStore.StreamDataAsync(collectionName, cancellationToken))
            {
                var group = ConvertDocumentToMatchGroup(doc);
                await writer.WriteAsync(group, cancellationToken);
                totalCount++;

                if (totalCount % 100 == 0)
                {
                    await progressTracker.UpdateProgressAsync(100, $"Loaded {totalCount} groups");
                }
            }

            writer.Complete();

            _logger.LogInformation("Loaded {Count} groups from {Collection}", totalCount, collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading groups from {Collection}", collectionName);
            writer.Complete(ex);
            throw;
        }
    }

    private async Task<int> ProcessGroupsPipelineAsync(
        ChannelReader<MatchGroup> reader,
        ChannelWriter<MatchGroup> writer,
        FieldOverwriteRuleSet ruleSet,
        Guid projectId,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            var processedCount = 0;

            var processedGroups = _fieldOverwriteService.OverwriteAsync(
                reader.ReadAllAsync(cancellationToken),
                ruleSet,
                projectId,
                cancellationToken);

            await foreach (var group in processedGroups.WithCancellation(cancellationToken))
            {
                await writer.WriteAsync(group, cancellationToken);
                processedCount++;

                if (processedCount % 100 == 0)
                {
                    await progressTracker.UpdateProgressAsync(100, $"Processed {processedCount} groups");
                }
            }

            writer.Complete();

            _logger.LogInformation("Processed {Count} groups", processedCount);
            return processedCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing groups pipeline");
            writer.Complete(ex);
            throw;
        }
    }

    private async Task<int> WriteGroupsAsync(
        ChannelReader<MatchGroup> reader,
        string collectionName,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            var totalCount = 0;
            var batch = new List<IDictionary<string, object>>();

            await foreach (var group in reader.ReadAllAsync(cancellationToken))
            {
                var doc = ConvertMatchGroupToDocument(group);
                batch.Add(doc);
                totalCount++;

                if (batch.Count >= _config.BatchSize)
                {
                    await FlushBatchAsync(batch, collectionName);
                    await progressTracker.UpdateProgressAsync(batch.Count, $"Saved {totalCount} groups");
                }
            }

            if (batch.Count > 0)
            {
                await FlushBatchAsync(batch, collectionName);
                await progressTracker.UpdateProgressAsync(batch.Count, $"Saved {totalCount} groups");
            }

            _logger.LogInformation("Saved {Count} groups to {Collection}", totalCount, collectionName);
            return totalCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing groups to {Collection}", collectionName);
            throw;
        }
    }

    private async Task FlushBatchAsync(
        List<IDictionary<string, object>> batch,
        string collectionName)
    {
        await _writeSemaphore.WaitAsync();
        try
        {
            await _dataStore.InsertBatchAsync(collectionName, batch);
            batch.Clear();
        }
        finally
        {
            _writeSemaphore.Release();
        }
    }

    private MatchGroup ConvertDocumentToMatchGroup(IDictionary<string, object> doc)
    {
        var group = new MatchGroup
        {
            GroupId = doc.TryGetValue("GroupId", out var groupId)
                ? Convert.ToInt32(groupId)
                : 0,

            GroupHash = doc.TryGetValue("GroupHash", out var hash)
                ? hash?.ToString()
                : string.Empty,

            Records = doc.TryGetValue("Records", out var records)
                ? ConvertToRecordsList(records)
                : new List<IDictionary<string, object>>(),

            Metadata = doc.TryGetValue("Metadata", out var metadata)
                ? ConvertToDictionary(metadata)
                : new Dictionary<string, object>()
        };

        return group;
    }

    private IDictionary<string, object> ConvertMatchGroupToDocument(MatchGroup group)
    {
        return new Dictionary<string, object>
        {
            ["GroupId"] = group.GroupId,
            ["GroupHash"] = group.GroupHash,
            ["Records"] = group.Records,
            ["Metadata"] = group.Metadata ?? new Dictionary<string, object>(),
            ["ProcessedAt"] = DateTime.UtcNow
        };
    }

    private List<IDictionary<string, object>> ConvertToRecordsList(object records)
    {
        if (records is List<IDictionary<string, object>> recordsList)
            return recordsList;

        if (records is IEnumerable<object> enumerable)
        {
            return enumerable
                .Select(r => r as IDictionary<string, object> ?? new Dictionary<string, object>())
                .ToList();
        }

        return new List<IDictionary<string, object>>();
    }

    private Dictionary<string, object> ConvertToDictionary(object metadata)
    {
        if (metadata is Dictionary<string, object> dict)
            return dict;

        if (metadata is IDictionary<string, object> idict)
            return new Dictionary<string, object>(idict);

        return new Dictionary<string, object>();
    }

    private FieldOverwriteCollectionNames GetCollectionNames(Guid projectId)
    {
        var normalizedProjectId = GuidCollectionNameConverter.ToValidCollectionName(projectId);
        string masterCollection = $"groups_master_{normalizedProjectId}";
        string groupsCollection = $"groups_{normalizedProjectId}";
        bool masterExists = _dataStore.GetPagedDataAsync(masterCollection, 1, 1).Result.Data.Any();
        return new FieldOverwriteCollectionNames
        {
            InputGroupsCollection = masterExists ? masterCollection : groupsCollection,
            OverwrittenGroupsCollection = $"groups_overwritten_{normalizedProjectId}"
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        _writeSemaphore?.Dispose();

        _logger.LogInformation("FieldOverwriteOrchestrator disposed");
    }
}

public interface IFieldOverwriteOrchestrator
{
    Task<FieldOverwriteResult> ExecuteFieldOverwritingAsync(
        Guid projectId,
        FieldOverwriteOptions options = null,
        ICommandContext commandContext = null,
        CancellationToken cancellationToken = default);
}
