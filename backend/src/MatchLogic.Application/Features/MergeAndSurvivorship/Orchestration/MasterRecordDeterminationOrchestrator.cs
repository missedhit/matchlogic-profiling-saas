using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Orchestrator for master record determination that reads groups from database,
/// determines masters, and writes back to new collection
/// </summary>
public class MasterRecordDeterminationOrchestrator : IMasterRecordDeterminationOrchestrator, IAsyncDisposable
{
    private readonly IDataStore _dataStore;
    private readonly IMasterRecordDeterminationService _masterService;
    private readonly IMasterRecordRuleSetRepository _ruleSetRepository;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly ILogger<MasterRecordDeterminationOrchestrator> _logger;
    private readonly ITelemetry _telemetry;
    private readonly MasterRecordDeterminationConfig _config;
    private readonly SemaphoreSlim _writeSemaphore;
    private bool _disposed;

    public MasterRecordDeterminationOrchestrator(
        IDataStore dataStore,
        IMasterRecordDeterminationService masterService,
        IMasterRecordRuleSetRepository ruleSetRepository,
        IJobEventPublisher jobEventPublisher,
        ILogger<MasterRecordDeterminationOrchestrator> logger,
        ITelemetry telemetry,
        IOptions<MasterRecordDeterminationConfig> config)
    {
        _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
        _masterService = masterService ?? throw new ArgumentNullException(nameof(masterService));
        _ruleSetRepository = ruleSetRepository ?? throw new ArgumentNullException(nameof(ruleSetRepository));
        _jobEventPublisher = jobEventPublisher ?? throw new ArgumentNullException(nameof(jobEventPublisher));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _config = config?.Value ?? throw new ArgumentNullException(nameof(config));
        _writeSemaphore = new SemaphoreSlim(4, 4);
    }

    /// <summary>
    /// Executes master record determination for a project
    /// Reads groups from database, determines masters, writes to new collection
    /// </summary>
    public async Task<MasterDeterminationResult> ExecuteMasterDeterminationAsync(
        Guid projectId,
        MasterDeterminationOptions options = null,
        ICommandContext commandContext = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        options ??= MasterDeterminationOptions.Default();

        using var operation = _telemetry.MeasureOperation("execute_master_determination");

        var result = new MasterDeterminationResult
        {
            ProjectId = projectId,
            StartTime = DateTime.UtcNow
        };

        try
        {
            _logger.LogInformation(
                "Starting master record determination for project {ProjectId}", projectId);

            var stepId = commandContext?.StepId ?? Guid.Empty;

            // Step 1: Load rule set
            var ruleSet = await LoadRuleSetAsync(projectId, cancellationToken);

            // Step 2: Get collection names
            var collectionNames = GetCollectionNames(projectId);

            // Step 3: Delete master collection if it exists (fresh start)
            await _dataStore.DeleteCollection(collectionNames.MasterGroupsCollection);
            await _dataStore.DeleteCollection(collectionNames.OverwriteValuesCollection);

            // Step 4: Setup progress tracking
            var loadingStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Loading Groups", 1, 3);
            var processingStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Determining Masters", 2, 3);
            var persistenceStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Saving Results", 3, 3);

            // Step 5: Load groups from database
            await loadingStep.StartStepAsync(0, cancellationToken);

            var groupsChannel = Channel.CreateBounded<MatchGroup>(_config.ChannelCapacity);
            var loadingTask = LoadGroupsFromDatabaseAsync(
                collectionNames.GroupsCollection,
                groupsChannel.Writer,
                loadingStep,
                cancellationToken);

            // Step 6: Process groups through master determination
            await processingStep.StartStepAsync(0, cancellationToken);

            var processedChannel = Channel.CreateBounded<MatchGroup>(_config.ChannelCapacity * 2);
            var processingTask = ProcessGroupsPipelineAsync(
                groupsChannel.Reader,
                processedChannel.Writer,
                ruleSet,
                projectId,
                processingStep,
                cancellationToken);

            // Step 7: Write processed groups to new collection
            await persistenceStep.StartStepAsync(0, cancellationToken);

            var writingTask = WriteGroupsAsync(
                processedChannel.Reader,
                collectionNames.MasterGroupsCollection,
                persistenceStep,
                cancellationToken);

            // Wait for all tasks to complete
            await loadingTask;  // This one doesn't return a value
            var processedCount = await processingTask;
            var writtenCount = await writingTask;

            await loadingStep.CompleteStepAsync("Groups loaded");
            await processingStep.CompleteStepAsync("Masters determined");
            await persistenceStep.CompleteStepAsync("Results saved");

            result.EndTime = DateTime.UtcNow;
            result.Success = true;
            result.OutputCollections = collectionNames;
            result.TotalGroupsProcessed = processedCount;
            result.TotalMasterChanges = writtenCount;

            await _dataStore.DeleteCollection(collectionNames.GroupsCollection);
            await _dataStore.RenameCollection(collectionNames.MasterGroupsCollection, collectionNames.GroupsCollection);

            _logger.LogInformation(
                "Completed master record determination for project {ProjectId}. " +
                "Groups processed: {GroupsProcessed}, Master changes: {Changes}",
                projectId, result.TotalGroupsProcessed, result.TotalMasterChanges);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error during master determination orchestration for project {ProjectId}",
                projectId);

            result.EndTime = DateTime.UtcNow;
            result.Success = false;
            result.ErrorMessage = ex.Message;

            throw;
        }
    }

    /// <summary>
    /// Loads rule set for the project
    /// </summary>
    private async Task<MasterRecordRuleSet> LoadRuleSetAsync(
        Guid projectId,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Loading master record rule set for project {ProjectId}", projectId);

        var ruleSet = await _ruleSetRepository.GetActiveRuleSetAsync(projectId);

                    _logger.LogInformation(
            "Loaded rule set '{Id}' with {RuleCount} rules",
            ruleSet.Id, ruleSet.Rules?.Count ?? 0);

        return ruleSet;
    }

    /// <summary>
    /// Loads groups from database into channel
    /// Producer: Reads from database
    /// </summary>
    private async Task LoadGroupsFromDatabaseAsync(
        string collectionName,
        ChannelWriter<MatchGroup> writer,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Loading groups from collection {Collection}", collectionName);

            int loadedCount = 0;
            await foreach (var groupDoc in _dataStore.StreamDataAsync(collectionName, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var group = ConvertDocumentToMatchGroup(groupDoc);

                await writer.WriteAsync(group, cancellationToken);
                loadedCount++;

                if (loadedCount % 100 == 0)
                {
                    await progressTracker.UpdateProgressAsync(100, $"Loaded {loadedCount} groups");
                }
            }

            _logger.LogInformation("Loaded {Count} groups into processing pipeline", loadedCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading groups from {Collection}", collectionName);
            throw;
        }
        finally
        {
            writer.Complete();
        }
    }

    /// <summary>
    /// Processes groups through master determination service
    /// Consumer/Producer: Reads groups, processes them, writes to output channel
    /// </summary>
    private async Task<int> ProcessGroupsPipelineAsync(
        ChannelReader<MatchGroup> reader,
        ChannelWriter<MatchGroup> writer,
        MasterRecordRuleSet ruleSet,
        Guid projectId,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Starting master determination processing pipeline");

            int processedCount = 0;

            // Stream groups through master determination service
            var groupsEnumerable = reader.ReadAllAsync(cancellationToken);
            var processedGroups = _masterService.DetermineAsync(
                groupsEnumerable,
                ruleSet,
                projectId,
                cancellationToken);

            // Forward processed groups to writer channel
            await foreach (var group in processedGroups.WithCancellation(cancellationToken))
            {
                await writer.WriteAsync(group, cancellationToken);
                processedCount++;

                if (processedCount % 100 == 0)
                {
                    await progressTracker.UpdateProgressAsync(
                        100, $"Processed {processedCount} groups");
                }
            }

            _logger.LogInformation("Processed {Count} groups through master determination",
                processedCount);
            return processedCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in master determination processing pipeline");
            throw;
        }
        finally
        {
            writer.Complete();
        }
    }

    /// <summary>
    /// Writes processed groups to database
    /// Consumer: Reads from channel, writes to database in batches
    /// </summary>
    private async Task<int> WriteGroupsAsync(
        ChannelReader<MatchGroup> reader,
        string collectionName,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        var batch = new List<IDictionary<string, object>>(_config.BatchSize);
        int totalCount = 0;

        try
        {
            await foreach (var group in reader.ReadAllAsync(cancellationToken))
            {
                var groupDict = ConvertMatchGroupToDocument(group);
                batch.Add(groupDict);
                totalCount++;

                if (batch.Count >= _config.BatchSize)
                {
                    await FlushBatchAsync(batch, collectionName);
                    await progressTracker.UpdateProgressAsync(
                        batch.Count, $"Saved {totalCount} groups");
                }
            }

            // Flush remaining items
            if (batch.Count > 0)
            {
                await FlushBatchAsync(batch, collectionName);
                await progressTracker.UpdateProgressAsync(
                    batch.Count, $"Saved {totalCount} groups");
            }

            _logger.LogInformation("Saved {Count} groups to {Collection}",
                totalCount, collectionName);
            return totalCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing groups to {Collection}", collectionName);
            throw;
        }
    }

    /// <summary>
    /// Flushes a batch of documents to database with semaphore coordination
    /// </summary>
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

    /// <summary>
    /// Converts a database document to MatchGroup
    /// </summary>
    private MatchGroup ConvertDocumentToMatchGroup(IDictionary<string, object> doc)
    {
        return MatchGroupConverter.ConvertDocumentToMatchGroup(doc);
    }

    /// <summary>
    /// Converts MatchGroup to database document
    /// </summary>
    private IDictionary<string, object> ConvertMatchGroupToDocument(MatchGroup group)
    {
        return MatchGroupConverter.ConvertMatchGroupToDocument(group);
    }    

    /// <summary>
    /// Gets collection names for the project
    /// </summary>
    private MasterDeterminationCollectionNames GetCollectionNames(Guid projectId)
    {
        var normalizedProjectId = GuidCollectionNameConverter.ToValidCollectionName(projectId);

        return new MasterDeterminationCollectionNames
        {
            GroupsCollection = $"groups_{normalizedProjectId}",
            MasterGroupsCollection = $"groups_master_{normalizedProjectId}",
            OverwriteValuesCollection = $"groups_overwritten_{normalizedProjectId}"
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        _writeSemaphore?.Dispose();

        _logger.LogInformation("MasterRecordDeterminationOrchestrator disposed");
    }
}

/// <summary>
/// Interface for master record determination orchestrator
/// </summary>
public interface IMasterRecordDeterminationOrchestrator
{
    Task<MasterDeterminationResult> ExecuteMasterDeterminationAsync(
        Guid projectId,
        MasterDeterminationOptions options = null,
        ICommandContext commandContext = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Options for master record determination
/// </summary>
public class MasterDeterminationOptions
{
    public bool CreateBackup { get; set; }
    public bool ValidateRuleSet { get; set; }

    public static MasterDeterminationOptions Default()
    {
        return new MasterDeterminationOptions
        {
            CreateBackup = false,
            ValidateRuleSet = true
        };
    }
}

/// <summary>
/// Result of master record determination
/// </summary>
public class MasterDeterminationResult
{
    public Guid ProjectId { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public bool Success { get; set; }
    public string ErrorMessage { get; set; }
    public MasterDeterminationCollectionNames OutputCollections { get; set; }
    public int TotalGroupsProcessed { get; set; }
    public int TotalMasterChanges { get; set; }
}

/// <summary>
/// Collection names for master determination
/// </summary>
public class MasterDeterminationCollectionNames
{
    public string GroupsCollection { get; set; }
    public string MasterGroupsCollection { get; set; }
    public string OverwriteValuesCollection { get; set; }
}