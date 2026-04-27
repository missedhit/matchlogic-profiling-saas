using Mapster;
using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.Grouping;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MatchLogic.Domain.Import;

namespace MatchLogic.Application.Features.DataMatching.Orchestration;

public class RecordLinkageOrchestrator : IRecordLinkageOrchestrator, IAsyncDisposable
{
    private readonly IDataStore _dataStore;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<MatchDefinitionCollection, Guid> _matchDefinitionRepository;
    private readonly IMatchConfigurationService _matchConfigurationService;
    private readonly IGenericRepository<MatchSettings, Guid> _matchSettings;
    private readonly IDataSourceIndexMapper _indexMapper;
    private readonly IProductionQGramIndexer _qgramIndexer;
    private readonly IEnhancedRecordComparisonService _comparisonService;
    private readonly IEnhancedGroupingService _groupingService;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly ILogger<RecordLinkageOrchestrator> _logger;
    private readonly ITelemetry _telemetry;
    private readonly RecordLinkageOptions _options;
    private readonly SemaphoreSlim _writeSemaphore;
    private readonly IHeaderUtility _headerUtility;
    private bool _disposed;

    public RecordLinkageOrchestrator(
        IDataStore dataStore,
        IGenericRepository<DataSource, Guid> dataSourceRepository,
        IGenericRepository<MatchDefinitionCollection, Guid> matchDefinitionRepository,
        IGenericRepository<MatchSettings, Guid> matchSettings,
        IMatchConfigurationService matchConfigurationService,
        IDataSourceIndexMapper indexMapper,
        IProductionQGramIndexer qgramIndexer,
        IEnhancedRecordComparisonService comparisonService,
        IEnhancedGroupingService groupingService,
        IJobEventPublisher jobEventPublisher,
        ILogger<RecordLinkageOrchestrator> logger,
        ITelemetry telemetry,
        IOptions<RecordLinkageOptions> options,
        IHeaderUtility headerUtility)
    {
        _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
        _dataSourceRepository = dataSourceRepository ?? throw new ArgumentNullException(nameof(dataSourceRepository));
        _matchDefinitionRepository = matchDefinitionRepository ?? throw new ArgumentNullException(nameof(matchDefinitionRepository));
        _matchSettings = matchSettings ?? throw new ArgumentNullException(nameof(matchSettings));
        _matchConfigurationService = matchConfigurationService ?? throw new ArgumentNullException(nameof(matchConfigurationService));
        _indexMapper = indexMapper ?? throw new ArgumentNullException(nameof(indexMapper));
        _qgramIndexer = qgramIndexer ?? throw new ArgumentNullException(nameof(qgramIndexer));
        _comparisonService = comparisonService ?? throw new ArgumentNullException(nameof(comparisonService));
        _groupingService = groupingService ?? throw new ArgumentNullException(nameof(groupingService));
        _jobEventPublisher = jobEventPublisher ?? throw new ArgumentNullException(nameof(jobEventPublisher));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _writeSemaphore = new SemaphoreSlim(4, 4);
        _headerUtility = headerUtility ?? throw new ArgumentNullException(nameof(_headerUtility));
    }

    public async Task<OrchestrationResult> ExecuteRecordLinkageAsync(
        Guid projectId,
        OrchestrationOptions orchestrationOptions = null,
        ICommandContext commandContext = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        orchestrationOptions ??= OrchestrationOptions.Default();

        using var operation = _telemetry.MeasureOperation("execute_record_linkage");

        var result = new OrchestrationResult
        {
            ProjectId = projectId,
            StartTime = DateTime.UtcNow
        };

        try
        {
            _logger.LogInformation("Starting record linkage orchestration for project {ProjectId}", projectId);

            var stepId = commandContext == null ? Guid.Empty : commandContext.StepId;
            // Step 1: Load configuration
            var configuration = await LoadConfigurationAsync(projectId, cancellationToken);

            orchestrationOptions.RequireTransitiveGroups = !configuration.MatchSettings.MergeOverlappingGroups;

            // Step 2: Initialize the index mapper
            await _indexMapper.InitializeAsync(projectId);

            var collectionNames = GetCollectionNames(projectId);

            await _dataStore.DeleteCollection(collectionNames.GraphCollection);
            await _dataStore.DeleteCollection(collectionNames.GroupsCollection);
            await _dataStore.DeleteCollection(collectionNames.PairsCollection);

            // Step 3: Initialize progress tracking
            //await _jobEventPublisher.PublishJobStartedAsync(projectId, 5, $"Starting record linkage for project {projectId}");

            var indexingStep = _jobEventPublisher.CreateStepTracker(stepId, "Indexing Data Sources", 1, 5);
            var candidateStep = _jobEventPublisher.CreateStepTracker(stepId, "Generating Candidates", 2, 5);
            var comparisonStep = _jobEventPublisher.CreateStepTracker(stepId, "Comparing Records", 3, 5);
            var groupingStep = _jobEventPublisher.CreateStepTracker(stepId, "Creating Groups", 4, 5);
            var persistenceStep = _jobEventPublisher.CreateStepTracker(stepId, "Saving Results", 5, 5);

            // Step 4: Index all data sources
            if (_qgramIndexer is ProductionQGramIndexerWithBlocking)
            {
                ((ProductionQGramIndexerWithBlocking)_qgramIndexer).InitializeBlockingConfiguration(configuration.MatchDefinitions);
            }
            await IndexDataSourcesAsync(configuration, indexingStep, cancellationToken);

            // Step 5: Generate candidates
            await candidateStep.StartStepAsync(0, cancellationToken);
            var candidates = _qgramIndexer.GenerateCandidatesFromMatchDefinitionsAsync(
                configuration.MatchDefinitions,
                cancellationToken);

            // Step 6: Setup channels for data flow
            var pairsChannel = Channel.CreateBounded<ScoredMatchPair>(_options.BufferSize);
            var graphChannel = new BroadcastChannel<MatchGraph>(1, 2); // Only one graph
            var groupsChannel = Channel.CreateBounded<MatchGroup>(_options.BufferSize);

            var broadcastTask = graphChannel.StartBroadcastAsync(cancellationToken);
            // Step 7: Start comparison and graph building
            await comparisonStep.StartStepAsync(0, cancellationToken);
            var comparisonTask = RunComparisonPipelineAsync(
                candidates,
                configuration,
                pairsChannel.Writer,
                graphChannel.Writer,
                comparisonStep,
                cancellationToken);


            var pairsWriterTask = WritePairsAsync(
                pairsChannel.Reader,
                collectionNames.PairsCollection,
                persistenceStep,
                cancellationToken);

            await Task.WhenAll(pairsWriterTask, comparisonTask);

            await candidateStep.CompleteStepAsync("Candidate generation completed");
            await comparisonStep.CompleteStepAsync("Comparison completed");

            // Step 8: Start grouping
            await groupingStep.StartStepAsync(0, cancellationToken);
            var groupingTask = RunGroupingPipelineAsync(
                graphChannel.Readers[0],
                groupsChannel.Writer,
                orchestrationOptions.RequireTransitiveGroups,
                groupingStep,
                cancellationToken);

            // Step 9: Start persistence tasks

            await persistenceStep.StartStepAsync(0, cancellationToken);

            var graphWriterTask = WriteGraphAsync(
                graphChannel.Readers[1],
                collectionNames.GraphCollection,
                cancellationToken);

            var groupsWriterTask = WriteGroupsAsync(
                groupsChannel.Reader,
                collectionNames.GroupsCollection,
                persistenceStep,
                cancellationToken);

            // Step 10: Wait for all tasks to complete
            await Task.WhenAll(
                broadcastTask,
                groupingTask,
                pairsWriterTask,
                graphWriterTask,
                groupsWriterTask);

            // Step 11: Complete progress tracking               
            await groupingStep.CompleteStepAsync("Grouping completed");
            await persistenceStep.CompleteStepAsync("Results saved");
            //await _jobEventPublisher.PublishJobCompletedAsync(projectId);

            result.EndTime = DateTime.UtcNow;
            result.Success = true;
            result.OutputCollections = collectionNames;

            _logger.LogInformation("Completed record linkage orchestration for project {ProjectId}", projectId);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during record linkage orchestration for project {ProjectId}", projectId);
            //await _jobEventPublisher.PublishJobFailedAsync(projectId, ex.Message);

            result.EndTime = DateTime.UtcNow;
            result.Success = false;
            result.ErrorMessage = ex.Message;

            throw;
        }
    }

    private async Task<OrchestrationConfiguration> LoadConfigurationAsync(
        Guid projectId,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Loading configuration for project {ProjectId}", projectId);

        // Load data sources
        var dataSources = await _dataSourceRepository.QueryAsync(
            ds => ds.ProjectId == projectId,
            Constants.Collections.DataSources);

        if (!dataSources.Any())
        {
            throw new InvalidOperationException($"No data sources found for project {projectId}");
        }

        // Load match definitions
        var matchDefinitionCollections = await _matchDefinitionRepository.QueryAsync(
            mdc => mdc.ProjectId == projectId,
            Constants.Collections.MatchDefinitionCollection);

        var matchSettingList = await _matchSettings.QueryAsync(
           mS => mS.ProjectId == projectId,
           Constants.Collections.MatchSettings);

        var matchSetting = matchSettingList.FirstOrDefault();
        if (matchSetting == null)
        {
            throw new InvalidOperationException($"No match setting found for project {projectId}");
        }

        var matchDefinitions = matchDefinitionCollections.FirstOrDefault();
        if (matchDefinitions == null || !matchDefinitions.Definitions.Any())
        {
            throw new InvalidOperationException($"No match definitions found for project {projectId}");
        }

        // Load match configuration
        var matchConfig = await _matchConfigurationService.GetDataSourcePairsByProjectIdAsync(projectId);

        return new OrchestrationConfiguration
        {
            ProjectId = projectId,
            DataSources = dataSources.ToList(),
            MatchDefinitions = matchDefinitions,
            MatchConfiguration = matchConfig,
            MatchSettings = matchSetting,
        };
    }

    private async Task IndexDataSourcesAsync(
        OrchestrationConfiguration configuration,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Indexing {Count} data sources", configuration.DataSources.Count);

        await progressTracker.StartStepAsync(configuration.DataSources.Count, cancellationToken);

        foreach (var dataSource in configuration.DataSources)
        {
            var collectionName = GetDataSourceCollectionName(dataSource);
            _logger.LogInformation("Indexing data source {Name} from collection {Collection}",
                dataSource.Name, collectionName);

            // Get fields to index from match definitions
            var fieldsToIndex = GetFieldsToIndex(dataSource.Id, configuration.MatchDefinitions);

            var indexingConfig = new DataSourceIndexingConfig
            {
                DataSourceId = dataSource.Id,
                DataSourceName = dataSource.Name,
                FieldsToIndex = fieldsToIndex,
                UseInMemoryStore = true,//dataSource.RecordCount < 100000, // Use memory for smaller datasets
                InMemoryThreshold = 100000
            };

            // Stream data from collection and index
            var dataStream = _dataStore.StreamDataAsync(collectionName, cancellationToken);

            var indexResult = await _qgramIndexer.IndexDataSourceAsync(
                dataStream,
                indexingConfig,
                progressTracker,
                cancellationToken);

            _logger.LogInformation("Indexed {Count} records from data source {Name}",
                indexResult.ProcessedRecords, dataSource.Name);

            await progressTracker.UpdateProgressAsync(1, $"Indexed {dataSource.Name}");
        }

        await progressTracker.CompleteStepAsync("All data sources indexed");
    }

    private List<string> GetFieldsToIndex(Guid dataSourceId, MatchDefinitionCollection matchDefinitions)
    {
        var fields = new HashSet<string>();

        foreach (var definition in matchDefinitions.Definitions)
        {
            foreach (var criteria in definition.Criteria)
            {
                var mappings = criteria.FieldMappings
                    .Where(fm => fm.DataSourceId == dataSourceId)
                    .Select(fm => fm.FieldName);

                foreach (var field in mappings)
                {
                    fields.Add(field);
                }
            }
        }

        return fields.ToList();
    }

    /*private async Task RunComparisonPipelineAsync(
        IAsyncEnumerable<CandidatePair> candidates,
        OrchestrationConfiguration configuration,
        ChannelWriter<ScoredMatchPair> pairsWriter,
        ChannelWriter<MatchGraph> graphWriter,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            // Compare candidates and build graph
            var (scoredPairs, matchGraph) = await _comparisonService.CompareWithGraphAsync(
                candidates,
                configuration.MatchDefinitions,
                _indexMapper,
                cancellationToken);

            // Write graph
            await graphWriter.WriteAsync(matchGraph, cancellationToken);
            graphWriter.Complete();

            // Stream scored pairs to channel
            int pairCount = 0;
            await foreach (var pair in scoredPairs.WithCancellation(cancellationToken))
            {
                await pairsWriter.WriteAsync(pair, cancellationToken);
                pairCount++;

                if (pairCount % 1000 == 0)
                {
                    await progressTracker.UpdateProgressAsync(1000, $"Processed {pairCount} pairs");
                }
            }

            _logger.LogInformation("Comparison completed. Processed {Count} pairs", pairCount);
        }
        finally
        {
            pairsWriter.Complete();
        }
    }*/

    private async Task RunComparisonPipelineAsync(
        IAsyncEnumerable<CandidatePair> candidates,
        OrchestrationConfiguration configuration,
        ChannelWriter<ScoredMatchPair> pairsWriter,
        ChannelWriter<MatchGraph> graphWriter,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            var (scoredPairs, matchGraph, processingTask) = await _comparisonService.CompareWithGraphAndProcessingAsync(
                candidates,
                configuration.MatchDefinitions,
                _indexMapper,
                cancellationToken);

            // Write graph
            await graphWriter.WriteAsync(matchGraph, cancellationToken);
            graphWriter.Complete();

            // Stream scored pairs to channel
            int pairCount = 0;
            await foreach (var pair in scoredPairs.WithCancellation(cancellationToken))
            {
                await pairsWriter.WriteAsync(pair, cancellationToken);
                pairCount++;

                if (pairCount % 1000 == 0)
                {
                    await progressTracker.UpdateProgressAsync(1000, $"Processed {pairCount} pairs");
                }
            }

            _logger.LogInformation("Stream completed. Processed {Count} pairs", pairCount);

            // ✅ FIX: Wait for background processing to complete
            _logger.LogInformation("Waiting for background processing to complete...");
            await processingTask;
            _logger.LogInformation("Background processing completed");
        }
        finally
        {
            pairsWriter.Complete();
        }
    }

    private async Task RunGroupingPipelineAsync(
        ChannelReader<MatchGraph> graphReader,
        ChannelWriter<MatchGroup> groupsWriter,
        bool requireTransitive,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            var graph = await graphReader.ReadAsync(cancellationToken);

            _logger.LogInformation("Creating groups from graph with {Nodes} nodes and {Edges} edges",
                graph.TotalNodes, graph.TotalEdges);

            int groupCount = 0;
            await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(
                graph,
                requireTransitive,
                cancellationToken))
            {
                await groupsWriter.WriteAsync(group, cancellationToken);
                groupCount++;

                if (groupCount % 100 == 0)
                {
                    await progressTracker.UpdateProgressAsync(100, $"Created {groupCount} groups");
                }
            }

            _logger.LogInformation("Grouping completed. Created {Count} groups", groupCount);
        }
        finally
        {
            groupsWriter.Complete();
        }
    }

    private async Task WritePairsAsync(
        ChannelReader<ScoredMatchPair> reader,
        string collectionName,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        var batch = new List<IDictionary<string, object>>(_options.BatchSize);
        int totalCount = 0;

        await foreach (var pair in reader.ReadAllAsync(cancellationToken))
        {
            var pairDict = ConvertPairToDictionary(pair);
            batch.Add(pairDict);
            totalCount++;

            if (batch.Count >= _options.BatchSize)
            {
                await FlushBatchAsync(batch, collectionName);
                await progressTracker.UpdateProgressAsync(batch.Count, $"Saved {totalCount} pairs");
            }
        }

        if (batch.Count > 0)
        {
            await FlushBatchAsync(batch, collectionName);
            await progressTracker.UpdateProgressAsync(batch.Count, $"Saved {totalCount} pairs");
        }

        _logger.LogInformation("Saved {Count} pairs to {Collection}", totalCount, collectionName);
    }

    private async Task WriteGraphAsync(
        ChannelReader<MatchGraph> reader,
        string collectionName,
        CancellationToken cancellationToken)
    {
        var graph = await reader.ReadAsync(cancellationToken);

        // Serialize graph to dictionary
        var graphDict = SerializeMatchGraph(graph);

        await _writeSemaphore.WaitAsync(cancellationToken);
        try
        {
            //await _dataStore.InsertBatchAsync(collectionName, new[] { graphDict });
            _logger.LogInformation("Saved match graph to {Collection}", collectionName);
        }
        finally
        {
            _writeSemaphore.Release();
        }
    }

    private async Task WriteGroupsAsync(
        ChannelReader<MatchGroup> reader,
        string collectionName,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        var batch = new List<IDictionary<string, object>>(_options.BatchSize);
        int totalCount = 0;

        await foreach (var group in reader.ReadAllAsync(cancellationToken))
        {
            var groupDict = new Dictionary<string, object>
            {
                ["GroupId"] = group.GroupId,
                ["GroupHash"] = group.GroupHash,
                ["Records"] = group.Records,
                ["Metadata"] = group.Metadata ?? new Dictionary<string, object>()
            };

            batch.Add(groupDict);
            totalCount++;

            if (batch.Count >= _options.BatchSize)
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
    }

    private async Task FlushBatchAsync(List<IDictionary<string, object>> batch, string collectionName)
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

    private IDictionary<string, object> ConvertPairToDictionary(ScoredMatchPair pair)
    {
        return new Dictionary<string, object>
        {
            ["PairId"] = pair.PairId,
            ["Record1"] = pair.Record1,
            ["Record2"] = pair.Record2,
            ["DataSource1Id"] = pair.DataSource1Id,
            ["DataSource2Id"] = pair.DataSource2Id,
            ["DataSource1Index"] = pair.DataSource1Index,
            ["DataSource2Index"] = pair.DataSource2Index,
            ["Row1Number"] = pair.Row1Number,
            ["Row2Number"] = pair.Row2Number,
            ["MaxScore"] = pair.MaxScore,
            ["ScoresByDefinition"] = pair.ScoresByDefinition
                .ToDictionary(kvp => kvp.Key.ToString(), kvp => (object)kvp.Value),
            ["MatchDefinitionIndices"] = pair.MatchDefinitionIndices
        };
    }

    private IDictionary<string, object> SerializeMatchGraph(MatchGraph graph)
    {
        var dict = new Dictionary<string, object>
        {
            ["TotalNodes"] = graph.TotalNodes,
            ["TotalEdges"] = graph.TotalEdges,
            ["CreatedAt"] = DateTime.UtcNow
        };

        // Serialize adjacency list
        var adjacencyList = new Dictionary<string, List<string>>();
        foreach (var kvp in graph.AdjacencyList)
        {
            adjacencyList[kvp.Key.ToString()] = kvp.Value.Select(v => v.ToString()).ToList();
        }
        dict["AdjacencyList"] = adjacencyList;

        // Serialize edge details
        var edgeDetails = new Dictionary<string, object>();
        foreach (var kvp in graph.EdgeDetails)
        {
            var edgeKey = $"{kvp.Key.Item1}_{kvp.Key.Item2}";
            edgeDetails[edgeKey] = new Dictionary<string, object>
            {
                ["PairId"] = kvp.Value.PairId,
                ["MaxScore"] = kvp.Value.MaxScore,
                ["MatchDefinitionIndices"] = kvp.Value.MatchDefinitionIndices,
                ["ScoresByDefinition"] = kvp.Value.ScoresByDefinition
                    .ToDictionary(sd => sd.Key.ToString(), sd => (object)sd.Value)
            };
        }
        dict["EdgeDetails"] = edgeDetails;

        // Serialize node metadata
        var nodeMetadata = new Dictionary<string, object>();
        foreach (var kvp in graph.NodeMetadata)
        {
            nodeMetadata[kvp.Key.ToString()] = new Dictionary<string, object>
            {
                ["RecordData"] = kvp.Value.RecordData,
                ["DegreeCount"] = kvp.Value.DegreeCount,
            };
        }
        dict["NodeMetadata"] = nodeMetadata;

        return dict;
    }

    private string GetDataSourceCollectionName(DataSource dataSource)
    {
        var headers = _headerUtility.GetHeadersFromCollectionAsync($"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSource.Id)}").Result;
        return headers.Count == 0 ? DatasetNames.SnapshotRows(dataSource.ActiveSnapshotId.GetValueOrDefault()) :
            $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSource.Id)}";
    }

    private CollectionNames GetCollectionNames(Guid projectId)
    {
        var normalizedProjectId = GuidCollectionNameConverter.ToValidCollectionName(projectId);

        return new CollectionNames
        {
            PairsCollection = $"pairs_{normalizedProjectId}",
            GraphCollection = $"matchgraph_{normalizedProjectId}",
            GroupsCollection = $"groups_{normalizedProjectId}"
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        _writeSemaphore?.Dispose();
        _qgramIndexer?.Dispose();

        if (_comparisonService != null)
            await _comparisonService.DisposeAsync();

        _logger.LogInformation("RecordLinkageOrchestrator disposed");
    }
}

// Supporting classes
public class OrchestrationConfiguration
{
    public Guid ProjectId { get; set; }
    public List<DataSource> DataSources { get; set; }
    public MatchDefinitionCollection MatchDefinitions { get; set; }
    public MatchingDataSourcePairs MatchConfiguration { get; set; }

    public MatchSettings MatchSettings { get; set; }
}

public class OrchestrationOptions
{
    public bool RequireTransitiveGroups { get; set; }
    public bool UseLegacyTransitiveAlgorithm { get; set; } = false;
    public bool PreferSmallestGroup { get; set; } = false;
    public bool EnableProbabilisticMatching { get; set; }
    public int MaxParallelism { get; set; }
    public bool EnableMatchQualityAnalysis { get; set; }
    public bool SaveMatchGraph { get; set; }

    public static OrchestrationOptions Default()
    {
        return new OrchestrationOptions
        {
            RequireTransitiveGroups = false,
            EnableProbabilisticMatching = false,
            MaxParallelism = Environment.ProcessorCount,
            UseLegacyTransitiveAlgorithm = true,
            PreferSmallestGroup = true,
            EnableMatchQualityAnalysis = true,
            SaveMatchGraph = false
        };
    }
}

public class OrchestrationResult
{
    public Guid ProjectId { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public bool Success { get; set; }
    public string ErrorMessage { get; set; }
    public CollectionNames OutputCollections { get; set; }
}

public class CollectionNames
{
    public string PairsCollection { get; set; }
    public string GraphCollection { get; set; }
    public string GroupsCollection { get; set; }
    public string MasterCollection { get; set; }
    public string OverwrittenGroupsCollection { get; set; }
    public string AnalyticsCollection { get; set; }
}

public interface IRecordLinkageOrchestrator
{
    Task<OrchestrationResult> ExecuteRecordLinkageAsync(
        Guid projectId,
        OrchestrationOptions options = null,
        ICommandContext commandContext = null,
        CancellationToken cancellationToken = default);
}