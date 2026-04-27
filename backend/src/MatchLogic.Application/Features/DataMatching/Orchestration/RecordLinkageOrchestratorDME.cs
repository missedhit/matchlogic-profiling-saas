using Mapster;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.Grouping;
using MatchLogic.Application.Features.DataMatching.Orchestration;
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
using MatchLogic.Application.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Application.Features.DataMatching.Storage;
using MatchLogic.Application.Features.DataMatching.Analytics;
using MatchLogic.Domain.Analytics;

namespace MatchLogic.Application.Features.DataMatching.Orchestration;

public class RecordLinkageOrchestratorDME : IRecordLinkageOrchestrator, IAsyncDisposable
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
    private OrchestrationOptions _orchestrationOptions;
    private readonly IMatchGraphStorage _graphStorage;
    private bool _disposed;
    private readonly MatchQualityAnalysisDME _matchQualityAnalysis;

    public RecordLinkageOrchestratorDME(
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
        IHeaderUtility headerUtility,
        IMatchGraphStorage graphStorage,
        MatchQualityAnalysisDME matchQualityAnalysis)
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
        _headerUtility = headerUtility;
        _graphStorage = graphStorage;
        _matchQualityAnalysis = matchQualityAnalysis ?? new MatchQualityAnalysisDME();
    }

    public async Task<OrchestrationResult> ExecuteRecordLinkageAsync(
        Guid projectId,
        OrchestrationOptions orchestrationOptions = null,
        ICommandContext commandContext = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        orchestrationOptions ??= OrchestrationOptions.Default();
        _orchestrationOptions = orchestrationOptions;

        using var operation = _telemetry.MeasureOperation("execute_record_linkage_dme");

        var result = new OrchestrationResult
        {
            ProjectId = projectId,
            StartTime = DateTime.UtcNow
        };

        try
        {
            _logger.LogInformation(
                "Starting DME record linkage for project {ProjectId}", projectId);

            var stepId = commandContext?.StepId ?? Guid.Empty;

            // Step 1: Load configuration
            var configuration = await LoadConfigurationAsync(projectId, cancellationToken);
            orchestrationOptions.RequireTransitiveGroups = configuration.MatchSettings.MergeOverlappingGroups;

            // Step 2: Initialize index mapper
            await _indexMapper.InitializeAsync(projectId);

            var collectionNames = GetCollectionNames(projectId);
            await _dataStore.DeleteCollection(collectionNames.GraphCollection);
            await _dataStore.DeleteCollection(collectionNames.GroupsCollection);
            await _dataStore.DeleteCollection(collectionNames.PairsCollection);
            if (_graphStorage.GraphExistsAsync(_dataStore, collectionNames.GraphCollection, cancellationToken).GetAwaiter().GetResult())
            {
                await _dataStore.DeleteCollection(collectionNames.GraphCollection);
            }
            await _dataStore.DeleteCollection(collectionNames.MasterCollection);
            await _dataStore.DeleteCollection(collectionNames.OverwrittenGroupsCollection);
            await _dataStore.DeleteCollection(collectionNames.AnalyticsCollection);

            // Step 3: Setup progress tracking
            var totalSteps = orchestrationOptions.EnableMatchQualityAnalysis ? 6 : 5;

            var indexingStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Indexing Data Sources", 1, totalSteps);
            var candidateStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Generating Candidates", 2, totalSteps);
            var comparisonStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Comparing Records", 3, totalSteps);
            var groupingStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Creating Groups", 4, totalSteps);
            var persistenceStep = _jobEventPublisher.CreateStepTracker(
                stepId, "Saving Results", 5, totalSteps);

            IStepProgressTracker analyticsStep = null;
            if (orchestrationOptions.EnableMatchQualityAnalysis)
            {
                analyticsStep = _jobEventPublisher.CreateStepTracker(
                    stepId, "Analyzing Match Quality", 6, totalSteps);
            }
            // Step 4: Index all data sources
            if (_qgramIndexer is ProductionQGramIndexerDME dmeIndexer)
            {
                dmeIndexer.InitializeBlockingConfiguration(configuration.MatchDefinitions);
            }

            await IndexDataSourcesAsync(configuration, indexingStep, cancellationToken);

            // Step 5: Generate candidates
            await candidateStep.StartStepAsync(0, cancellationToken);
            var candidates = _qgramIndexer.GenerateCandidatesFromMatchDefinitionsAsync(
                configuration.MatchDefinitions, cancellationToken);

            // Step 6: Compare records and COLLECT pairs (no graph building yet)
            await comparisonStep.StartStepAsync(0, cancellationToken);

            List<ScoredMatchPair> collectedPairs;

            if (_comparisonService is EnhancedRecordComparisonServiceDME dmeComparisonService)
            {
                // Use new synchronous comparison with pair collection
                var (pairs, _) = await dmeComparisonService.CompareAndCollectPairsAsync(
                    candidates,
                    configuration.MatchDefinitions,
                    _indexMapper,
                    cancellationToken);

                collectedPairs = pairs;
            }
            else
            {
                // Fallback to collecting from async enumerable
                collectedPairs = new List<ScoredMatchPair>();
                await foreach (var pair in _comparisonService.CompareAsync(
                    candidates, configuration.MatchDefinitions, _indexMapper, cancellationToken))
                {
                    collectedPairs.Add(pair);
                }
            }

            // Step 7: Write pairs to storage
            await persistenceStep.StartStepAsync(0, cancellationToken);

            var pairsChannel = Channel.CreateBounded<ScoredMatchPair>(_options.BufferSize);
            var pairsWriter = WritePairsAsync(
                pairsChannel.Reader,
                collectionNames.PairsCollection,
                persistenceStep,
                cancellationToken);

            // Feed collected pairs to writer
            foreach (var pair in collectedPairs)
            {
                await pairsChannel.Writer.WriteAsync(pair, cancellationToken);
            }
            pairsChannel.Writer.Complete();

            await pairsWriter;

            // Step 8: Build graph from collected pairs (DEFERRED - single-threaded, optimized)
            await groupingStep.StartStepAsync(0, cancellationToken);

            var matchGraph = BuildGraphFromCollectedPairs(collectedPairs, configuration.MatchDefinitions);

            _logger.LogInformation(
                "Built graph: {Nodes} nodes, {Edges} edges",
                matchGraph.TotalNodes, matchGraph.TotalEdges);
            // Step 8.5: Generate graph-based analytics
            MatchQualityReportDME analyticsReport = null;
            GroupStatisticsAccumulator groupAccumulator = null;

            if (orchestrationOptions.EnableMatchQualityAnalysis)
            {
                await analyticsStep.StartStepAsync(0, cancellationToken);
                await analyticsStep.UpdateProgressAsync(0, "Analyzing match graph...");

                // Build data source name lookup
                var dataSourceNames = configuration.DataSources
                    .ToDictionary(ds => ds.Id, ds => ds.Name);

                var scoreBand = await _dataStore.QueryAsync<ScoreBandCollection>(x => x.ProjectId == projectId
                , Constants.Collections.ScoreBand);

                // Generate graph-based analytics
                analyticsReport = _matchQualityAnalysis.GenerateReport(
                    matchGraph,
                    configuration.MatchDefinitions,
                    scoreBand.FirstOrDefault(),
                    dataSourceNames);

                // Initialize accumulator for group statistics
                groupAccumulator = new GroupStatisticsAccumulator();

                await analyticsStep.UpdateProgressAsync(30,
                    $"Graph analysis complete: {analyticsReport.Summary.MatchRate:P1} match rate");

                _logger.LogInformation(
                    "Graph analytics completed: {MatchRate:P1} match rate, {Pairs} pairs",
                    analyticsReport.Summary.MatchRate,
                    analyticsReport.Summary.TotalMatchPairs);
            }

            // Step 9: Write graph
            var graphChannel = Channel.CreateUnbounded<MatchGraphDME>();
            await graphChannel.Writer.WriteAsync(matchGraph, cancellationToken);
            graphChannel.Writer.Complete();

            // Step 10: Create groups from graph
            var groupsChannel = Channel.CreateBounded<MatchGroup>(_options.BufferSize);
            var groupingTask = RunGroupingPipelineAsync(
                matchGraph,
                groupsChannel.Writer,
                orchestrationOptions.RequireTransitiveGroups,
                configuration.MatchConfiguration,
                groupingStep,
                cancellationToken);

            Task groupsWriter;
            if (orchestrationOptions.EnableMatchQualityAnalysis && groupAccumulator != null)
            {
                groupsWriter = WriteGroupsWithAccumulatorAsync(
                    groupsChannel.Reader,
                    collectionNames.GroupsCollection,
                    groupAccumulator,
                    persistenceStep,
                    cancellationToken);
            }
            else
            {
                groupsWriter = WriteGroupsAsync(
                    groupsChannel.Reader,
                    collectionNames.GroupsCollection,
                    persistenceStep,
                    cancellationToken);
            }

            // Wait for all to complete
            await Task.WhenAll(groupingTask, groupsWriter);

            var graphWriter = WriteGraphAsync(
                graphChannel.Reader,
                collectionNames.GraphCollection,
                cancellationToken);

            await Task.WhenAll(graphWriter);

            // Step 11: Finalize analytics with group data
            if (orchestrationOptions.EnableMatchQualityAnalysis && analyticsReport != null)
            {
                await analyticsStep.UpdateProgressAsync(0, "Populating group statistics...");

                // Populate GroupIds by threshold from accumulated data
                if (groupAccumulator != null)
                {
                    _matchQualityAnalysis.PopulateGroupIdsByThreshold(
                        analyticsReport,
                        groupAccumulator.GetGroupScores());
                }

                await analyticsStep.UpdateProgressAsync(0, "Saving analytics report...");

                // Write analytics to separate collection
                await WriteAnalyticsAsync(
                    analyticsReport,
                    collectionNames.AnalyticsCollection,
                    cancellationToken);

                await analyticsStep.CompleteStepAsync(
                    $"Analytics complete: {analyticsReport.Anomalies?.Count ?? 0} issues found, " +
                    $"{groupAccumulator?.GroupCount ?? 0} groups analyzed");

                _logger.LogInformation(
                    "Match quality analytics saved to {Collection}: {Groups} groups, {Anomalies} anomalies",
                    collectionNames.AnalyticsCollection,
                    groupAccumulator?.GroupCount ?? 0,
                    analyticsReport.Anomalies?.Count ?? 0);
            }

            await _dataStore.CreateGroupFilterIndexesAsync(collectionNames.GroupsCollection);

            await candidateStep.CompleteStepAsync("Candidate generation completed");
            await comparisonStep.CompleteStepAsync($"Comparison completed: {collectedPairs.Count} pairs");
            await groupingStep.CompleteStepAsync("Grouping completed");
            await persistenceStep.CompleteStepAsync("Results saved");

            result.EndTime = DateTime.UtcNow;
            result.Success = true;
            result.OutputCollections = collectionNames;

            _logger.LogInformation(
                "Completed DME record linkage for project {ProjectId}", projectId);

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

    /// <summary>
    /// Build graph from collected pairs - SINGLE-THREADED, OPTIMIZED
    /// Uses MatchGraphDME which has no concurrent collections or locks
    /// </summary>
    private MatchGraphDME BuildGraphFromCollectedPairs(
        List<ScoredMatchPair> pairs,
        MatchDefinitionCollection matchDefinitions)
    {
        _logger.LogInformation(
            "Building optimized graph from {Count} pairs (single-threaded)",
            pairs.Count);

        // Pre-allocate capacity based on pair count
        // Estimate: typically 1.5-2x pairs for nodes in dense graphs
        var estimatedNodes = (int)(pairs.Count * 1.8);
        var graph = new MatchGraphDME(matchDefinitions.ProjectId, estimatedNodes);

        // Process pairs sequentially - no locks needed
        foreach (var pair in pairs)
        {
            var node1 = new RecordKey(pair.DataSource1Id, pair.Row1Number);
            var node2 = new RecordKey(pair.DataSource2Id, pair.Row2Number);

            // Add nodes (internally checks if exists)
            graph.AddNode(node1, pair.Record1);
            graph.AddNode(node2, pair.Record2);

            // Add edge with details
            var edgeDetails = new MatchEdgeDetails
            {
                PairId = pair.PairId,
                MaxScore = pair.MaxScore,
                MatchDefinitionIndices = new List<int>(pair.MatchDefinitionIndices),
                ScoresByDefinition = new Dictionary<int, MatchScoreDetail>(pair.ScoresByDefinition),
                MatchedAt = DateTime.UtcNow
            };

            graph.AddEdge(node1, node2, edgeDetails);

            // Update participating definitions in node metadata
            if (graph.NodeMetadata.TryGetValue(node1, out var meta1))
            {
                foreach (var defIndex in pair.MatchDefinitionIndices)
                    meta1.ParticipatingDefinitions.Add(defIndex);
            }

            if (graph.NodeMetadata.TryGetValue(node2, out var meta2))
            {
                foreach (var defIndex in pair.MatchDefinitionIndices)
                    meta2.ParticipatingDefinitions.Add(defIndex);
            }
        }

        // Update all node degrees after all edges added (more efficient)
        graph.UpdateNodeDegrees();

        // Log statistics
        var stats = graph.GetStatistics();
        _logger.LogInformation(
            "Graph built: {Nodes} nodes, {Edges} edges, avg degree: {AvgDegree:F1}, memory: {Memory}",
            stats.TotalNodes, stats.TotalEdges, stats.AverageDegree, stats.EstimatedMemoryMB);

        return graph;
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
                UseInMemoryStore = dataSource.RecordCount < 100000, // Use memory for smaller datasets
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

        if (_qgramIndexer is ProductionQGramIndexerDME dmeIndexer2)
        {
            dmeIndexer2.SealGlobalIndex();
            _logger.LogInformation("Global index sealed after indexing all datasources");
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

    /// <summary>
    /// Updated grouping to work with pre-built graph
    /// </summary>
    private async Task RunGroupingPipelineAsync(
        MatchGraphDME matchGraph,
        ChannelWriter<MatchGroup> groupsWriter,
        bool requireTransitive,
        MatchingDataSourcePairs configuredSourcePairs,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "Creating groups from optimized graph with {Nodes} nodes and {Edges} edges",
                matchGraph.TotalNodes, matchGraph.TotalEdges);

            int groupCount = 0;
            await foreach (var group in _groupingService.CreateGroupsFromGraphAsync(
                matchGraph, requireTransitive, _orchestrationOptions.PreferSmallestGroup, _orchestrationOptions.UseLegacyTransitiveAlgorithm, configuredSourcePairs
                , cancellationToken))
            {
                await groupsWriter.WriteAsync(group, cancellationToken);
                groupCount++;

                if (groupCount % 100 == 0)
                {
                    await progressTracker.UpdateProgressAsync(
                        100, $"Created {groupCount} groups");
                }
            }

            _logger.LogInformation("Grouping completed: {Count} groups", groupCount);
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
        ChannelReader<MatchGraphDME> graph,
        string collectionName,
        CancellationToken cancellationToken)
    {
        await foreach (var g in graph.ReadAllAsync(cancellationToken))
        {
            if (_orchestrationOptions.SaveMatchGraph)
            {
                await _graphStorage.SaveMatchGraphAsync(
                    _dataStore,
                    collectionName,
                    g,
                    cancellationToken);
            }
        }
    }

    /// <summary>
    /// Write groups to storage AND accumulate statistics for analytics.
    /// Single-threaded - no locks needed.
    /// </summary>
    private async Task WriteGroupsWithAccumulatorAsync(
        ChannelReader<MatchGroup> reader,
        string collectionName,
        GroupStatisticsAccumulator accumulator,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        var batch = new List<IDictionary<string, object>>(_options.BatchSize);
        int totalCount = 0;

        await foreach (var group in reader.ReadAllAsync(cancellationToken))
        {
            // Accumulate group stats (no locks - single-threaded)
            if (accumulator != null && group.Metadata != null)
            {
                var avgScore = 0.0;
                if (group.Metadata.TryGetValue("avg_match_score", out var scoreObj))
                {
                    avgScore = Convert.ToDouble(scoreObj);
                }

                accumulator.AccumulateGroup(
                    group.GroupId,
                    avgScore,
                    group.Records?.Count ?? 0);
            }

            // Serialize group for storage (same as existing WriteGroupsAsync)
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
                await progressTracker.UpdateProgressAsync(
                    batch.Count, $"Saved {totalCount} groups");
            }
        }

        if (batch.Count > 0)
        {
            await FlushBatchAsync(batch, collectionName);
            await progressTracker.UpdateProgressAsync(
                batch.Count, $"Saved {totalCount} groups");
        }

        _logger.LogInformation(
            "Saved {Count} groups to {Collection}",
            totalCount, collectionName);
    }

    /// <summary>
    /// Write analytics report to separate collection
    /// </summary>
    private async Task WriteAnalyticsAsync(
        MatchQualityReportDME report,
        string collectionName,
        CancellationToken cancellationToken)
    {
        var analyticsDict = report.ToDictionary();
        var batch = new List<IDictionary<string, object>> { analyticsDict };

        await FlushBatchAsync(batch, collectionName);

        _logger.LogInformation(
            "Saved analytics report to {Collection}",
            collectionName);
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


    private IDictionary<string, object> SerializeMatchGraphDME(MatchGraphDME graph)
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
            .ToDictionary(
                kvp => kvp.Key.ToString(),
                kvp => (object)ConvertScoreDetailToDictionary(kvp.Value)),
            ["MatchDefinitionIndices"] = pair.MatchDefinitionIndices
        };
    }

    /// <summary>
    /// Convert MatchScoreDetail to dictionary for MongoDB serialization
    /// </summary>
    private IDictionary<string, object> ConvertScoreDetailToDictionary(MatchScoreDetail detail)
    {
        return new Dictionary<string, object>
        {
            ["WeightedScore"] = detail.WeightedScore,
            ["FinalScore"] = detail.FinalScore,
            ["FieldScores"] = new Dictionary<string, object>(
                                        detail.FieldScores.ToDictionary(
                                            fs => fs.Key,
                                            fs => (object)fs.Value
                                        )
                                    ), // ✅ Explicit dictionary
            ["FieldWeights"] = new Dictionary<string, object>(
                                        detail.FieldWeights.ToDictionary(
                                            fs => fs.Key,
                                            fs => (object)fs.Value
                                        )
                                    ) // ✅ Explicit dictionary
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
            GroupsCollection = $"groups_{normalizedProjectId}",
            MasterCollection = $"groups_master_{normalizedProjectId}",
            OverwrittenGroupsCollection = $"groups_overwritten_{normalizedProjectId}",
            AnalyticsCollection = $"analytics_{normalizedProjectId}"
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
//public class OrchestrationConfiguration
//{
//    public Guid ProjectId { get; set; }
//    public List<DataSource> DataSources { get; set; }
//    public MatchDefinitionCollection MatchDefinitions { get; set; }
//    public MatchingDataSourcePairs MatchConfiguration { get; set; }

//    public MatchSettings MatchSettings { get; set; }
//}

//public class OrchestrationOptions
//{
//    public bool RequireTransitiveGroups { get; set; }
//    public bool EnableProbabilisticMatching { get; set; }
//    public int MaxParallelism { get; set; }

//    public static OrchestrationOptions Default()
//    {
//        return new OrchestrationOptions
//        {
//            RequireTransitiveGroups = false,
//            EnableProbabilisticMatching = false,
//            MaxParallelism = Environment.ProcessorCount
//        };
//    }
//}

//public class OrchestrationResult
//{
//    public Guid ProjectId { get; set; }
//    public DateTime StartTime { get; set; }
//    public DateTime EndTime { get; set; }
//    public bool Success { get; set; }
//    public string ErrorMessage { get; set; }
//    public CollectionNames OutputCollections { get; set; }
//}

//public class CollectionNames
//{
//    public string PairsCollection { get; set; }
//    public string GraphCollection { get; set; }
//    public string GroupsCollection { get; set; }
//}

//public interface IRecordLinkageOrchestrator
//{
//    Task<OrchestrationResult> ExecuteRecordLinkageAsync(
//        Guid projectId,
//        OrchestrationOptions options = null,
//        ICommandContext commandContext = null,
//        CancellationToken cancellationToken = default);
//}