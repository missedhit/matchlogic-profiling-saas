using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Transform;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Transform;
using MatchLogic.Domain.Export;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Export;

internal class DataExportModule : IExportModule
{
    private const int BatchSize = 1000;
    private const int MaxDegreeOfParallelism = 4;
    private readonly IDataStore _dataStore;
    private readonly ILogger<DataExportModule> _logger;
    private readonly IExportDataStrategy _exportStrategy;
    private readonly IColumnFilter _columnFilter;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IDataTransformerFactory _transformerFactory;

    private readonly IAutoMappingService _mappingService;
    private readonly IGenericRepository<MatchLogic.Domain.Project.DataSource, Guid> _dataSourceRepository;

    public DataExportModule(
        IDataStore dataStore,
        IExportDataStrategy exportStrategy,
        IColumnFilter columnFilter,
        IJobEventPublisher jobEventPublisher,
        IDataTransformerFactory transformerFactory,
        IAutoMappingService mappingService,
        IGenericRepository<MatchLogic.Domain.Project.DataSource, Guid> dataSourceRepository,
        ILogger<DataExportModule> logger)
    {
        _dataStore = dataStore;
        _exportStrategy = exportStrategy;
        _columnFilter = columnFilter;
        _jobEventPublisher = jobEventPublisher;
        _transformerFactory = transformerFactory;
        _mappingService = mappingService;
        _dataSourceRepository = dataSourceRepository;
        _logger = logger;
    }

    public async Task<bool> ExportDataAsync(
        DataExportOptions options,
        ICommandContext commandContext,
        CancellationToken cancellationToken = default)
    {
        var dataQueue = new BlockingCollection<List<IDictionary<string, object>>>(MaxDegreeOfParallelism * 2);
        var jobId = await _dataStore.InitializeJobAsync(options.CollectionName);

        try
        {
            var readingStep = _jobEventPublisher.CreateStepTracker(commandContext.StepId, "Reading Data", 1, 3);
            var transformingStep = _jobEventPublisher.CreateStepTracker(commandContext.StepId, "Transforming Data", 2, 3);
            var exportingStep = _jobEventPublisher.CreateStepTracker(commandContext.StepId, "Exporting Data", 3, 3);

            await readingStep.StartStepAsync(0, cancellationToken);
            await transformingStep.StartStepAsync(0, cancellationToken);
            await exportingStep.StartStepAsync(0, cancellationToken);


            // Start reader task
            var readerTask = Task.Run(
                () => ReadDataFromSourceAsync(jobId, dataQueue, readingStep, options.CollectionName, cancellationToken),
                cancellationToken);

            // Start processor tasks with transformation pipeline
            var processorTasks = Enumerable.Range(0, MaxDegreeOfParallelism)
                .Select(_ => Task.Run(
                    () => ProcessExportBatchAsync(jobId, dataQueue, options, transformingStep, exportingStep, cancellationToken),
                    cancellationToken))
                .ToArray();

            // Wait for completion
            await Task.WhenAll(readerTask.ContinueWith(_ => dataQueue.CompleteAdding()));
            await Task.WhenAll(processorTasks);

            await readingStep.CompleteStepAsync();
            await transformingStep.CompleteStepAsync();
            await exportingStep.CompleteStepAsync();

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during export process for collection {Collection}", options.CollectionName);
            throw;
        }
    }

    private async Task ReadDataFromSourceAsync(
        Guid jobId,
        BlockingCollection<List<IDictionary<string, object>>> dataQueue,
        IStepProgressTracker progressTracker,
        string inputCollection,
        CancellationToken cancellationToken)
    {
        try
        {
            var batch = new List<IDictionary<string, object>>();
            await foreach (var row in _dataStore.StreamJobDataAsync(jobId, progressTracker, inputCollection, cancellationToken))
            {
                batch.Add(row);
                if (batch.Count >= BatchSize)
                {
                    dataQueue.Add(batch, cancellationToken);
                    batch = new List<IDictionary<string, object>>();
                }
            }

            if (batch.Count > 0)
            {
                dataQueue.Add(batch, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading data from source");
            throw;
        }
    }

    /// <summary>
    /// Process batches with transformation pipeline.
    /// Data flow: Read → Filter (existing ColumnMappings) → Transform → Export
    /// 
    /// Transformation configuration is passed via transformerConfiguration parameter.
    /// This separates export concerns (DataExportOptions) from transformation concerns (TransformerConfiguration).
    /// 
    /// EXAMPLE FLOWS:
    /// 1. Simple column renaming (no transformation):
    ///    - options.TransformerType: "none"
    ///    - transformerConfiguration.ColumnProjections: { "groupId": "Group ID", "dataSourceName": "Data Source" }
    ///    - Result: Columns renamed but data unchanged
    /// 
    /// 2. Flatten with whitespace separator and rename:
    ///    - options.TransformerType: "flatten"
    ///    - transformerConfiguration.Settings: { "separator": " ", "depth": 2 }
    ///    - transformerConfiguration.ColumnProjections: { "user name": "User Name", "user age": "User Age" }
    ///    - Result: Nested data flattened with space, then renamed
    /// 
    /// 3. Select any Custom Transformer columns and rename:
    ///    - options.TransformerType: "projection"
    ///    - transformerConfiguration.Settings: { "columns": ["id", "name", "email"] }
    ///    - transformerConfiguration.ColumnProjections: { "name": "Name", "email": "Email Address" }
    ///    - Result: Only specified columns, renamed
    /// </summary>
    private async Task ProcessExportBatchAsync(
        Guid jobId,
        BlockingCollection<List<IDictionary<string, object>>> dataQueue,
        DataExportOptions options,
        IStepProgressTracker transformingStep,
        IStepProgressTracker exportingStep,
        CancellationToken cancellationToken)
    {
        // Get pre-computed export context data
        var exportableFieldMappings = await _mappingService.GetSavedExportableMappedFieldRowsAsync(options.ProjectId);
        var dataSourceDict = (await _dataSourceRepository.QueryAsync(ds => ds.ProjectId == options.ProjectId, Constants.Collections.DataSources))
            .ToDictionary(ds => ds.Id, ds => ds.Name);

        var transformerConfiguration = new TransformerConfiguration
        {
            TransformerType = options.ViewType,
            Settings = new Dictionary<string, object>
            {
                ["fieldMappings"] = exportableFieldMappings,
                ["dataSourceDict"] = dataSourceDict
            }
        };

        using var transformer = _transformerFactory.GetTransformer(transformerConfiguration);
        
        try
        {
            foreach (var batch in dataQueue.GetConsumingEnumerable(cancellationToken))
            {
                // Transform with streaming for memory efficiency
                var transformedStream = transformer.TransformAsync(
                    batch.ToAsyncEnumerable(),
                    cancellationToken);

                // Track transformation progress
                var trackedStream = TrackTransformationProgress(
                    transformedStream,
                    transformingStep,
                    cancellationToken);

                // Export with optimized context - field mappings passed for schema creation
                await _exportStrategy.ExportAsync(
                    trackedStream, 
                    options, 
                    _columnFilter, 
                    exportableFieldMappings, 
                    cancellationToken);

                await exportingStep.UpdateProgressAsync(batch.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing export batch with transformer {TransformerName}",
                transformer.Name);
            throw;
        }
    }

    /// <summary>
    /// Wrap transformation stream to track progress.
    /// Minimal overhead: single counter increment per row.
    /// </summary>
    private static async IAsyncEnumerable<IDictionary<string, object>> TrackTransformationProgress(
        IAsyncEnumerable<IDictionary<string, object>> transformedRows,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken)
    {
        await foreach (var row in transformedRows.WithCancellation(cancellationToken))
        {
            await progressTracker.UpdateProgressAsync(1);
            yield return row;
        }
    }
}