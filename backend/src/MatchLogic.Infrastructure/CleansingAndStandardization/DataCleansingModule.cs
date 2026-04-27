using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization;

internal class DataCleansingModule : ICleansingModule
{
    private const int BatchSize = 1000;
    private const int MaxDegreeOfParallelism = 4;
    private const int PreviewRecordLimit = 100;
    private readonly IDataStore _dataStore;
    private readonly ILogger<DataCleansingModule> _logger;
    private readonly IRulesManager<EnhancedTransformationRule> _rulesManager;
    private readonly IEnhancedRuleFactory _ruleFactory;
    private readonly IJobEventPublisher _jobEventPublisher;
    public DataCleansingModule(
            IDataStore dataStore,
            IRulesManager<EnhancedTransformationRule> rulesManager,
            IEnhancedRuleFactory ruleFactory,
             IJobEventPublisher jobEventPublisher,
            ILogger<DataCleansingModule> logger
        )
    {
        _dataStore = dataStore;
        _logger = logger;
        _rulesManager = rulesManager ?? throw new ArgumentNullException(nameof(rulesManager));
        _ruleFactory = ruleFactory ?? throw new ArgumentNullException(nameof(ruleFactory));
        _jobEventPublisher = jobEventPublisher ?? throw new ArgumentNullException(nameof(_jobEventPublisher));
    }
    public async Task<Guid> ProcessDataAsync(string inputCollection, string outputCollection
        , EnhancedCleaningRules fieldOperations, ICommandContext commandContext = null, bool isPreview = false
        , CancellationToken cancellationToken = default)
    {
        try
        {
            await _dataStore.DeleteCollection(outputCollection);

            var jobId = await _dataStore.InitializeJobAsync(outputCollection);


            _logger.LogInformation("Starting cleansing process with job ID: {JobId}", jobId);

            var stepId = commandContext == null ? Guid.Empty : commandContext.StepId;
            var ruleStep = _jobEventPublisher.CreateStepTracker(stepId, "Applying transformation", 1, 2);
            var wrtingStep = _jobEventPublisher.CreateStepTracker(stepId, "Writing data", 2, 2);

            await ruleStep.StartStepAsync(0, cancellationToken);
            await wrtingStep.StartStepAsync(0, cancellationToken);

            // Load rules into the rules manager
            await _rulesManager.LoadRulesFromConfigAsync(fieldOperations);

            var dataQueue = new BlockingCollection<List<IDictionary<string, object>>>(MaxDegreeOfParallelism * 2);

            // Start reader task
            var readerTask = Task.Run(
                () => ReadDataFromSourceAsync(jobId, commandContext, dataQueue, ruleStep, inputCollection,isPreview, cancellationToken),
                cancellationToken);

            // Start processor tasks
            var processorTasks = Enumerable.Range(0, MaxDegreeOfParallelism)
                .Select(_ => Task.Run(
                    () => ProcessDataBatchAsync(jobId, commandContext, dataQueue, outputCollection, wrtingStep, cancellationToken),
                    cancellationToken))
                .ToArray();

            // Wait for completion
            await Task.WhenAll(readerTask.ContinueWith(_ => dataQueue.CompleteAdding()));
            await Task.WhenAll(processorTasks);
            

            await ruleStep.CompleteStepAsync();
            await wrtingStep.CompleteStepAsync();            

            return jobId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during cleansing process");
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

            // Get the source enumerable
            var source = _dataStore.StreamJobDataAsync(jobId, progressTracker, inputCollection, cancellationToken);

            // If preview mode, limit the source using Take
            if (isPreview)
            {
                source = source.Take(PreviewRecordLimit);
            }

            var currentBatchSize = isPreview ? Math.Min(BatchSize, PreviewRecordLimit) : BatchSize;
            await foreach (var row in source)
            {
                batch.Add(row);
                totalRecordsRead++;

                // For preview mode, use smaller batches for better responsiveness               

                if (batch.Count >= currentBatchSize)
                {
                    dataQueue.Add(batch, cancellationToken);
                    batch = new List<IDictionary<string, object>>();
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

                // Apply transformations
                _rulesManager.ApplyRules(recordBatch);

                // Update statistics
                lock (flowContext.Statistics)
                {
                    flowContext.Statistics.RecordsProcessed += recordBatch.Count;
                    flowContext.Statistics.BatchesProcessed++;
                }
                
                // Count transformations
                //int transformationCount = 0;
                //foreach (var record in recordBatch.Records)
                //{
                //    foreach (var column in record.Columns)
                //    {
                //        transformationCount += column.AppliedTransformations.Count;
                //    }
                //}
                //flowContext.Statistics.TransformationsApplied = transformationCount;

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
