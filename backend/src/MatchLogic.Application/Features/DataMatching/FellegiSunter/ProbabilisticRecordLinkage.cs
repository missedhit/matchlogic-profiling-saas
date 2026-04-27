using CsvHelper;
using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Formats.Asn1;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace MatchLogic.Application.Features.DataMatching.FellegiSunter;
public class ProbabilisticRecordLinkage
{
    #region Fields
    private readonly ParallelEM _expectationMaximisation;
    private List<ProbabilisticMatchCriteria> _fields;
    private readonly IDataStore _dataStore;
    private Guid _jobId;
    private string _sampleRecordCollection;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IBlockingStrategy _blockingStrategy;
    private readonly SemaphoreSlim _semaphore;
    private readonly ILogger _logger;
    private readonly SimpleRecordPairer _simpleRecordPairer;
    private readonly ITelemetry _telemetry;
    private readonly Channel<MatchResult> _matchChannel;
    private List<ProbabilisticMatchCriteria> _filterFields;
    private ProbabilisticOption _option;
    private IComparatorBuilder _comparatorBuilder;
    #endregion

    #region Constructor
    public ProbabilisticRecordLinkage(ParallelEM em,
        IDataStore dataStore,
        IJobEventPublisher jobEventPublisher,
        IBlockingStrategy blockingStrategy,
        ITelemetry telemetry,
        SimpleRecordPairer simpleRecordPairer,
        IComparatorBuilder comparatorBuilder,
        ILogger<ParallelEM> logger, IOptions<ProbabilisticOption> option)
    {
        _expectationMaximisation = em;
        _dataStore = dataStore;
        _comparatorBuilder = comparatorBuilder;
        _jobEventPublisher = jobEventPublisher;
        _blockingStrategy = blockingStrategy;
        _telemetry = telemetry;
        _simpleRecordPairer = simpleRecordPairer;
        _logger = logger;
        _option = option.Value;
        _semaphore = new SemaphoreSlim(2);
        _matchChannel = Channel.CreateBounded<MatchResult>(
            new BoundedChannelOptions(_option.BufferSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });
    }
    #endregion
    #region Methods

    public void Initialize(Guid sourceJobId, List<MatchCriteria> matchCriterias)
    {
        _jobId = sourceJobId;
        _fields = ConvertFromMatchCriteriaToProbabilisticMatchCriteria(matchCriterias);
        _sampleRecordCollection = GuidCollectionNameConverter.ToValidCollectionName(_jobId) + "_sampleRecord";
    }
    private List<ProbabilisticMatchCriteria> ConvertFromMatchCriteriaToProbabilisticMatchCriteria(List<MatchCriteria> matchCriterias)
    {
        List<ProbabilisticMatchCriteria> _convertedList = new List<ProbabilisticMatchCriteria>();
        foreach (var criteria in matchCriterias)
        {
            ProbabilisticMatchCriteria convertedCriteria = new ProbabilisticMatchCriteria(criteria.FieldName, _comparatorBuilder, criteria.Arguments, _option);
            convertedCriteria.MatchingType = criteria.MatchingType;
            _convertedList.Add(convertedCriteria);
        }
        return _convertedList;
    }
    public async Task Train()
    {
        await GetSampleForDedupe();

        CancellationToken cancellationToken = default;

        var broadcastChannel = new BroadcastChannel<IDictionary<string, object>>(_option.BufferSize, 2);

        var broadcastTask = broadcastChannel.StartBroadcastAsync(cancellationToken);

        ParallelTermFrequencyIndex parallelTermFrequencyIndex = new ParallelTermFrequencyIndex();

        var exactCriteria = _fields
         .Where(c => c.MatchingType == MatchingType.Exact && c.DataType != CriteriaDataType.Phonetic)
         .ToList();

        var exactCriteriaField = exactCriteria.Select(x => x.FieldName).ToList();

        var filterFields = _fields.Where(x => !exactCriteriaField.Contains(x.FieldName)).ToList();
        _filterFields = filterFields;
        _expectationMaximisation.Initialize(filterFields);


        var indexingTask = parallelTermFrequencyIndex.IndexTerms(broadcastChannel.Readers[0]);

        var patternTask = _expectationMaximisation.GeneratePatterns(broadcastChannel.Readers[1]);

        var statsChannel = new List<IDictionary<string, object>>();
        await foreach (var record in _dataStore.GetStreamFromTempCollection(_sampleRecordCollection, CancellationToken.None))
        {
            await broadcastChannel.Writer.WriteAsync(record);
            statsChannel.Add(record);
        }


        broadcastChannel.Writer.Complete();

        await Task.WhenAll(broadcastTask, indexingTask, patternTask);

        foreach (var field in _filterFields)
        {
            var values = statsChannel.Select(r => r[field.FieldName]?.ToString() ?? "").ToAsyncEnumerable();
            field.Statistics = await ParallelFieldStatistics.Calculate(values, parallelTermFrequencyIndex, field.FieldName);
        }


        _expectationMaximisation.RunEM();

        _expectationMaximisation.GetPatterns();
    }
    private Task GetSampleForDedupe(double maxPairs = 1e6)
    {
        var collectionName = GuidCollectionNameConverter.ToValidCollectionName(_jobId);
        return _dataStore.SampleAndStoreTempData(collectionName, _sampleRecordCollection, maxPairs);
    }

    // Execute Fellegi Sunter
    // Blocking this would be injected from already implemented
    // Create block using blocking and then create pairs
    public async IAsyncEnumerable<MatchResult> FindMatchesAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        CancellationToken cancellationToken = default)
    {
        var exactCriteria = _fields
            .Where(c => c.MatchingType == MatchingType.Exact && c.DataType != CriteriaDataType.Phonetic)
            .ToList();

        if (!exactCriteria.Any())
        {
            throw new Exception("Provide  atleast one exact criteria");
        }

        _ = ProcessRecordsAsync(
                records,
                exactCriteria,
                cancellationToken);

        await foreach (var matchResult in _matchChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return matchResult;
        };
        
    }

    private async Task ProcessRecordsAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        List<ProbabilisticMatchCriteria> exactCriteria,
        CancellationToken cancellationToken)
    {
        try
        {
            if (exactCriteria.Any())
            {
                await ProcessBlockedRecordsAsync(
                    records,
                    exactCriteria,
                    cancellationToken);
            }

        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Processing cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing records");
        }
        finally
        {
            _matchChannel.Writer.Complete();
        }
    }
    public async Task ProcessBlockedRecordsAsync(IAsyncEnumerable<IDictionary<string, object>> records,
        List<ProbabilisticMatchCriteria> exactCriteria,
        CancellationToken cancellationToken = default)
    {
        var blockProcessingTasks = new List<Task>();
        var processedRecords = 0;

        try
        {
            var blockedRecords = await _blockingStrategy.BlockRecordsAsync(
                records,
                exactCriteria.Select(c => c.FieldName),
                cancellationToken);

            await foreach (var block in blockedRecords.WithCancellation(cancellationToken))
            {
                if (block.Count() > 1)
                {
                    await _semaphore.WaitAsync(cancellationToken);

                    blockProcessingTasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessBlockAsync(
                                block,
                                cancellationToken);

                            Interlocked.Add(ref processedRecords, block.Count());
                        }
                        finally
                        {
                            _semaphore.Release();
                        }
                    }, cancellationToken));
                }
                else
                {
                    Interlocked.Increment(ref processedRecords);
                    _telemetry.RecordProcessed();
                }

                if (processedRecords % 10000 == 0)
                {
                    _logger.LogInformation("Processed {Count} records", processedRecords);
                }
            }

            await Task.WhenAll(blockProcessingTasks);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error in blocked processing");
            throw;
        }
    }
    private async Task ProcessBlockAsync(
        IGrouping<string, IDictionary<string, object>> block,
        //Dictionary<string, double> qGramCriteria, IStepProgressTracker progressTracker,
        //IStepProgressTracker progressTracker1,
        CancellationToken cancellationToken)
    {
        var blockList = block.ToList();
        await foreach (var pair in _simpleRecordPairer.GeneratePairsAsync(blockList.ToAsyncEnumerable(), cancellationToken))
        {
            // here we need to perform comparision and populate data
            var matchResult = PerformMatching(pair.Item1, pair.Item2);
            await _matchChannel.Writer.WriteAsync(matchResult, cancellationToken);
            _telemetry.MatchFound();
            _telemetry.RecordProcessed();
        }
    }
    public MatchResult PerformMatching(
    IDictionary<string, object> record1,
    IDictionary<string, object> record2)
    {
        double compositeWeight = 0;
        var fieldScores = new Dictionary<string, FieldScore>();

        foreach (var field in _filterFields)
        {
            var value1 = record1[field.FieldName]?.ToString() ?? "";
            var value2 = record2[field.FieldName]?.ToString() ?? "";

            // Calculate similarity
            double similarity = field.Comparator.Compare(value1, value2);
            var level = field.Settings.GetLevel(similarity);
            var maxWeight = field.Settings.Levels.First(field => field.Name == "Exact").Weight;
            var minWeight = field.Settings.Levels.First(field => field.Name == "Low").Weight;
            var delta = maxWeight - Math.Abs(minWeight);

            // Pure Fellegi-Sunter weight calculation
            //double weight_agree = Math.Log2(level.M_Probability / level.U_Probability);
            //double weight_disagree = Math.Log2((1 - level.M_Probability) / (1 - level.U_Probability));
            double weight = level.Weight;

            if (level.Name == "High")
            {
                var weightDifference = (maxWeight - level.Weight) / ((maxWeight + level.Weight) / 2);
                if(weightDifference < (1 - level.Threshold))
                    weight = (weight - (weight * (1 - level.Threshold))) * similarity;
            }
            else if (level.Name == "Medium")
                weight = weight - (delta * 0.9 * (1 - similarity));
            else if (level.Name == "Low")
                weight = weight - (delta * 0.9);

            fieldScores[field.FieldName] = new FieldScore
            {
                Similarity = similarity,
                Weight = weight,
                Level = level.Name,
                M = level.M_Probability,
                U = level.U_Probability
                //ContributionToTotal = weight / (compositeWeight + double.Epsilon)
            };

            compositeWeight += weight;
        }

        // Convert weight to probability using logistic function
        // The division by field count helps normalize across different numbers of fields
        double normalizedWeight = compositeWeight / _filterFields.Count;
        double matchProbability = 1.0 / (1.0 + Math.Exp(-normalizedWeight));

        return new MatchResult
        {
            CompositeWeight = compositeWeight,
            FieldScores = fieldScores,
            MatchProbability = matchProbability,
            Record1 = record1,
            Record2 = record2
        };
    }
    //public MatchResult PerformMatching(
    //    IDictionary<string, object> record1,
    //    IDictionary<string, object> record2)
    //{
    //    double compositeWeight = 0;
    //    var fieldScores = new Dictionary<string, FieldScore>();

    //    foreach (var field in _filterFields)
    //    {
    //        var value1 = record1[field.FieldName]?.ToString() ?? "";
    //        var value2 = record2[field.FieldName]?.ToString() ?? "";

    //        // Calculate similarity
    //        double similarity = field.Comparator.Compare(value1, value2, true);
    //        var level = field.Settings.GetLevel(similarity);

    //        // Calculate base weight using m/u probabilities
    //        double baseWeight = Math.Log(level.M_Probability / level.U_Probability);

    //        // Adjust weight based on actual similarity score
    //        double adjustedWeight;
    //        if (similarity >= 1.0) // Exact match
    //        {
    //            adjustedWeight = 10.0; // Maximum weight for exact matches
    //        }
    //        else if (similarity >= 0.9) // High similarity
    //        {
    //            // Scale between 5-8 based on actual similarity
    //            adjustedWeight = 5.0 + ((similarity - 0.9) * 30.0);
    //        }
    //        else if (similarity >= 0.7) // Medium similarity
    //        {
    //            // Scale between 0-5 based on actual similarity
    //            adjustedWeight = ((similarity - 0.7) * 25.0);
    //        }
    //        else // Low similarity
    //        {
    //            // Negative weight scaled by dissimilarity
    //            adjustedWeight = -10.0 * (1.0 - similarity);
    //        }

    //        // Apply term specificity for significant matches
    //        //double termAdjustment = 0;
    //        //if (similarity >= 0.8 && field.Statistics?.TermSpecificity > 0)
    //        //{
    //        //    termAdjustment = Math.Min(1.0, field.Statistics.TermSpecificity);
    //        //    adjustedWeight *= (1 + termAdjustment);
    //        //}

    //        fieldScores[field.FieldName] = new FieldScore
    //        {
    //            Similarity = similarity,
    //            Weight = adjustedWeight,
    //            Level = level.Name,
    //            //ContributionToTotal = termAdjustment
    //        };

    //        compositeWeight += adjustedWeight;
    //    }

    //    // Calculate probability using scaled logistic function
    //    double matchProbability;
    //    //if (compositeWeight >= 8.0)
    //    //{
    //    //    matchProbability = 1.0;
    //    //}
    //    //else if (compositeWeight <= -8.0)
    //    //{
    //    //    matchProbability = 0.0;
    //    //}
    //    //else
    //    //{
    //        // Use steeper sigmoid for middle range
    //        matchProbability = 1.0 / (1.0 + Math.Exp(-compositeWeight));
    //        // Round to 3 decimal places for cleaner output
    //        matchProbability = Math.Round(matchProbability, 3);
    //    //}

    //    return new MatchResult
    //    {
    //        CompositeWeight = Math.Round(compositeWeight, 4),
    //        FieldScores = fieldScores,
    //        MatchProbability = matchProbability,
    //        RecordId1 = record1,
    //        RecordId2 = record2
    //    };
    //}
    
    #endregion
}

public class ProbabilisticOption: RecordLinkageOptions
{
    public int DecimalPlaces { get; set; }
}

public static class AsyncExcelWriter
{
    private static readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1, 1);
    private static bool isFirstWrite = true;

    public static async Task WriteToExcelWithBatchingAsync(
        List<MatchResult> dataStream,
        List<ProbabilisticMatchCriteria> fields, string filePath,
        int batchSize = 1000)
    {
        if (!dataStream.Any()) return;

        await _writeSemaphore.WaitAsync();
        try
        {
            // For the first write, create a new file
            using var writer = new StreamWriter(filePath, append: !isFirstWrite);
            using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            if (isFirstWrite)
            {
                WriteHeaders(csv,fields, dataStream[0]);
                await csv.NextRecordAsync();
                isFirstWrite = false;
            }

            // Process each record in the batch
            foreach (var item in dataStream)
            {
                // Write RecordId1 fields
                foreach (var kv in item.Record1)
                {                    
                    if (fields.Where(x => x.FieldName == kv.Key).Count() > 0)
                    csv.WriteField(kv.Value?.ToString() ?? "");
                }

                // Write RecordId2 fields
                foreach (var kv in item.Record2)
                {
                    if (fields.Where(x => x.FieldName == kv.Key).Count() > 0)
                        csv.WriteField(kv.Value?.ToString() ?? "");
                }

                // Write MatchProbability
                csv.WriteField(Math.Round(item.MatchProbability, 6));

                // Write CompositeWeight
                csv.WriteField(Math.Round(item.CompositeWeight, 6));

                // Write FieldScores
                foreach (var kv in item.FieldScores)
                {
                    csv.WriteField(Math.Round(kv.Value.Similarity,6));
                    csv.WriteField(Math.Round(kv.Value.Weight,6));
                    csv.WriteField(kv.Value.Level);
                    //csv.WriteField(kv.Value.ContributionToTotal);
                }

                await csv.NextRecordAsync();
            }

            await csv.FlushAsync();
            await writer.FlushAsync();
        }
        finally
        {
            _writeSemaphore.Release();
            dataStream.Clear();
        }
    }

    private static void WriteHeaders(CsvWriter csv,List<ProbabilisticMatchCriteria> fields, MatchResult sampleRecord)
    {
        // Delete the file if it exists to ensure headers are at the top
        // Write RecordId1 headers
        foreach (var key in sampleRecord.Record1.Keys)
        {
            if (fields.Where(x => x.FieldName == key).Count() > 0)
                csv.WriteField($"Record1_{key}");
        }

        // Write RecordId2 headers
        foreach (var key in sampleRecord.Record2.Keys)
        {
            if (fields.Where(x => x.FieldName == key).Count() > 0)
                csv.WriteField($"Record2_{key}");
        }

        // Write MatchProbability header
        csv.WriteField("Match_Probability");

        // Write CompositeWeight header
        csv.WriteField("Composite_Weight");

        // Write FieldScores headers
        foreach (var key in sampleRecord.FieldScores.Keys)
        {
            csv.WriteField($"{key}_Similarity");
            csv.WriteField($"{key}_Weight");
            csv.WriteField($"{key}_Level");
            //csv.WriteField($"{key}_Contribution");
        }
    }

    // Add a method to reset the writer state if needed
    public static void Reset()
    {
        isFirstWrite = true;
    }
}