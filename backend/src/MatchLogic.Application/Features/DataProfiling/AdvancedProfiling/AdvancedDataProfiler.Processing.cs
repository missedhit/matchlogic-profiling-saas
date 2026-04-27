using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;
using System.Reactive.Subjects;
using System.Reactive.Linq;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public partial class AdvancedDataProfiler
    {
        /// <summary>
        /// Process data using Channels for parallel processing
        /// </summary>
        private async Task ProcessDataWithChannels(
            IAsyncEnumerable<IDictionary<string, object>> dataStream,
            ConcurrentDictionary<string, AdvancedColumnAnalyzer> fieldAnalyzers,
            List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> regexPatterns,
            List<(Guid Id, string Name, HashSet<string> Items)> dictionaries,
            HashSet<string> columnsToProfileSet,
            List<IDictionary<string, object>> rawData,
            AdvancedProfilingOptions options,
            CancellationToken cancellationToken)
        {
            var processedRecords = 0;

            // Initialize the channel for parallel processing
            var channel = Channel.CreateBounded<(IDictionary<string, object> Record, long RowNumber)>(
                new BoundedChannelOptions(options.BufferSize)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleWriter = true,
                    SingleReader = false
                });

            // Start processing task
            var processingTask = Task.Run(async () =>
            {
                try
                {
                    var tasks = new List<Task>();

                    await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken))
                    {
                        await _semaphore.WaitAsync(cancellationToken);

                        tasks.Add(Task.Run(() =>
                        {
                            try
                            {
                                ProcessRecord(
                                    item.Record,
                                    item.RowNumber,
                                    fieldAnalyzers,
                                    regexPatterns,
                                    dictionaries,
                                    columnsToProfileSet,
                                    options);

                                Interlocked.Increment(ref processedRecords);

                                if (processedRecords % options.BatchSize == 0)
                                {
                                    _logger.LogInformation("Profiled {Count} records", processedRecords);
                                }
                            }
                            finally
                            {
                                _semaphore.Release();
                            }
                        }, cancellationToken));

                        // Periodically clean up completed tasks
                        if (tasks.Count > options.MaxDegreeOfParallelism * 2)
                        {
                            var completedTasks = tasks.Where(t => t.IsCompleted).ToList();
                            foreach (var task in completedTasks)
                            {
                                tasks.Remove(task);
                                if (task.IsFaulted && task.Exception != null)
                                {
                                    _logger.LogError(task.Exception.InnerException, "Error processing record");
                                }
                            }
                        }
                    }

                    // Wait for all remaining tasks to complete
                    await Task.WhenAll(tasks);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Profiling operation cancelled");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing records for profiling");
                }
            }, cancellationToken);

            // Feed data into the channel with row numbers
            long rowNumber = 0;
            await foreach (var record in dataStream.WithCancellation(cancellationToken))
            {
                if (record.ContainsKey(MetadataField) && record[MetadataField] is Dictionary<string, object> metadata)
                {
                    if (metadata.TryGetValue(RowNumberField, out var rowNum))
                    {
                        rowNumber = Convert.ToInt64(rowNum);
                    }
                    else
                    {
                        rowNumber++;
                    }
                }
                else
                {
                    rowNumber++;
                }

                // Store raw data if needed for row-level analysis
                if (rawData != null)
                {
                    rawData.Add(record);
                }

                await channel.Writer.WriteAsync((record, rowNumber), cancellationToken);
            }

            // Signal that all data has been fed
            channel.Writer.Complete();

            // Wait for processing to complete
            await processingTask;
        }

        /// <summary>
        /// Process data using Reactive Extensions for streaming processing
        /// </summary>
        private async Task ProcessDataWithReactiveExtensions(
            IAsyncEnumerable<IDictionary<string, object>> dataStream,
            ConcurrentDictionary<string, AdvancedColumnAnalyzer> fieldAnalyzers,
            List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> regexPatterns,
            List<(Guid Id, string Name, HashSet<string> Items)> dictionaries,
            HashSet<string> columnsToProfileSet,
            List<IDictionary<string, object>> rawData,
            AdvancedProfilingOptions options,
            CancellationToken cancellationToken)
        {
            var processedRecords = 0;
            var subject = new Subject<(IDictionary<string, object> Record, long RowNumber)>();

            // Create an observable from the subject
            var observable = subject.AsObservable()
                .Buffer(options.BatchSize)
                .Select(batch => Observable.FromAsync(async () =>
                {
                    await ProcessBatchAsync(
                        batch,
                        fieldAnalyzers,
                        regexPatterns,
                        dictionaries,
                        columnsToProfileSet,
                        options);

                    Interlocked.Add(ref processedRecords, batch.Count);

                    _logger.LogInformation("Profiled {Count} records", processedRecords);

                    return batch.Count;
                }))
                .Merge(options.MaxDegreeOfParallelism);

            // Subscribe to the observable
            using var subscription = observable.Subscribe(
                count => { }, // OnNext - already logged in the processing
                ex => _logger.LogError(ex, "Error in reactive processing"),
                () => _logger.LogInformation("Completed reactive processing")
            );

            // Feed data into the subject
            long rowNumber = 0;
            await foreach (var record in dataStream.WithCancellation(cancellationToken))
            {
                if (record.ContainsKey(MetadataField) && record[MetadataField] is Dictionary<string, object> metadata)
                {
                    if (metadata.TryGetValue(RowNumberField, out var rowNum))
                    {
                        rowNumber = Convert.ToInt64(rowNum);
                    }
                    else
                    {
                        rowNumber++;
                    }
                }
                else
                {
                    rowNumber++;
                }

                // Store raw data if needed for row-level analysis
                if (rawData != null)
                {
                    rawData.Add(record);
                }

                subject.OnNext((record, rowNumber));
            }

            // Signal completion
            subject.OnCompleted();

            // Wait for all processing to complete
            await Task.Delay(100, cancellationToken); // Small delay to ensure all processing completes
        }

        /// <summary>
        /// Process a batch of records asynchronously
        /// </summary>
        private async Task ProcessBatchAsync(
            IEnumerable<(IDictionary<string, object> Record, long RowNumber)> batch,
            ConcurrentDictionary<string, AdvancedColumnAnalyzer> fieldAnalyzers,
            List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> regexPatterns,
            List<(Guid Id, string Name, HashSet<string> Items)> dictionaries,
            HashSet<string> columnsToProfileSet,
            AdvancedProfilingOptions options)
        {
            await Task.Run(() =>
            {
                Parallel.ForEach(
                    batch,
                    new ParallelOptions { MaxDegreeOfParallelism = options.MaxDegreeOfParallelism },
                    item => ProcessRecord(
                        item.Record,
                        item.RowNumber,
                        fieldAnalyzers,
                        regexPatterns,
                        dictionaries,
                        columnsToProfileSet,
                        options));
            });
        }

        /// <summary>
        /// Process a single record 
        /// </summary>
        private void ProcessRecord(
            IDictionary<string, object> record,
            long rowNumber,
            ConcurrentDictionary<string, AdvancedColumnAnalyzer> columnAnalyzers,
            List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> regexPatterns,
            List<(Guid Id, string Name, HashSet<string> Items)> dictionaries,
            HashSet<string> columnsToProfileSet,
            AdvancedProfilingOptions options)
        {
            try
            {
                // Process each field in the record
                foreach (var field in record)
                {
                    // Skip system fields
                    if (SystemColumns.Contains(field.Key))
                        continue;

                    // Skip fields not in the columns to profile set, if specified
                    if (columnsToProfileSet != null && !columnsToProfileSet.Contains(field.Key))
                        continue;

                    // Get or create column analyzer
                    var analyzer = columnAnalyzers.GetOrAdd(field.Key, key =>
                        new AdvancedColumnAnalyzer(key, options, regexPatterns, dictionaries));

                    // Use SIMD optimizations if enabled
                    if (options.EnableSimdOptimizations && field.Value is string strValue && strValue.Length > 16)
                    {
                        analyzer.AnalyzeWithSimd(strValue, record, rowNumber);
                    }
                    else
                    {
                        // Standard analysis
                        analyzer.Analyze(field.Value, record, rowNumber);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing record {RowNumber}", rowNumber);
            }
        }
        
    }
}
