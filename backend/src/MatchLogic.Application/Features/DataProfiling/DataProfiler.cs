using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Interfaces.Dictionary;
using MatchLogic.Application.Interfaces.Regex;
using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Project;

namespace MatchLogic.Application.Features.DataProfiling
{
    public class DataProfiler : IDataProfiler, IAsyncDisposable
    {
        private readonly ILogger<DataProfiler> _logger;
        private readonly ProfilingOptions _options;
        private readonly IRegexInfoService _regexService;
        private readonly IDictionaryCategoryService _dictionaryService;
        private readonly SemaphoreSlim _semaphore;
        private readonly ArrayPool<char> _charPool;
        private const string MetadataField = "_metadata";
        private const string IdField = "_id";
        private const string RowNumberField = "RowNumber";
        private bool _disposed;
        private readonly IProfileRepository _profileRepository;
        private string[] SystemColumns = new string[]{IdField, MetadataField };        
        public DataProfiler(
            ILogger<DataProfiler> logger,
            IOptions<ProfilingOptions> options,
            IRegexInfoService regexService,
            IDictionaryCategoryService dictionaryService,
            IProfileRepository profileRepository)
        {
            _logger = logger;
            _options = options.Value;
            _regexService = regexService;
            _dictionaryService = dictionaryService;
            _semaphore = new SemaphoreSlim(_options.MaxDegreeOfParallelism);
            _profileRepository = profileRepository;
            _charPool = ArrayPool<char>.Shared;            
        }

        public async Task<ProfileResult> ProfileDataAsync(
            IAsyncEnumerable<IDictionary<string, object>> dataStream,
            DataSource dataSource = null,
            IEnumerable<string> columnsToProfile = null, 
            ICommandContext commandContext = null,
            string collectionName = null,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var startTime = DateTime.UtcNow;
            var profileResult = new ProfileResult
            {
                ProfiledAt = startTime,
                DataSourceName = dataSource.Name,
                DataSourceId = dataSource.Id,
                ColumnProfiles = new ConcurrentDictionary<string, ColumnProfile>()
            };

            // Get all regex patterns from the service
            var regexPatterns = await _regexService.GetAllRegexInfo();
            var activeRegexes = regexPatterns
                .Where(r => !r.IsDeleted && r.IsDefault)
                .Select(r => (Id: r.Id, Name: r.Name, Pattern: new System.Text.RegularExpressions.Regex(r.RegexExpression, RegexOptions.Compiled)))
                .ToList();

            // Get all dictionary categories
            var dictionaryCategories = await _dictionaryService.GetAllDictionaryCategories();
            var activeDictionaries = dictionaryCategories
                .Where(d => !d.IsDeleted && d.IsDefault)
                .Select(d => (Id: d.Id, Name: d.Name, Items: new HashSet<string>(d.Items, StringComparer.OrdinalIgnoreCase)))
                .ToList();

            // Create field analyzers for each column
            var fieldAnalyzers = new ConcurrentDictionary<string, ColumnAnalyzer>();
            var processedRecords = 0;
            var batchProcess = 0;
            var columnsToProfileSet = columnsToProfile?.ToHashSet(StringComparer.OrdinalIgnoreCase);

            try
            {
                _logger.LogInformation("Starting comprehensive data profiling");

                // Initialize the channel for parallel processing
                var channel = Channel.CreateBounded<(IDictionary<string, object> Record, long RowNumber)>(
                    new BoundedChannelOptions(_options.BufferSize)
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
                                    ProcessRecord(item.Record, item.RowNumber, fieldAnalyzers, activeRegexes, activeDictionaries, columnsToProfileSet, commandContext);
                                    Interlocked.Increment(ref processedRecords);                                    
                                    if (processedRecords % _options.BatchSize == 0)
                                    {
                                        Interlocked.Increment(ref batchProcess);
                                        _logger.LogInformation("Profiled {Count} records", processedRecords);                                        
                                    }
                                }
                                finally
                                {
                                    _semaphore.Release();
                                }
                            }));

                            // Periodically clean up completed tasks
                            if (tasks.Count > _options.MaxDegreeOfParallelism * 2)
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
                });

                // Feed data into the channel with row numbers
                long rowNumber = 0;
                await foreach (var record in dataStream.WithCancellation(cancellationToken))
                {
                    rowNumber = Convert.ToInt64((record[MetadataField] as Dictionary<string, object>)?[RowNumberField]);
                    await channel.Writer.WriteAsync((record, rowNumber), cancellationToken);
                }

                // Signal that all data has been fed
                channel.Writer.Complete();

                // Wait for processing to complete
                await processingTask;

                // Collections for row references
                var characteristicRowsByColumn = new Dictionary<string, Dictionary<ProfileCharacteristic, List<RowReference>>>();
                var patternRowsByColumn = new Dictionary<string, Dictionary<string, List<RowReference>>>();
                var valueRowsByColumn = new Dictionary<string, Dictionary<string, List<RowReference>>>();

                // Build column profiles
                foreach (var analyzer in fieldAnalyzers)
                {
                    var columnName = analyzer.Key;

                    // Convert row references
                    var characteristicRows = ExtractCharacteristicRows(analyzer.Value);
                    var patternRows = ExtractPatternRows(analyzer.Value);
                    var valueRows = ExtractValueRows(analyzer.Value);

                    // Store for saving
                    characteristicRowsByColumn[columnName] = characteristicRows;
                    patternRowsByColumn[columnName] = patternRows;
                    valueRowsByColumn[columnName] = valueRows;

                    // Build column profile
                    profileResult.ColumnProfiles[columnName] = analyzer.Value.BuildColumnProfile();
                }

                // Set total record count
                profileResult.TotalRecords = processedRecords;
                profileResult.ProfilingDuration = DateTime.UtcNow - startTime;

                commandContext.Statistics.RecordsProcessed = processedRecords;
                commandContext.Statistics.BatchesProcessed = batchProcess;

                // Save profile result with row references
                await _profileRepository.SaveProfileResultAsync(
                    profileResult,
                    characteristicRowsByColumn,
                    patternRowsByColumn,
                    valueRowsByColumn, collectionName);
               
                _logger.LogInformation("Completed profiling of {Count} records in {Duration}",
                    processedRecords, profileResult.ProfilingDuration);

                return profileResult;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in data profiling");
                throw;
            }
        }

        private void ProcessRecord(
            IDictionary<string, object> record,
            long rowNumber,
            ConcurrentDictionary<string, ColumnAnalyzer> columnAnalyzers,
            List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> regexPatterns,
            List<(Guid Id, string Name, HashSet<string> Items)> dictionaries,
            HashSet<string> columnsToProfileSet,
            ICommandContext commandContext)
        {
            try
            {
                // Process each field in the record
                foreach (var field in record)
                {
                    // Skip fields not in the columns to profile set, if specified
                    if (columnsToProfileSet != null && !columnsToProfileSet.Contains(field.Key) || SystemColumns.Contains(field.Key))
                        continue;

                    // Get or create column analyzer
                    var analyzer = columnAnalyzers.GetOrAdd(field.Key, key =>
                        new ColumnAnalyzer(key, _options, regexPatterns, dictionaries));

                    // Analyze the field value
                    analyzer.Analyze(field.Value, record, rowNumber);
                }
            }
            catch (Exception ex)
            {
                lock (commandContext.Statistics)
                {
                    commandContext.Statistics.RecordError(ex.Message ?? $"Error processing record {rowNumber}");
                }                
                _logger.LogError(ex, "Error processing record {RowNumber}", rowNumber);
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;
            _semaphore.Dispose();

            await Task.CompletedTask;
        }

        /// <summary>
        /// Extracts characteristic rows from a column analyzer
        /// </summary>
        private Dictionary<ProfileCharacteristic, List<RowReference>> ExtractCharacteristicRows(ColumnAnalyzer analyzer)
        {
            var characteristicRows = new Dictionary<ProfileCharacteristic, List<RowReference>>();

            foreach (var characteristicPair in analyzer._characteristicRows)
            {
                characteristicRows[characteristicPair.Key] = characteristicPair.Value
                    .Take(_options.MaxRowsPerCategory)
                    .ToList();
            }

            return characteristicRows;
        }

        /// <summary>
        /// Extracts pattern rows from a column analyzer
        /// </summary>
        private Dictionary<string, List<RowReference>> ExtractPatternRows(ColumnAnalyzer analyzer)
        {
            var patternRows = new Dictionary<string, List<RowReference>>();

            foreach (var patternPair in analyzer._patternStats)
            {
                patternRows[$"{patternPair.Key}_Valid"] = patternPair.Value.ValidRows
                    .Take(_options.MaxRowsPerCategory)
                    .ToList();

                patternRows[$"{patternPair.Key}_Invalid"] = patternPair.Value.InvalidRows
                    .Take(_options.MaxRowsPerCategory)
                    .ToList();
            }

            return patternRows;
        }

        /// <summary>
        /// Extracts value rows from a column analyzer
        /// </summary>
        private Dictionary<string, List<RowReference>> ExtractValueRows(ColumnAnalyzer analyzer)
        {
            var valueRows = new Dictionary<string, List<RowReference>>();

            foreach (var valuePair in analyzer._valueRows)
            {
                valueRows[valuePair.Key] = valuePair.Value
                    .Take(_options.MaxRowsPerCategory)
                    .ToList();
            }

            return valueRows;
        }
    }
}
