using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MatchLogic.Application.Common;
using System.Linq.Expressions;

namespace MatchLogic.Application.Features.Import;
public class OrderedDataImportModule : IImportModule, IDisposable
{
    private const int BatchSize = 1000;
    private readonly IConnectionReaderStrategy _dataReader;
    private readonly IDataStore _dataStore;
    private readonly ILogger _logger;
    private readonly IRecordHasher _recordHasher;
    private bool _disposed;
    private const string MetadataField = "_metadata";
    private readonly IColumnFilter _columnFilter;
    private readonly Dictionary<string, ColumnMapping> _columnMappings;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly ICommandContext _commandContext;
    private readonly Guid? _dataSourceId;
    private readonly long? _maxRowsToImport;
    private DataSource dataSource;

    public OrderedDataImportModule(
        IConnectionReaderStrategy dataReader,
        IDataStore dataStore,
        ILogger logger,
        IRecordHasher recordHasher,
        IJobEventPublisher jobEventPublisher,
        ICommandContext commandContext,
        Dictionary<string, ColumnMapping> columnMappings = null,
        IColumnFilter columnFilter = null,
        Guid? dataSourceId = null,
        long? maxRowsToImport = null)
    {
        _dataReader = dataReader;
        _dataStore = dataStore;
        _logger = logger;
        _recordHasher = recordHasher;
        _columnFilter = columnFilter;
        _columnMappings = columnMappings;
        _jobEventPublisher = jobEventPublisher;
        _commandContext = commandContext;
        _dataSourceId = dataSourceId;
        _maxRowsToImport = maxRowsToImport;
    }

    public async Task<Guid> ImportDataAsync(string collectionName = "", CancellationToken cancellationToken = default)
    {
        string sourceFile = _dataReader.Name;
        long totalNumberRecords = _dataReader.RowCount;
        _logger.LogInformation("Starting data import for file: {SourceFile}", sourceFile);
        Guid jobId = await _dataStore.InitializeJobAsync(collectionName);
        var stepId = _commandContext.StepId;
        _logger.LogDebug("Initialized import job with ID: {JobId} runId :{RunId}", jobId, stepId);

        var readingStep = _jobEventPublisher.CreateStepTracker(stepId, "Reading data", 1, 2);
        var writingStep = _jobEventPublisher.CreateStepTracker(stepId, "Writing data", 2, 2);

        var processingQueue = new BufferBlock<(Dictionary<string, object> Record, long RowNumber)>(
            new DataflowBlockOptions { BoundedCapacity = BatchSize * 4 });

        var writingQueue = new BatchBlock<(Dictionary<string, object>, long)>(
            BatchSize,
            new GroupingDataflowBlockOptions
            {
                BoundedCapacity = BatchSize * 4,
                CancellationToken = cancellationToken
            });

        var batchProcessor = new TransformBlock<(Dictionary<string, object>, long)[], List<Dictionary<string, object>>>(
            batch => {
                var processedBatch = ProcessBatch(batch, sourceFile);

              // Update statistics for the batch
                lock (_commandContext.Statistics)
                {
                    _commandContext.Statistics.BatchesProcessed++;
                    _commandContext.Statistics.RecordsProcessed += processedBatch.Count;
                }

                return processedBatch;
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                CancellationToken = cancellationToken
            });

        var writer = new ActionBlock<List<Dictionary<string, object>>>(
            async records =>
            {
                await _dataStore.InsertBatchAsync(jobId, records, collectionName);
                _logger.LogDebug("Written batch of {Count} records for job {JobId}", records.Count, jobId);
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                CancellationToken = cancellationToken
            });

        try
        {
            using var linkProcessingToWriting = processingQueue.LinkTo(writingQueue,
                new DataflowLinkOptions { PropagateCompletion = true });
            using var linkWritingToBatch = writingQueue.LinkTo(batchProcessor,
                new DataflowLinkOptions { PropagateCompletion = true });
            using var linkBatchToWriter = batchProcessor.LinkTo(writer,
                new DataflowLinkOptions { PropagateCompletion = true });

            // ✅ THE FIX: linked token so we can cancel the reader producer independently
            using var readerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            long rowNumber = -1;
            var rowsEnumerable = await _dataReader.ReadRowsAsync(1, readerCts.Token);
            _logger.LogDebug("Starting to read rows from source file");
            await readingStep.StartStepAsync((int)totalNumberRecords);
            await writingStep.StartStepAsync((int)totalNumberRecords);
            bool truncatedByLimit = false;

            try
            {
                await foreach (Dictionary<string, object> row in rowsEnumerable.WithCancellation(readerCts.Token))
                {
                    if (_maxRowsToImport.HasValue && (rowNumber + 1) >= _maxRowsToImport.Value)
                    {
                        truncatedByLimit = true;
                        _logger.LogWarning(
                            "Trial record limit reached after {RowsRead} rows. " +
                            "Remaining rows in file skipped. Activate a license to import without limits.",
                            rowNumber + 1);
                        readerCts.Cancel(); // stops the blocked producer immediately
                        break;
                    }

                    await processingQueue.SendAsync((row, ++rowNumber), cancellationToken);

                    if (rowNumber % BatchSize == 0)
                        _logger.LogDebug("Processed {RowCount} rows from source file", rowNumber);
                    //await readingStep.UpdateProgressAsync((int)rowNumber);
                }
            }
            catch (OperationCanceledException) when (truncatedByLimit)
            {
                // We cancelled the reader ourselves — expected, swallow and continue
            }

            var actualRowsRead = rowNumber + 1;
            _logger.LogInformation(
                truncatedByLimit
                    ? "Import truncated at {TotalRows} rows (trial limit reached)"
                    : "Completed reading {TotalRows} rows from source file",
                actualRowsRead);
            //_dataReader.ErrorMessage

            await readingStep.CompleteStepAsync();
            await writingStep.CompleteStepAsync();
            // Fetch DataSource for updating statistics, error messages and field data types
            if (_dataSourceId.HasValue)
                dataSource = await _dataStore.GetByIdAsync<DataSource, Guid>(_dataSourceId.Value, Constants.Collections.DataSources);
            // Add Field Data Types  
            await WriteFieldDataTypeAsync("");
            if (_dataReader.ErrorMessage.Count != 0)
            {
                _logger.LogWarning("Errors encountered during reading: {Errors}", string.Join(", ", _dataReader.ErrorMessage));
                await UpdateErrorMessage(_dataReader.ErrorMessage.ToArray());
            }
            //Update total records and columns in DataSource
            // Use actualRowsRead (not totalNumberRecords) because import may have been
            // truncated by the trial record limit — only count rows actually written.
            await UpdateDataSourceStatistics(actualRowsRead,
                ColumMappingHelper.GetImportedHeaders(_dataReader, _columnMappings).Count());

            processingQueue.Complete();
            await writer.Completion;
            _logger.LogInformation("Successfully completed import job {JobId} with {TotalRows} rows", jobId, rowNumber);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Import operation cancelled for job {JobId}", jobId);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during import for job {JobId}", jobId);
            throw;
        }

        return jobId;
    }


    private List<Dictionary<string, object>> ProcessBatch(
        (Dictionary<string, object> Record, long RowNumber)[] batch,
        string sourceFile)
    {
        try
        {
            return batch
                .OrderBy(item => item.RowNumber)
                .Select(item =>
                {
                    IDictionary<string, object> filteredRecord = item.Record;
                    if (_columnMappings != null)
                    {
                        filteredRecord = _columnFilter.FilterColumns(item.Record, _columnMappings);
                    }

                    var enrichedRecord = new Dictionary<string, object>(filteredRecord)
                    {
                        [MetadataField] = new RecordMetadata
                        {
                            RowNumber = item.RowNumber,
                            Hash = _recordHasher.ComputeHash(filteredRecord),
                            SourceFile = sourceFile
                        }
                    };
                    return enrichedRecord;
                })
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing batch starting with row {StartRow}",
                batch.FirstOrDefault().RowNumber);
            throw;
        }
    }
    private async Task UpdateDataSourceStatistics(long rowsCount, long NumberOfColumns)
    {
        if (_dataSourceId == null)
        {
            _logger.LogWarning("Data source ID is null, cannot update DataSource Statistics during import:  rowsCount = {rowsCount} || {NumberOfColumns}", rowsCount, NumberOfColumns);
            return;
        }

        if (dataSource == null)
        {
            _logger.LogWarning("Data source with ID {DataSourceId} not found for updating Statistics", _dataSourceId);
            return;
        }
        dataSource.RecordCount = rowsCount;
        dataSource.ColumnsCount = NumberOfColumns;
        // Update the data source with new error messages
        await _dataStore.UpdateAsync(dataSource, Constants.Collections.DataSources);
    }
    private async Task UpdateErrorMessage(string[] errorMessages)
    {
        if (_dataSourceId == null)
        {
            _logger.LogWarning("Data source ID is null, cannot update error messages during import: {Errors}", string.Join(", ", errorMessages));
            return;
        }

        if (dataSource == null)
        {
            _logger.LogWarning("Data source with ID {DataSourceId} not found for updating error messages", _dataSourceId);
            return;
        }
        // Update Error Messages
        dataSource.ErrorMessages = errorMessages;
        // Update the data source with new error messages
        await _dataStore.UpdateAsync(dataSource, Constants.Collections.DataSources);
        _logger.LogWarning("Updated error messages for data source {DataSourceId}: {Errors}", _dataSourceId, string.Join(", ", errorMessages));
    }
    private async Task WriteFieldDataTypeAsync(string tableName)
    {
        if (_dataSourceId == null)
        {
            _logger.LogWarning("Data source ID is null, cannot WriteFieldDataTypeAsync during import");
            return;
        }
        if (dataSource == null)
        {
            _logger.LogWarning("Data source with ID {DataSourceId} not found for updating error messages", _dataSourceId);
            return;
        }

        var tableSchema = await _dataReader.GetTableSchema(tableName);
        var importedHeaders = new HashSet<string>(
            ColumMappingHelper.GetImportedHeaders(_dataReader, _columnMappings),
            StringComparer.OrdinalIgnoreCase);

        List<FieldMappingEx> fieldMappings = new();
        int fieldIndex = 0;

        foreach (var column in tableSchema.Columns)
        {
            // Skip columns excluded by mapping
            if (!importedHeaders.Contains(column.Name))
                continue;

            // Resolve target column name from mapping (may be renamed)
            var targetName = _columnMappings != null &&
                             _columnMappings.TryGetValue(column.Name, out var mapping)
                                ? mapping.TargetColumn
                                : column.Name;

            fieldMappings.Add(new FieldMappingEx
            {
                FieldIndex = fieldIndex++,
                FieldName = targetName,       // use mapped/renamed target name
                DataSourceId = dataSource.Id,
                DataSourceName = dataSource.Name,
                DataType = column.DataType,
                Length = column.Length,
                Origin = FieldOrigin.Import,
                IsSystemManaged = false,
                IsActive = true,
                CreatedAt = DateTime.UtcNow
            });
        }

        var existing = await _dataStore.QueryAsync<FieldMappingEx>(
            x => x.DataSourceId == dataSource.Id,
            Constants.Collections.FieldMapping);

        if (existing == null || existing.Count == 0)
            await _dataStore.BulkInsertAsync<FieldMappingEx>(fieldMappings, Constants.Collections.FieldMapping);

    }
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                if (_dataStore is IDisposable disposableStore)
                {
                    disposableStore.Dispose();
                }
                if (_dataReader is IDisposable disposableReader)
                {
                    disposableReader.Dispose();
                }
                if (_recordHasher is IDisposable disposableHasher)
                {
                    disposableHasher.Dispose();
                }
            }
            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~OrderedDataImportModule()
    {
        Dispose(false);
    }
}
