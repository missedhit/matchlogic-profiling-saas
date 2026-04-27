using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Import;
public class DataImportModule : IImportModule
{
    private const int BatchSize = 1000;
    private const int MaxDegreeOfParallelism = 4;
    private readonly IConnectionReaderStrategy _dataReader;
    private readonly IDataStore _dataStore;
    private readonly ILogger _logger;
    private readonly IColumnFilter _columnFilter;
    private readonly Dictionary<string, ColumnMapping> _columnMappings;
    private readonly Guid? _dataSourceId;
    private readonly long? _maxRowsToImport;
    private long _actualRowsImported;
    private DataSource dataSource;
    public DataImportModule(IConnectionReaderStrategy dataReader, IDataStore dataStore, ILogger logger
        , Dictionary<string, ColumnMapping> columnMappings = null,
        IColumnFilter columnFilter = null, Guid? dataSourceId = null,
        long? maxRowsToImport = null)
    {
        _dataReader = dataReader;
        _dataStore = dataStore;
        _logger = logger;
        _columnFilter = columnFilter;
        _columnMappings = columnMappings;
        _dataSourceId = dataSourceId;
        _maxRowsToImport = maxRowsToImport;
    }

    public async Task<Guid> ImportDataAsync(string collectionName = "", CancellationToken cancellationToken = default)
    {
        Guid jobId = Guid.Empty;
        try
        {
            _logger.LogInformation("Starting data import process");
            jobId = await _dataStore.InitializeJobAsync(collectionName);
            _logger.LogInformation("Initialized import job with ID: {JobId}", jobId);

            var dataQueue = new BlockingCollection<List<IDictionary<string, object>>>(MaxDegreeOfParallelism * 2);
            var readerTask = Task.Run(() => ReadDataFromSourceAsync(dataQueue, cancellationToken), cancellationToken);

            var writerTasks = Enumerable.Range(0, MaxDegreeOfParallelism)
                .Select(_ => Task.Run(() => WriteDataToDestinationAsync(jobId, dataQueue, cancellationToken), cancellationToken))
                .ToArray();

            await Task.WhenAll(readerTask.ContinueWith(_ => dataQueue.CompleteAdding()));
            await Task.WhenAll(writerTasks);
            if (_dataSourceId.HasValue)
                dataSource = await _dataStore.GetByIdAsync<DataSource, Guid>(_dataSourceId.Value, Constants.Collections.DataSources);
            //Check for any Error Messages during Import
            if (_dataReader.ErrorMessage.Count != 0)
            {
                _logger.LogWarning("Errors encountered during reading: {Errors}", string.Join(", ", _dataReader.ErrorMessage));
                await UpdateErrorMessage(_dataReader.ErrorMessage.ToArray());
            }

            await WriteFieldDataTypeAsync("");
            //Update total records and columns in DataSource
            // Use _actualRowsImported (not _dataReader.RowCount) because import may have been
            // truncated by the trial record limit — only count rows actually written.
            await UpdateDataSourceStatistics(_actualRowsImported,
                ColumMappingHelper.GetImportedHeaders(_dataReader, _columnMappings).Count());

            _logger.LogInformation("Data import process completed successfully for job {JobId}", jobId);
            return jobId;
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Data import process was cancelled for job {JobId}", jobId);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during data import for job {JobId}", jobId);
            throw;
        }
    }

    private async Task ReadDataFromSourceAsync(BlockingCollection<List<IDictionary<string, object>>> dataQueue, CancellationToken cancellationToken)
    {
        using var readerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bool truncatedByLimit = false;

        try
        {
            var batch = new List<IDictionary<string, object>>();
            var rowsEnumerable = await _dataReader.ReadRowsAsync(MaxDegreeOfParallelism, readerCts.Token);
            long rowsRead = 0;

            try
            {
                await foreach (var row in rowsEnumerable.WithCancellation(readerCts.Token))
                {
                    if (_maxRowsToImport.HasValue && rowsRead >= _maxRowsToImport.Value)
                    {
                        truncatedByLimit = true;
                        _logger.LogWarning(
                            "Trial record limit reached after {RowsRead} rows. " +
                            "Remaining rows in file skipped. Activate a license to import without limits.",
                            rowsRead);
                        readerCts.Cancel();
                        break;
                    }

                    var filteredRow = row;
                    if (_columnMappings != null)
                        filteredRow = _columnFilter.FilterColumns(row, _columnMappings);

                    batch.Add(filteredRow);
                    rowsRead++;
                    _actualRowsImported = rowsRead;

                    if (batch.Count >= BatchSize)
                    {
                        dataQueue.Add(batch, readerCts.Token); // readerCts so it unblocks on limit cancel
                        batch = new List<IDictionary<string, object>>();
                        _logger.LogDebug("Added batch of {Count} records to queue", BatchSize);
                    }
                }
            }
            catch (OperationCanceledException) when (truncatedByLimit)
            {
                // We cancelled the reader ourselves — expected, swallow and continue
            }

            // readerCts may be cancelled here, use original token for final flush
            if (batch.Count > 0)
            {
                dataQueue.Add(batch, cancellationToken);
                _logger.LogDebug("Added final batch of {Count} records to queue", batch.Count);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error occurred while reading data from source");
            throw;
        }
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
            if (!importedHeaders.Contains(column.Name))
                continue;

            var targetName = _columnMappings != null &&
                             _columnMappings.TryGetValue(column.Name, out var mapping)
                                ? mapping.TargetColumn
                                : column.Name;

            _logger.LogInformation("Column: {ColumnName}, DataType: {DataType}", targetName, column.DataType);

            fieldMappings.Add(new FieldMappingEx
            {
                FieldIndex = fieldIndex++,
                FieldName = targetName,
                DataSourceId = dataSource.Id,
                DataSourceName = dataSource.Name,
                DataType = column.DataType,
                Length = column.Length,
                Ordinal = column.Ordinal,
            });
        }

        await _dataStore.BulkInsertAsync<FieldMappingEx>(fieldMappings, Constants.Collections.FieldMapping);
    }
    
    private async Task WriteDataToDestinationAsync(Guid jobId, BlockingCollection<List<IDictionary<string, object>>> dataQueue, CancellationToken cancellationToken, string collectionName = "")
    {
        try
        {
            foreach (var batch in dataQueue.GetConsumingEnumerable(cancellationToken))
            {
                await _dataStore.InsertBatchAsync(jobId, batch, collectionName);
                _logger.LogInformation("Imported batch of {Count} records for job {JobId}", batch.Count, jobId);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error occurred while importing data to destination for job {JobId}", jobId);
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
}
