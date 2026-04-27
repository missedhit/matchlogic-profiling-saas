using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace MatchLogic.Infrastructure.Export.Writers;

/// <summary>
/// LiteDB writer for preview functionality.
/// Writes export data to LiteDB collection for UI grid display.
/// Used when destinationWriter is null in FinalExportService.
/// </summary>
public class LiteDbExportDataWriter : BaseExportDataWriter
{
    private readonly IDataStore _dataStore;
    private readonly string _collectionName;

    public override string Name => "LiteDB Writer";
    public override DataSourceType Type => DataSourceType.LiteDB;

    /// <summary>
    /// Constructor for direct instantiation (preview mode)
    /// </summary>
    public LiteDbExportDataWriter(
        IDataStore dataStore,
        string collectionName,
        ILogger logger)
        : base(logger, 1000)
    {
        _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
    }

    #region IExportDataWriter Implementation

    public override async Task InitializeAsync(ExportSchema schema, CancellationToken ct = default)
    {
        await base.InitializeAsync(schema, ct);

        try
        {
            // Clear existing collection for fresh preview
            await _dataStore.DeleteCollection(_collectionName);

            _logger.LogInformation("LiteDB writer initialized: {Collection}", _collectionName);
        }
        catch (Exception ex)
        {
            AddError($"Failed to initialize LiteDB writer: {ex.Message}");
            throw;
        }
    }

    public override async Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        try
        {
            await _dataStore.InsertBatchAsync(_collectionName, batch);
            _rowsWritten += batch.Count;

            _logger.LogDebug("Wrote {Count} rows to LiteDB collection {Collection}",
                batch.Count, _collectionName);
        }
        catch (Exception ex)
        {
            AddError($"Error writing batch to LiteDB at row {_rowsWritten}: {ex.Message}");
            throw;
        }
    }

    public override async Task<ExportWriteResult> FinalizeAsync(CancellationToken ct = default)
    {
        _logger.LogInformation(
            "LiteDB export completed: {Rows} rows to {Collection}",
            _rowsWritten, _collectionName);

        return await base.FinalizeAsync(ct);
    }

    #endregion

    #region Legacy ExportAsync Support

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        await _dataStore.DeleteCollection(_collectionName);
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        await _dataStore.InsertBatchAsync(_collectionName, batch);
    }

    protected override Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    #endregion
}