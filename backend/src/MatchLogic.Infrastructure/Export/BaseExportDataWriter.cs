using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export;

public abstract class BaseExportDataWriter : IExportDataStrategy
{
    protected readonly ILogger _logger;
    protected readonly int _internalBatchSize;
    protected ExportSchema? _schema;
    protected long _rowsWritten;
    protected readonly Stopwatch _stopwatch = new();
    protected readonly List<string> _errors = new();
    protected bool _initialized;
    protected bool _finalized;
    protected bool _disposed;

    public abstract string Name { get; }
    public long RowsWritten => _rowsWritten;
    public IReadOnlyList<string> Errors => _errors;
    public abstract DataSourceType Type { get; }

    protected ExportContext? _exportContext;

    protected BaseExportDataWriter(ILogger logger, int batchSize = 1000)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _internalBatchSize = batchSize;
    }
    public virtual Task InitializeAsync(ExportSchema schema, CancellationToken ct = default)
    {
        if (_initialized)
            throw new InvalidOperationException($"{Name} already initialized");

        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _stopwatch.Start();
        _initialized = true;

        _logger.LogInformation(
            "{Writer} initializing with {Columns} columns, table={Table}",
            Name,
            schema.Columns.Count,
            schema.TableName ?? "(none)");

        return Task.CompletedTask;
    }

    public virtual Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        if (!_initialized)
            throw new InvalidOperationException($"{Name} not initialized. Call InitializeAsync first.");

        if (_finalized)
            throw new InvalidOperationException($"{Name} already finalized. Cannot write more data.");

        return Task.CompletedTask;
    }

    public virtual Task<ExportWriteResult> FinalizeAsync(CancellationToken ct = default)
    {
        if (!_initialized)
            throw new InvalidOperationException($"{Name} not initialized");

        if (_finalized)
            throw new InvalidOperationException($"{Name} already finalized");

        _finalized = true;
        _stopwatch.Stop();

        _logger.LogInformation(
            "{Writer} finalized: {Rows} rows in {Duration}",
            Name,
            _rowsWritten,
            _stopwatch.Elapsed);

        return Task.FromResult(new ExportWriteResult
        {
            Success = _errors.Count == 0,
            RowsWritten = _rowsWritten,
            Duration = _stopwatch.Elapsed,
            Errors = _errors.ToList()
        });
    }

    protected void AddError(string error)
    {
        _errors.Add(error);
        _logger.LogError("{Writer} error: {Error}", Name, error);
    }

    protected void AddWarning(string warning)
    {
        _logger.LogWarning("{Writer} warning: {Warning}", Name, warning);
    }


    protected virtual Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    => Task.CompletedTask;

    protected abstract Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken);

    protected virtual Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
        => Task.CompletedTask;
    public async Task<bool> ExportAsync(
        IAsyncEnumerable<IDictionary<string, object>> rows,
        DataExportOptions options,
        IColumnFilter columnFilter,
        List<FieldMappingEx> fieldMappings,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (fieldMappings == null)
                throw new ArgumentNullException(nameof(fieldMappings));

            // Create immutable export context with pre-computed lookups
            _exportContext = new ExportContext(options, fieldMappings.AsReadOnly());

            await InitializeExportAsync(_exportContext, cancellationToken);

            var batch = new List<IDictionary<string, object>>(_internalBatchSize);
            var rowCount = 0;

            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                // Transform row to maintain column order and apply filtering
                var orderedRow = TransformRowToOrderedDictionary(row, _exportContext, columnFilter);
                batch.Add(orderedRow);

                if (batch.Count >= _internalBatchSize)
                {
                    await WriteBatchAsync(batch, _exportContext, cancellationToken);
                    rowCount += batch.Count;
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await WriteBatchAsync(batch, _exportContext, cancellationToken);
                rowCount += batch.Count;
            }

            await FinalizeExportAsync(_exportContext, rowCount, cancellationToken);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during export.");
            throw;
        }
    }

    /// <summary>
    /// Transform input row to maintain column order and apply filtering.
    /// Uses pre-computed indices for optimal performance.
    /// </summary>
    private static IDictionary<string, object> TransformRowToOrderedDictionary(
        IDictionary<string, object> sourceRow,
        ExportContext context,
        IColumnFilter? columnFilter)
    {
        // Use OrderedDictionary equivalent for consistent column ordering
        var orderedRow = new Dictionary<string, object>(context.OrderedColumnNames.Count);

        foreach (var columnName in context.OrderedColumnNames)
        {
            if (sourceRow.TryGetValue(columnName, out var value))
            {
                // Apply column filtering if available
                orderedRow[columnName] = value;
            }
        }

        return orderedRow;
    }


    public virtual void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }
}