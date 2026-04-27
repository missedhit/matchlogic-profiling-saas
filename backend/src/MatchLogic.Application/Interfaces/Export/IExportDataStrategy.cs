using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
namespace MatchLogic.Application.Interfaces.Export;

public interface IExportDataStrategy : IDisposable
{
    //ConnectionConfig ConnectionConfig { get; set; }
    Task<bool> ExportAsync(IAsyncEnumerable<IDictionary<string, object>> rows, DataExportOptions options, IColumnFilter columnFilter, List<FieldMappingEx> fieldMappings, CancellationToken cancellationToken = default);
    DataSourceType Type { get; }

    /// <summary>
    /// Writer name for logging/display
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Current rows written count
    /// </summary>
    long RowsWritten { get; }

    /// <summary>
    /// Any errors encountered during writing
    /// </summary>
    IReadOnlyList<string> Errors { get; }

    /// <summary>
    /// Initialize writer with column schema.
    /// Creates file/table as needed.
    /// Must be called before WriteBatchAsync.
    /// </summary>
    /// <param name="schema">Export schema with column definitions</param>
    /// <param name="ct">Cancellation token</param>
    Task InitializeAsync(ExportSchema schema, CancellationToken ct = default);

    /// <summary>
    /// Write a batch of rows.
    /// Internal buffering/flushing handled by implementation.
    /// </summary>
    /// <param name="batch">Batch of rows to write</param>
    /// <param name="ct">Cancellation token</param>
    Task WriteBatchAsync(IReadOnlyList<IDictionary<string, object>> batch, CancellationToken ct = default);

    /// <summary>
    /// Finalize export - flush buffers, close connections.
    /// Returns final result with statistics.
    /// Must be called after all data is written.
    /// </summary>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Export result with statistics</returns>
    Task<ExportWriteResult> FinalizeAsync(CancellationToken ct = default);
}
