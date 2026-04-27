using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export;

public abstract class BaseDbExportDataWriter : BaseExportDataWriter
{
    protected BaseDbExportDataWriter(ILogger logger, int batchSize = 1000)
        : base(logger, batchSize) { }

    protected abstract Task OpenConnectionAsync(CancellationToken cancellationToken);
    protected abstract Task CloseConnectionAsync(CancellationToken cancellationToken);
    
    /// <summary>
    /// Ensure table exists with schema based on field mappings.
    /// </summary>
    protected abstract Task EnsureTableExistsAsync(
        ExportContext context, 
        CancellationToken cancellationToken);
    
    /// <summary>
    /// Perform optimized bulk insert with proper column ordering.
    /// </summary>
    protected abstract Task BulkInsertAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken);

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        try
        {
            await OpenConnectionAsync(cancellationToken);
            await EnsureTableExistsAsync(context, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Database initialization.");
            throw;
        }
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch, 
        ExportContext context, 
        CancellationToken cancellationToken)
    {
        try
        {
            await BulkInsertAsync(batch, context, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Database batch write operation.");
            throw;
        }
    }

    protected override async Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Successfully exported {TotalRows} rows to {TableName}", 
            totalRows, context.Options.TableName);
        await CloseConnectionAsync(cancellationToken);
    }
}