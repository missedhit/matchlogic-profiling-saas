// File: MatchLogic.Application/Interfaces/FinalExport/IFinalExportService.cs

using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.FinalExport;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Domain.Project;

namespace MatchLogic.Application.Interfaces.FinalExport;

public interface IFinalExportService
{
    /// <summary>
    /// Execute export. When maxGroups is provided, runs in preview mode (limited groups).
    /// When maxGroups is null, runs full export.
    /// </summary>
    /// <summary>
    /// Execute export with optional destination writer.
    /// - destinationWriter = null: writes to LiteDB (preview mode)
    /// - destinationWriter provided: writes directly to destination (file/database)
    /// </summary>
    /// <param name="projectId">Project ID</param>
    /// <param name="settings">Export settings</param>
    /// <param name="destinationWriter">Optional destination writer (null for LiteDB preview)</param>
    /// <param name="maxGroups">Limit groups for preview (null for full export)</param>
    /// <param name="context">Command context for progress tracking</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task<FinalExportResult> ExecuteExportAsync(
        Guid projectId,
        FinalExportSettings settings,
        BaseConnectionInfo? connectionInfo = null,
        int? maxGroups = null,
        ICommandContext? context = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get paginated export/preview data
    /// </summary>
    Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetExportDataAsync(
        Guid projectId,
        bool isPreview,
        int pageNumber = 1,
        int pageSize = 100,
        string? filterText = null,
        string? sortColumn = null,
        bool ascending = true,
        string? filters = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Validate before export
    /// </summary>
    Task<ExportValidationResult> ValidateExportAsync(
        Guid projectId,
        CancellationToken cancellationToken = default);
}