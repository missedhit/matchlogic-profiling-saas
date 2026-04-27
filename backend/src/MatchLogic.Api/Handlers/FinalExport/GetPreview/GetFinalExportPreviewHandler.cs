using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.FinalExport;
using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.FinalExport;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.FinalExport.GetPreview;

public class GetFinalExportPreviewHandler(
    IFinalExportService finalExportService,
    IGenericRepository<FinalExportResult, Guid> exportResultRepository,
    ILogger<GetFinalExportPreviewHandler> logger)
    : IRequestHandler<GetFinalExportPreviewQuery, Result<GetFinalExportPreviewResponse>>
{
    public async Task<Result<GetFinalExportPreviewResponse>> Handle(
        GetFinalExportPreviewQuery request,
        CancellationToken cancellationToken)
    {
        try
        {
            var (data, totalCount) = await finalExportService.GetExportDataAsync(
                request.ProjectId,
                request.IsPreview,
                request.PageNumber,
                request.PageSize,
                request.FilterText,
                request.SortColumn,
                request.Ascending,
                request.Filters,
                cancellationToken);

            // Get last export metadata
            var lastExport = (await exportResultRepository.QueryAsync(
                r => r.ProjectId == request.ProjectId,
                Constants.Collections.FinalExportResults))
                .OrderByDescending(r => r.CreatedAt)
                .FirstOrDefault();

            var response = new GetFinalExportPreviewResponse(
                Data: data.ToList(),
                TotalRecords: totalCount,
                PageNumber: request.PageNumber,
                PageSize: request.PageSize,
                LastExportedAt: lastExport?.CreatedAt,
                LastExportAction: lastExport?.ExportAction,
                LastSelectedAction: lastExport?.SelectedAction,
                Statistics: lastExport?.Statistics
            );

            return Result<GetFinalExportPreviewResponse>.Success(response);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error getting export preview for project {ProjectId}", request.ProjectId);
            return Result<GetFinalExportPreviewResponse>.Error($"Failed to get preview: {ex.Message}");
        }
    }
}