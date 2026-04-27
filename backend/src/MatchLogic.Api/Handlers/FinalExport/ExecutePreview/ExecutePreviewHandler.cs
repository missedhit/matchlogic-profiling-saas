using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.FinalExport;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.ExecutePreview;

public class ExecutePreviewHandler(
    IFinalExportService finalExportService,
    IGenericRepository<FinalExportSettings, Guid> settingsRepository,
    ILogger<ExecutePreviewHandler> logger)
    : IRequestHandler<ExecutePreviewCommand, Result<ExecutePreviewResponse>>
{
    public async Task<Result<ExecutePreviewResponse>> Handle(
        ExecutePreviewCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            // Load saved settings as base
            var savedSettings = (await settingsRepository.QueryAsync(
                s => s.ProjectId == request.ProjectId,
                Constants.Collections.FinalExportSettings))
                .FirstOrDefault();

            // Build settings - use request overrides or saved values or defaults
            var settings = new FinalExportSettings
            {
                ProjectId = request.ProjectId,
                ExportAction = request.ExportAction ?? savedSettings?.ExportAction ?? ExportAction.AllRecordsAndFlagDuplicates,
                SelectedAction = request.SelectedAction ?? savedSettings?.SelectedAction ?? SelectedAction.ShowAll,
                DataSetsToInclude = request.DataSetsToInclude ?? savedSettings?.DataSetsToInclude ?? new(),
                IncludeScoreFields = request.IncludeScoreFields ?? savedSettings?.IncludeScoreFields ?? true,
                IncludeSystemFields = request.IncludeSystemFields ?? savedSettings?.IncludeSystemFields ?? true
            };

            // Execute preview (limited groups)
            var exportResult = await finalExportService.ExecuteExportAsync(
              request.ProjectId,
              settings,
              null,
              maxGroups: request.MaxGroups,
              context: null,
              cancellationToken);

            var (previewData, totalCount) = await finalExportService.GetExportDataAsync(
          request.ProjectId,
          isPreview: true,
          pageNumber: 1,
          pageSize: request.PageSize,
          cancellationToken: cancellationToken);

            var response = new ExecutePreviewResponse(
               Success: true,
               Data: previewData.ToList(),
               TotalRecords: totalCount,
               PageNumber: 1,
               PageSize: request.PageSize,
               GroupsProcessed: exportResult.Statistics.GroupsProcessed,
               TotalGroupsAvailable: exportResult.Statistics.TotalGroupsAvailable,
               IsLimited: exportResult.Statistics.IsLimited,
               Statistics: exportResult.Statistics
           );

            return Result<ExecutePreviewResponse>.Success(response);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error executing preview for project {ProjectId}", request.ProjectId);
            return Result<ExecutePreviewResponse>.Error($"Preview failed: {ex.Message}");
        }
    }
}