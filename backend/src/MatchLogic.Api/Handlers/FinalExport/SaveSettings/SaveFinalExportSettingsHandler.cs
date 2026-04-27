using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.FinalExport;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.FinalExport.SaveSettings;

public class SaveFinalExportSettingsHandler(
    IGenericRepository<FinalExportSettings, Guid> settingsRepository,
    ILogger<SaveFinalExportSettingsHandler> logger)
    : IRequestHandler<SaveFinalExportSettingsCommand, Result<SaveFinalExportSettingsResponse>>
{
    public async Task<Result<SaveFinalExportSettingsResponse>> Handle(
        SaveFinalExportSettingsCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            // Check if settings already exist
            var existingSettings = (await settingsRepository.QueryAsync(
                s => s.ProjectId == request.ProjectId,
                Constants.Collections.FinalExportSettings))
                .FirstOrDefault();

            FinalExportSettings settings;

            if (existingSettings != null)
            {
                // Update existing
                existingSettings.ExportAction = request.ExportAction;
                existingSettings.SelectedAction = request.SelectedAction;
                existingSettings.DataSetsToInclude = request.DataSetsToInclude;
                existingSettings.IncludeScoreFields = request.IncludeScoreFields;
                existingSettings.IncludeSystemFields = request.IncludeSystemFields;
                existingSettings.UpdatedAt = DateTime.UtcNow;

                if(request.ConnectionInfo != null)
                {
                    existingSettings.ConnectionInfo = request.ConnectionInfo;
                }
                await settingsRepository.UpdateAsync(existingSettings, Constants.Collections.FinalExportSettings);
                settings = existingSettings;

                logger.LogInformation("Updated final export settings for project {ProjectId}", request.ProjectId);
            }
            else
            {
                // Create new
                settings = new FinalExportSettings
                {
                    ProjectId = request.ProjectId,
                    ExportAction = request.ExportAction,
                    SelectedAction = request.SelectedAction,
                    DataSetsToInclude = request.DataSetsToInclude,
                    IncludeScoreFields = request.IncludeScoreFields,
                    IncludeSystemFields = request.IncludeSystemFields,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                };

                if (request.ConnectionInfo != null)
                {
                    settings.ConnectionInfo = request.ConnectionInfo;
                }
                await settingsRepository.InsertAsync(settings, Constants.Collections.FinalExportSettings);

                logger.LogInformation("Created final export settings for project {ProjectId}", request.ProjectId);
            }

            return Result<SaveFinalExportSettingsResponse>.Success(
                new SaveFinalExportSettingsResponse(settings.Id, true));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error saving final export settings for project {ProjectId}", request.ProjectId);
            return Result<SaveFinalExportSettingsResponse>.Error($"Failed to save settings: {ex.Message}");
        }
    }
}