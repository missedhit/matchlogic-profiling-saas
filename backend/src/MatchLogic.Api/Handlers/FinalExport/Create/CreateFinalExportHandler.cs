using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.FinalExport.Create;

public class CreateFinalExportHandler(
    IProjectService projectService,
    IGenericRepository<FinalExportSettings, Guid> settingsRepository,
    ILogger<CreateFinalExportHandler> logger)
    : IRequestHandler<CreateFinalExportCommand, Result<CreateFinalExportResponse>>
{
    public async Task<Result<CreateFinalExportResponse>> Handle(
    CreateFinalExportCommand request,
    CancellationToken cancellationToken)
    {
        try
        {
            FinalExportSettings settings = null;

            if (request.SettingsId.HasValue && request.SettingsId.Value != Guid.Empty)
            {
                settings = await settingsRepository.GetByIdAsync(
                    request.SettingsId.Value,
                    Constants.Collections.FinalExportSettings);
            }

            // Fall back to project-based lookup if no settings found by ID
            if (settings == null)
            {
                settings = (await settingsRepository.QueryAsync(
                    s => s.ProjectId == request.ProjectId,
                    Constants.Collections.FinalExportSettings))
                    .FirstOrDefault();
            }

            // Create settings if none exist
            if (settings == null)
            {
                settings = new FinalExportSettings
                {
                    Id = Guid.NewGuid(),
                    ProjectId = request.ProjectId,
                    ConnectionInfo = request.ConnectionInfo
                };

                await settingsRepository.InsertAsync(
                    settings,
                    Constants.Collections.FinalExportSettings);
            }
            else
            {
                // Always ensure ConnectionInfo is updated
                settings.ConnectionInfo = request.ConnectionInfo;

                await settingsRepository.UpdateAsync(
                    settings,
                    Constants.Collections.FinalExportSettings);
            }

            // Create step configuration
            var stepInformation = new List<StepConfiguration>
        {
            new(
                StepType.Export,
                new Dictionary<string, object>
                {
                    { "ProjectId", request.ProjectId },
                    { "ExportSettingsId", settings.Id }
                },
                dataSourceIds: [request.ProjectId]
            )
        };

            var queuedRun = await projectService.StartNewRun(
                request.ProjectId,
                stepInformation);

            logger.LogInformation(
                "Queued final export job for project {ProjectId} with settings {SettingsId}. RunId: {RunId}",
                request.ProjectId, settings.Id, queuedRun.Id);

            return Result<CreateFinalExportResponse>.Success(
                new CreateFinalExportResponse(queuedRun));
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Error creating final export job for project {ProjectId}",
                request.ProjectId);

            return Result<CreateFinalExportResponse>.Error(
                $"Failed to start export: {ex.Message}");
        }
    }

}