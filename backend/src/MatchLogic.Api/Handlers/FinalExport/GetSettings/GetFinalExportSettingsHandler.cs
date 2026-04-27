using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.FinalExport;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Api.Handlers.FinalExport.GetSettings;

public class GetFinalExportSettingsHandler(
    IGenericRepository<FinalExportSettings, Guid> settingsRepository,
    IGenericRepository<DomainDataSource, Guid> dataSourceRepository,
    ILogger<GetFinalExportSettingsHandler> logger)
    : IRequestHandler<GetFinalExportSettingsQuery, Result<GetFinalExportSettingsResponse>>
{
    public async Task<Result<GetFinalExportSettingsResponse>> Handle(
        GetFinalExportSettingsQuery request,
        CancellationToken cancellationToken)
    {
        try
        {
            // Get existing settings or create default
            var settings = (await settingsRepository.QueryAsync(
                s => s.ProjectId == request.ProjectId,
                Constants.Collections.FinalExportSettings))
                .FirstOrDefault();

            // Get available data sources for the project
            var dataSources = (await dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == request.ProjectId,
                Constants.Collections.DataSources))
                .Select(ds => new DataSourceInfo
                {
                    Id = ds.Id,
                    Name = ds.Name,
                    RecordCount = ds.RecordCount
                })
                .ToList();

            // If no settings exist, create default
            if (settings == null)
            {
                settings = new FinalExportSettings
                {
                    ProjectId = request.ProjectId,
                    ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
                    SelectedAction = SelectedAction.ShowAll,
                    IncludeScoreFields = true,
                    IncludeSystemFields = true
                };

                // Default: include all data sources
                foreach (var ds in dataSources)
                {
                    settings.DataSetsToInclude[ds.Id] = true;
                }
            }

            var response = new GetFinalExportSettingsResponse(
                settings,
                dataSources
            );

            return Result<GetFinalExportSettingsResponse>.Success(response);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error getting final export settings for project {ProjectId}", request.ProjectId);
            return Result<GetFinalExportSettingsResponse>.NotFound($"Failed to get settings: {ex.Message}");
        }
    }
}