using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;
using System.Linq;
using MatchLogic.Domain.Scheduling;

namespace MatchLogic.Infrastructure.Project.Commands;

public class FinalExportCommand : BaseCommand
{
    private readonly IFinalExportService _finalExportService;
    private readonly IGenericRepository<FinalExportSettings, Guid> _exportSettingsRepository;
    private readonly IOAuthTokenService _oauthTokenService;
    private readonly IDataStore _dataStore;

    private Guid? _projectId;
    private FinalExportSettings? _settings;


    public FinalExportCommand(
        IFinalExportService finalExportService,
        IGenericRepository<FinalExportSettings, Guid> exportSettingsRepository,
        IProjectService projectService,
        IJobEventPublisher jobEventPublisher,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IGenericRepository<DomainDataSource, Guid> dataSourceRepository,
        IOAuthTokenService oauthTokenService,
        IDataStore dataStore,
        ILogger<FinalExportCommand> logger)
        : base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository, dataSourceRepository, logger)
    {
        _finalExportService = finalExportService;
        _exportSettingsRepository = exportSettingsRepository;
        _oauthTokenService = oauthTokenService;
        _dataStore = dataStore;
    }

    protected override int NumberOfSteps => 3;

    protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (!_projectId.HasValue)
            throw new InvalidOperationException("ProjectId is required");

        if (_settings == null)
            throw new InvalidOperationException("Export settings are required");

        BaseConnectionInfo? destinationConnection = null;
        var projectRun = await _projectRunRepository.GetByIdAsync(context.RunId, Constants.Collections.ProjectRuns);
        if (_settings.ConnectionInfo != null)
        {
            destinationConnection = _settings.ConnectionInfo;            
        }
        // override  scheduler ConnectionInfo 
        if (projectRun != null)
        {
            var scheduleExec = await _dataStore.GetByIdAsync<ScheduledTaskExecution, Guid>
                (projectRun.ScheduledTaskExecutionId.GetValueOrDefault(),
                Constants.Collections.ScheduledTaskExecutions);
            if (scheduleExec != null)
            {
                var schedule = await _dataStore.GetByIdAsync<ScheduledTask, Guid>
                    (scheduleExec.ScheduledTaskId, Constants.Collections.ScheduledTasks);

                if (schedule?.ConnectionInfo != null)
                {
                    destinationConnection = schedule?.ConnectionInfo;
                }
            }
            
            // Resolve OAuth access token for cloud storage providers (Google Drive, Dropbox, OneDrive)
            if (IsOAuthProvider(destinationConnection.Type))
            {
                if (destinationConnection.Parameters.TryGetValue(RemoteFileConnectionConfig.OAuthDataSourceIdKey, out var oauthDsIdStr) &&
                    Guid.TryParse(oauthDsIdStr, out var oauthDsId))
                {
                    var accessToken = await _oauthTokenService.GetValidAccessTokenAsync(oauthDsId);
                    destinationConnection.Parameters[RemoteFileConnectionConfig.AccessTokenKey] = accessToken;
                    _logger.LogInformation("Resolved OAuth access token for {Type} export", destinationConnection.Type);
                }
                else
                {
                    _logger.LogWarning(
                        "OAuth export for {Type} but OAuthDataSourceId is missing from Parameters. Keys: {Keys}",
                        destinationConnection.Type,
                        string.Join(", ", destinationConnection.Parameters.Keys));
                }
            }
        }

        var result = await _finalExportService.ExecuteExportAsync(
            _projectId.Value,
            _settings,
            destinationConnection,
             maxGroups: null,
            context,
            cancellationToken);

        context.Statistics.RecordsProcessed = (int)Math.Min(result.Statistics.TotalRecordsProcessed, int.MaxValue);
        //context.Statistics.RecordsOutput = (int)Math.Min(result.Statistics.RecordsExported, int.MaxValue);

        return new StepData
        {
            Id = Guid.NewGuid(),
            StepJobId = step.Id,
            DataSourceId = Guid.Empty,
            CollectionName = result.CollectionName,
            Metadata = new Dictionary<string, object>
            {
                ["ProjectId"] = _projectId.Value.ToString(),
                ["RecordsExported"] = result.Statistics.RecordsExported,
                ["RecordsSkipped"] = result.Statistics.RecordsSkipped,
                ["GroupsProcessed"] = result.Statistics.GroupsProcessed,
                ["UniqueRecords"] = result.Statistics.UniqueRecordsExported,
                ["DuplicateRecords"] = result.Statistics.DuplicateRecordsExported,
                ["MasterRecords"] = result.Statistics.MasterRecordsExported,
                ["CrossReferenceRecords"] = result.Statistics.CrossReferenceRecordsExported,
                ["ExportAction"] = _settings.ExportAction.ToString(),
                ["SelectedAction"] = _settings.SelectedAction.ToString(),
                ["ProcessingTimeMs"] = result.Statistics.ProcessingTime.TotalMilliseconds
            }
        };
    }

    protected override async Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        // Extract ProjectId
        if (step.Configuration?.TryGetValue("ProjectId", out var pid) == true)
        {
            _projectId = pid is Guid g ? g : Guid.Parse(pid.ToString()!);
        }
        else
        {
            throw new InvalidOperationException("ProjectId is required");
        }

        // Load settings
        if (step.Configuration?.TryGetValue("ExportSettingsId", out var sid) == true)
        {
            var settingsId = sid is Guid sg ? sg : Guid.Parse(sid.ToString()!);
            _settings = await _exportSettingsRepository.GetByIdAsync(settingsId, Constants.Collections.FinalExportSettings);
        }
        else if (step.Configuration?.TryGetValue("ExportSettings", out var settingsObj) == true &&
                 settingsObj is FinalExportSettings inlineSettings)
        {
            _settings = inlineSettings;
        }

        // Fallback to existing or default
        _settings ??= (await _exportSettingsRepository.QueryAsync(
            s => s.ProjectId == _projectId.Value,
            Constants.Collections.FinalExportSettings)).FirstOrDefault()
            ?? new FinalExportSettings { ProjectId = _projectId.Value };

        // Validate
        var validation = await _finalExportService.ValidateExportAsync(_projectId.Value);
        if (!validation.IsValid)
        {
            throw new InvalidOperationException(string.Join("; ", validation.ValidationErrors));
        }
    }

    private static bool IsOAuthProvider(DataSourceType type)
        => type is DataSourceType.GoogleDrive or DataSourceType.Dropbox or DataSourceType.OneDrive;
}