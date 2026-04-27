using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Project;

/// <summary>
/// Slim SaaS profiling-only ProjectService. The original main-product version
/// coordinated cleanup across MatchDefinitions, CleansingRules, MergeRules,
/// FieldOverwriteRules, MasterRecordRules, FinalExportSettings — all gone in
/// this fork. The remaining surface is: project CRUD, DataSource CRUD, and the
/// run/step orchestration that hands profile jobs to Hangfire.
/// </summary>
public class ProjectService : IProjectService
{
    private readonly IGenericRepository<Domain.Project.Project, Guid> _projectRepository;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<DataSnapshot, Guid> _snapshotRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepJobRepository;
    private readonly IDataStore _dataStore;
    private readonly ILogger<ProjectService> _logger;
    private readonly ISecureParameterHandler _secureParameterHandler;
    private readonly IOAuthTokenService _oAuthTokenService;
    private readonly IScheduler _scheduler;
    private readonly IJobEventPublisher _jobEventPublisher;

    public ProjectService(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<DataSource, Guid> dataSourceRepository,
        IGenericRepository<DataSnapshot, Guid> snapshotRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IDataStore dataStore,
        ISecureParameterHandler secureParameterHandler,
        IOAuthTokenService oAuthTokenService,
        IScheduler scheduler,
        IJobEventPublisher jobEventPublisher,
        ILogger<ProjectService> logger)
    {
        _projectRepository = projectRepository;
        _projectRunRepository = projectRunRepository;
        _dataSourceRepository = dataSourceRepository;
        _snapshotRepository = snapshotRepository;
        _stepJobRepository = stepJobRepository;
        _dataStore = dataStore;
        _secureParameterHandler = secureParameterHandler;
        _oAuthTokenService = oAuthTokenService;
        _scheduler = scheduler;
        _jobEventPublisher = jobEventPublisher;
        _logger = logger;
    }

    public async Task<Domain.Project.Project> CreateProject(string name, string description, int retentionRuns = 2)
    {
        var project = new Domain.Project.Project
        {
            Name = name,
            Description = description,
            RetentionRuns = retentionRuns
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);
        return project;
    }

    public async Task<Domain.Project.Project> GetProjectById(Guid projectId)
    {
        var project = await _projectRepository.GetByIdAsync(projectId, Constants.Collections.Projects);
        return project ?? throw new InvalidOperationException($"Project with ID {projectId} not found");
    }

    public async Task<Domain.Project.Project> UpdateProject(Guid projectId, string name, string description, int retentionRuns = 2)
    {
        var project = await _projectRepository.GetByIdAsync(projectId, Constants.Collections.Projects)
            ?? throw new InvalidOperationException($"Project with ID {projectId} not found");

        project.Name = name;
        project.Description = description;
        project.RetentionRuns = retentionRuns;

        await _projectRepository.UpdateAsync(project, Constants.Collections.Projects);
        return project;
    }

    public async Task DeleteProject(Guid projectId)
    {
        var projectRunIds = (await _projectRunRepository.QueryAsync(
                e => e.ProjectId == projectId, Constants.Collections.ProjectRuns))
            .Select(e => e.Id);

        var dataCollections = (await _stepJobRepository.QueryAsync(
                e => projectRunIds.Contains(e.RunId), Constants.Collections.StepJobs))
            .SelectMany(e => e.StepData)
            .Select(e => e.CollectionName)
            .Where(c => !string.IsNullOrEmpty(c))
            .Distinct();

        foreach (var collection in dataCollections)
            await _dataStore.DeleteCollection(collection);

        await _dataSourceRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.DataSources);
        await _projectRunRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.ProjectRuns);
        await _projectRepository.DeleteAsync(projectId, Constants.Collections.Projects);
    }

    public async Task<List<Domain.Project.Project>> GetAllProjects()
        => await _projectRepository.GetAllAsync(Constants.Collections.Projects);

    public async Task AddDataSource(Guid projectId, List<DataSource> dataSources)
    {
        foreach (var ds in dataSources)
        {
            ds.ProjectId = projectId;
            if (ds.ConnectionDetails?.Parameters != null)
            {
                var parameters = await _secureParameterHandler
                    .EncryptSensitiveParametersAsync(ds.ConnectionDetails.Parameters, ds.Id);
                ds.ConnectionDetails = new BaseConnectionInfo
                {
                    Parameters = parameters,
                    Type = ds.ConnectionDetails.Type
                };
            }
            await _dataSourceRepository.InsertAsync(ds, Constants.Collections.DataSources);
        }
    }

    public async Task<DataSource> RenameDataSourceAsync(Guid dataSourceId, string newName)
    {
        var dataSource = await _dataSourceRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources)
            ?? throw new InvalidOperationException($"DataSource with ID {dataSourceId} not found");

        if (dataSource.Name.Equals(newName, StringComparison.OrdinalIgnoreCase))
            return dataSource;

        dataSource.Name = newName;
        await _dataSourceRepository.UpdateAsync(dataSource, Constants.Collections.DataSources);
        return dataSource;
    }

    public async Task RemoveDataSource(Guid projectId, Guid dataSourceId)
    {
        var dataSource = await _dataSourceRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources)
            ?? throw new InvalidOperationException($"DataSource with ID {dataSourceId} not found");

        // Drop the active snapshot's row collection + snapshots
        var activeSnapshot = dataSource.ActiveSnapshotId.HasValue
            ? await _snapshotRepository.GetByIdAsync(dataSource.ActiveSnapshotId.Value, Constants.Collections.DataSnapshots)
            : null;

        if (activeSnapshot != null)
        {
            if (activeSnapshot.FileImportId.HasValue)
                await _dataStore.DeleteAsync(activeSnapshot.FileImportId.Value, Constants.Collections.ImportFile);
            if (!string.IsNullOrEmpty(activeSnapshot.StoragePrefix))
                await _dataStore.DeleteCollection(activeSnapshot.StoragePrefix);
        }

        await _dataStore.DeleteAllAsync<DataSnapshot>(
            x => x.DataSourceId == dataSourceId, Constants.Collections.DataSnapshots);

        // Drop StepJobs and their data collections for this DataSource
        var stepJobs = await _stepJobRepository.QueryAsync(
            x => x.Type == StepType.Import && x.DataSourceId == dataSourceId, Constants.Collections.StepJobs);

        var stepIds = stepJobs.Select(s => s.Id).ToList();
        var dataCollections = stepJobs
            .SelectMany(s => s.StepData)
            .Select(s => s.CollectionName)
            .Where(c => !string.IsNullOrEmpty(c))
            .Distinct();

        foreach (var collection in dataCollections)
            await _dataStore.DeleteCollection(collection);

        await _stepJobRepository.DeleteAllAsync(
            s => stepIds.Contains(s.Id), Constants.Collections.StepJobs);

        // Drop the profile collection (per-DataSource analytics output)
        var profileCollection = $"{StepType.Profile.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";
        var profileRowRefCollection = $"{profileCollection}_RowReferenceDocument";
        try { await _dataStore.DeleteCollection(profileCollection); } catch { /* best-effort */ }
        try { await _dataStore.DeleteCollection(profileRowRefCollection); } catch { /* best-effort */ }

        await _dataStore.DeleteAllAsync<DataSourceColumnNotes>(
            x => x.DataSourceId == dataSourceId, Constants.Collections.DataSourceColumnNotes);

        // OAuth token revocation for cloud-storage providers (still relevant if user uploaded
        // via Google Drive / Dropbox / OneDrive). TODO (M4e): drop if remote-storage is killed.
        if (dataSource.Type is DataSourceType.GoogleDrive or DataSourceType.Dropbox or DataSourceType.OneDrive)
        {
            try
            {
                await _oAuthTokenService.RevokeTokensAsync(dataSourceId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to revoke OAuth tokens for DataSource {DataSourceId}", dataSourceId);
            }
        }

        await _dataSourceRepository.DeleteAsync(dataSourceId, Constants.Collections.DataSources);
    }

    public async Task<ProjectRun> StartNewRun(
        Guid projectId,
        List<StepConfiguration> stepsConfiguration,
        Guid? scheduledTaskExecutionId = null)
    {
        var run = new ProjectRun
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            StartTime = DateTime.UtcNow,
            Status = RunStatus.InProgress,
            ScheduledTaskExecutionId = scheduledTaskExecutionId
        };
        await _projectRunRepository.InsertAsync(run, Constants.Collections.ProjectRuns);

        stepsConfiguration = stepsConfiguration.OrderBy(s => s.Type).ToList();
        var steps = new List<StepJob>();

        foreach (var stepConfig in stepsConfiguration)
        {
            foreach (var dataSourceId in stepConfig.DataSourceIds)
            {
                var stepJob = new StepJob
                {
                    Type = stepConfig.Type,
                    RunId = run.Id,
                    Status = RunStatus.NotStarted,
                    StartTime = DateTime.UtcNow,
                    DataSourceId = dataSourceId,
                    Configuration = stepConfig.Configuration ?? new Dictionary<string, object>()
                };
                if (stepConfig.Type == StepType.Import)
                    stepJob.Configuration[Constants.FieldNames.DataSourceId] = dataSourceId;

                steps.Add(stepJob);
                await _stepJobRepository.InsertAsync(stepJob, Constants.Collections.StepJobs);
            }
        }

        if (steps.Any())
        {
            var firstStepType = steps.OrderBy(s => s.Type).First().Type;
            foreach (var step in steps.Where(s => s.Type == firstStepType))
                await QueueStepJob(run, step);
        }

        return run;
    }

    public async Task<StepJob> StartStep(Guid runId, StepType stepType, Dictionary<string, object> configuration)
    {
        var run = await _projectRunRepository.GetByIdAsync(runId, Constants.Collections.ProjectRuns);
        var step = (await _stepJobRepository.QueryAsync(
                s => s.RunId == runId && s.Type == stepType && s.Status == RunStatus.NotStarted,
                Constants.Collections.StepJobs)).First();

        step.Configuration = configuration ?? new Dictionary<string, object>();
        step.Status = RunStatus.InProgress;
        await _stepJobRepository.UpdateAsync(step, Constants.Collections.StepJobs);
        await QueueStepJob(run, step);
        return step;
    }

    public async Task CompleteStep(
        StepJob step,
        StepData stepData,
        RunStatus runStatus = RunStatus.Completed,
        FlowStatistics statistics = null,
        string errorMessage = null)
    {
        var run = await _projectRunRepository.GetByIdAsync(step.RunId, Constants.Collections.ProjectRuns)
            ?? throw new InvalidOperationException($"Run not found for step {step.Id}");

        var currentStep = await _stepJobRepository.GetByIdAsync(step.Id, Constants.Collections.StepJobs);
        currentStep.Status = runStatus;
        currentStep.EndTime = DateTime.UtcNow;
        currentStep.StepData.Add(stepData);

        if (statistics != null)
        {
            currentStep.Configuration["RecordsProcessed"] = statistics.RecordsProcessed;
            currentStep.Configuration["BatchesProcessed"] = statistics.BatchesProcessed;
            currentStep.Configuration["ErrorCount"] = statistics.ErrorRecords;
            currentStep.Configuration["Duration"] = statistics.Duration;
            currentStep.Configuration["OperationType"] = statistics.OperationType;
        }
        if (errorMessage != null)
            currentStep.Configuration["ErrorMessage"] = errorMessage;

        await _stepJobRepository.UpdateAsync(currentStep, Constants.Collections.StepJobs);

        var allSteps = (await _stepJobRepository.QueryAsync(
            e => e.RunId == run.Id, Constants.Collections.StepJobs)).ToList();

        if (runStatus == RunStatus.Completed)
            await TryEnqueueNextStepTypeAsync(run, currentStep, allSteps);
        else if (runStatus == RunStatus.Failed)
        {
            await CancelRemainingStepsAsync(allSteps, run.Id);
            allSteps = (await _stepJobRepository.QueryAsync(
                e => e.RunId == run.Id, Constants.Collections.StepJobs)).ToList();
        }

        if (!allSteps.Any(s => s.Status == RunStatus.NotStarted || s.Status == RunStatus.InProgress))
        {
            run.Status = allSteps.Any(s => s.Status == RunStatus.Failed)
                ? RunStatus.Failed
                : RunStatus.Completed;
            run.EndTime = DateTime.UtcNow;

            var project = await _projectRepository.GetByIdAsync(run.ProjectId, Constants.Collections.Projects);
            project.LastRunStep = currentStep.Type;
            await _projectRepository.UpdateAsync(project, Constants.Collections.Projects);
        }

        await _projectRunRepository.UpdateAsync(run, Constants.Collections.ProjectRuns);
    }

    private async Task CancelRemainingStepsAsync(List<StepJob> allSteps, Guid runId)
    {
        var stepsToCancel = allSteps.Where(s => s.Status == RunStatus.NotStarted).ToList();
        foreach (var step in stepsToCancel)
        {
            step.Status = RunStatus.Cancelled;
            step.EndTime = DateTime.UtcNow;
            await _stepJobRepository.UpdateAsync(step, Constants.Collections.StepJobs);
        }
        if (stepsToCancel.Any())
            _logger.LogWarning(
                "Cancelled {Count} NotStarted step(s) for run {RunId} due to upstream failure",
                stepsToCancel.Count, runId);
    }

    private async Task TryEnqueueNextStepTypeAsync(
        ProjectRun run, StepJob completedStep, List<StepJob> allSteps)
    {
        var stepsOfSameType = allSteps.Where(s => s.Type == completedStep.Type).ToList();
        var allOfTypeComplete = stepsOfSameType.All(s =>
            s.Status == RunStatus.Completed || s.Status == RunStatus.Failed);
        if (!allOfTypeComplete) return;
        if (stepsOfSameType.Any(s => s.Status == RunStatus.Failed)) return;

        var nextStepType = GetNextStepType(completedStep.Type, allSteps);
        if (nextStepType == null) return;

        var nextSteps = allSteps
            .Where(s => s.Type == nextStepType.Value && s.Status == RunStatus.NotStarted)
            .ToList();
        foreach (var step in nextSteps)
            await QueueStepJob(run, step);
    }

    private static StepType? GetNextStepType(StepType currentType, List<StepJob> allSteps)
    {
        var configuredStepTypes = allSteps
            .OrderBy(s => s.Type)
            .Select(s => s.Type)
            .Distinct()
            .ToList();
        var currentIndex = configuredStepTypes.IndexOf(currentType);
        return currentIndex >= 0 && currentIndex < configuredStepTypes.Count - 1
            ? configuredStepTypes[currentIndex + 1]
            : null;
    }

    private async Task QueueStepJob(ProjectRun run, StepJob step)
    {
        var jobInfo = new ProjectJobInfo
        {
            JobId = Guid.NewGuid(),
            RunId = run.Id,
            ProjectId = run.ProjectId,
            CurrentStep = step
        };
        await _scheduler.EnqueueJobAsync(jobInfo);
    }
}
