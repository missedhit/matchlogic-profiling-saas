using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Infrastructure.Project.Commands
{
    public abstract class BaseCommand : ICommand
    {
        protected readonly IProjectService _projectService;
        protected readonly IJobEventPublisher _jobEventPublisher;
        protected readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
        protected readonly IGenericRepository<StepJob, Guid> _stepJobRepository;
        protected readonly IGenericRepository<DomainDataSource, Guid> _dataSourceRepository;
        protected readonly ILogger _logger;

        protected BaseCommand(
            IProjectService projectService,
            IJobEventPublisher jobEventPublisher,
            IGenericRepository<ProjectRun, Guid> projectRunRepository,
            IGenericRepository<StepJob, Guid> stepJobRepository,
            IGenericRepository<DomainDataSource, Guid> dataSourceRepository,
            ILogger logger)
        {
            _projectService = projectService;
            _jobEventPublisher = jobEventPublisher;
            _projectRunRepository = projectRunRepository;
            _stepJobRepository = stepJobRepository;
            _dataSourceRepository = dataSourceRepository;
            _logger = logger;
        }

        public async Task ExecuteAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
        {
            try
            {
                await context.InitializeContext();
                await ValidateInputs(context, step, cancellationToken);

                context.Statistics.OperationType = step.Type.ToString();
                context.Statistics.StartTime = DateTime.UtcNow;

                var dataSourceId = step.DataSourceId.GetValueOrDefault();
                var dataSource = await _dataSourceRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);

                var stepToUpdate = await _stepJobRepository.GetByIdAsync(step.Id, Constants.Collections.StepJobs);
                stepToUpdate.Status = RunStatus.InProgress;
                await _stepJobRepository.UpdateAsync(stepToUpdate, Constants.Collections.StepJobs);

                await _jobEventPublisher.PublishJobStartedAsync(step.Id, NumberOfSteps, $"Starting {step.Type}", dataSource?.Name);

                var stepData = await ExecCommandAsync(context, step, cancellationToken);
                var projectRun = await _projectRunRepository.GetByIdAsync(context.RunId, Constants.Collections.ProjectRuns);

                // Mark statistics complete
                context.Statistics.MarkComplete();

                // Guard completion calls independently — if CompleteStep fails, still try to publish failure
                try
                {
                    await _projectService.CompleteStep(step, stepData, statistics: context.Statistics);
                }
                catch (Exception completeEx)
                {
                    _logger.LogError(completeEx, "Failed to complete step {StepId} for {StepType}, attempting to mark as Failed", step.Id, step.Type);
                    try { await _projectService.CompleteStep(step, new StepData(), RunStatus.Failed, context.Statistics, completeEx.Message); }
                    catch (Exception failEx) { _logger.LogCritical(failEx, "CRITICAL: Could not mark step {StepId} as Failed. Job will be stuck InProgress.", step.Id); }
                    try { await _jobEventPublisher.PublishJobFailedAsync(step.Id, completeEx.Message, context.Statistics); }
                    catch (Exception pubEx) { _logger.LogCritical(pubEx, "CRITICAL: Failed to publish failure event for step {StepId}", step.Id); }
                    throw;
                }

                try
                {
                    await _jobEventPublisher.PublishJobCompletedAsync(step.Id, $"Completed {step.Type}", context.Statistics);
                }
                catch (Exception pubEx)
                {
                    _logger.LogError(pubEx, "Failed to publish completion event for step {StepId}, but step is marked complete", step.Id);
                }
            }
            catch (Exception ex)
            {
                context.Statistics.MarkComplete();
                // Guard each call independently — one failure should not prevent the other
                try { await _projectService.CompleteStep(step, new StepData(), RunStatus.Failed, statistics: context.Statistics); }
                catch (Exception innerEx) { _logger.LogCritical(innerEx, "CRITICAL: Failed to mark step {StepId} as Failed: {Error}", step.Id, innerEx.Message); }
                try { await _jobEventPublisher.PublishJobFailedAsync(step.Id, ex.Message, context.Statistics); }
                catch (Exception pubEx) { _logger.LogCritical(pubEx, "CRITICAL: Failed to publish failure event for step {StepId}", step.Id); }
                throw;
            }
        }

        protected abstract Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default);
        protected abstract Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default);

        protected abstract int NumberOfSteps { get; }
    }
}
public class CommandContext : ICommandContext
{
    public Guid RunId { get; set; }
    public Guid StepId { get; set; }
    public Guid ProjectId { get; set; }
    private readonly Dictionary<StepType, List<StepData>> _stepOutputs = new();
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepJobRepository;

    // Add FlowStatistics
    public FlowStatistics Statistics { get; private set; } = new FlowStatistics();
    public CommandContext(Guid runId, Guid projectId, Guid stepId, IGenericRepository<ProjectRun, Guid> projectRunRepository
        , IGenericRepository<StepJob, Guid> stepJobRepository)
    {
        RunId = runId;
        ProjectId = projectId;
        StepId = stepId;
        _projectRunRepository = projectRunRepository;
        _stepJobRepository = stepJobRepository;
    }

    public async Task InitializeContext()
    {
        var run = await _projectRunRepository.GetByIdAsync(RunId, Constants.Collections.ProjectRuns);
        foreach (var step in await _stepJobRepository.QueryAsync(s => s.RunId == run.Id && s.Status == RunStatus.Completed
                                            , Constants.Collections.StepJobs))
        {
            _stepOutputs[step.Type] = step.StepData;
        }
    }

    public List<StepData> GetStepOutput(StepType stepType, Guid? dataSourceId = null)
    {
        if (dataSourceId == null)
        {
            return _stepOutputs.TryGetValue(stepType, out var outputs) ? outputs : new List<StepData>();
        }
        var dataSourceIdGuid = dataSourceId.GetValueOrDefault(Guid.Empty);

        var step = _stepJobRepository.QueryAsync(s => s.DataSourceId == dataSourceIdGuid &&
                        s.Status == RunStatus.Completed && s.Type == stepType
                        , Constants.Collections.StepJobs).Result.OrderByDescending(x => x.EndTime);

        return step == null ? null : step.First().StepData;
    }

    #region Review and remove these
    public void ResetStatistics()
    {
        Statistics = new FlowStatistics();
    }
    public Dictionary<string, string> GetCollectionNames(StepType stepType)
    {
        var stepOutputs = GetStepOutput(stepType);
        return stepOutputs
            .Where(o => !string.IsNullOrEmpty(o.CollectionName))
            .ToDictionary(o => o.StepJobId.ToString(), o => o.CollectionName);
    }

    public string CreateCollectionName(Guid stepId, string baseName)
    {
        return $"{stepId}_{baseName}_{DateTime.UtcNow.Ticks}";
    }
    // Need to decide do we need that 
    public void SetStepOutput(StepType stepType, StepData output)
    {
        //_stepOutputs[stepType] = output;
        //if (!string.IsNullOrEmpty(output.CollectionName))
        //{
        //    _collectionNameMap[output.CollectionName] = $"{stepType.ToString().ToLower()}_{output.CollectionName}";
        //}
    }
    #endregion
}

public class ColumnFilter : IColumnFilter
{
    public IDictionary<string, object> FilterColumns(
        IDictionary<string, object> row,
        Dictionary<string, ColumnMapping> columnMappings)
    {
        if (columnMappings == null || !columnMappings.Any())
            return row;

        return row
            .Where(kvp => columnMappings.ContainsKey(kvp.Key) &&
                         columnMappings[kvp.Key].Include)
            .ToDictionary(
                kvp => columnMappings[kvp.Key].TargetColumn,
                kvp => kvp.Value);
    }
}