using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Project.Commands;

/// <summary>
/// Command for executing master record determination on matched groups
/// </summary>
public class MasterRecordDeterminationCommand : BaseCommand
{
    private Guid? _projectId;
    private readonly IMasterRecordDeterminationOrchestrator _masterOrchestrator;
    private readonly MasterDeterminationOptions? _options;

    public MasterRecordDeterminationCommand(
        IMasterRecordDeterminationOrchestrator masterOrchestrator,
        IProjectService projectService,
        IJobEventPublisher jobEventPublisher,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        ILogger<MasterRecordDeterminationCommand> logger,
        MasterDeterminationOptions? options = null) :
        base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository, dataSourceRepository, logger)
    {
        _masterOrchestrator = masterOrchestrator ?? throw new ArgumentNullException(nameof(masterOrchestrator));
        _options = options;
    }

    protected override int NumberOfSteps => 3; // Loading, Processing, Saving

    protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (!_projectId.HasValue)
        {
            throw new InvalidOperationException("ProjectId is required for master determination step");
        }

        // Execute master record determination
        var result = await _masterOrchestrator.ExecuteMasterDeterminationAsync(
            _projectId.GetValueOrDefault(),
            _options,
            context);

        if (!result.Success)
        {
            throw new InvalidOperationException($"Master determination failed: {result.ErrorMessage}");
        }

        // Format output collections
        var output = $"{result.OutputCollections.GroupsCollection} | {result.OutputCollections.MasterGroupsCollection}";

        return new StepData
        {
            Id = Guid.NewGuid(),
            StepJobId = step.Id,
            DataSourceId = _projectId.GetValueOrDefault(),
            CollectionName = output,
            Metadata = new Dictionary<string, object>
            {
                ["TotalGroupsProcessed"] = result.TotalGroupsProcessed,
                ["TotalMasterChanges"] = result.TotalMasterChanges,
                ["StartTime"] = result.StartTime,
                ["EndTime"] = result.EndTime,
                ["Duration"] = (result.EndTime - result.StartTime).TotalSeconds
            }
        };
    }

    protected override Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (step.Configuration?.TryGetValue("ProjectId", out var value) == true)
        {
            _projectId = (Guid)value;
        }
        else
        {
            throw new InvalidOperationException("ProjectId is required for master determination step");
        }

        return Task.CompletedTask;
    }
}