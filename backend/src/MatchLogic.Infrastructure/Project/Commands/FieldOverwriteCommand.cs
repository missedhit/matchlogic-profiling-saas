using MatchLogic.Application.Common;
using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Project.Commands;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Project.Commands;

public class FieldOverwriteCommand : BaseCommand
{
    private readonly IFieldOverwriteOrchestrator _orchestrator;

    public FieldOverwriteCommand(
        IFieldOverwriteOrchestrator orchestrator,
        IProjectService projectService,
        IJobEventPublisher jobEventPublisher,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        ILogger<FieldOverwriteCommand> logger)
        : base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository, dataSourceRepository, logger)
    {
        _orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
    }

    protected override int NumberOfSteps => 3;

    protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        var projectId = ExtractProjectId(step);

        _logger.LogInformation("Starting field overwriting for project {ProjectId}", projectId);

        var result = await _orchestrator.ExecuteFieldOverwritingAsync(projectId);

        if (!result.Success)
        {
            throw new InvalidOperationException($"Field overwriting failed: {result.ErrorMessage}");
        }

        _logger.LogInformation(
            "Field overwriting completed. Groups: {Groups}, Fields: {Fields}",
            result.TotalGroupsProcessed,
            result.TotalFieldsOverwritten);

        return new StepData
        {
            Id = Guid.NewGuid(),
            StepJobId = step.Id,
            CollectionName = result.OutputCollections.OverwrittenGroupsCollection,
            Metadata = new Dictionary<string, object>
            {
                ["TotalGroupsProcessed"] = result.TotalGroupsProcessed,
                ["TotalFieldsOverwritten"] = result.TotalFieldsOverwritten,
                ["Duration"] = result.Duration.TotalSeconds
            }
        };
    }

    protected override Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        var projectId = step.Configuration?.ContainsKey("ProjectId") == true
            ? step.Configuration["ProjectId"]
            : null;

        if (projectId == null || (projectId is Guid guid && guid == Guid.Empty))
        {
            throw new InvalidOperationException("ProjectId is required in step configuration");
        }

        return Task.CompletedTask;
    }

    private Guid ExtractProjectId(StepJob step)
    {
        if (step.Configuration?.TryGetValue("ProjectId", out var projectIdObj) == true)
        {
            if (projectIdObj is Guid guid)
                return guid;

            if (projectIdObj is string str && Guid.TryParse(str, out var parsedGuid))
                return parsedGuid;
        }

        throw new InvalidOperationException("ProjectId not found in step configuration");
    }
}
