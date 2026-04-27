using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.Orchestration;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Project.Commands;

public class MatchingCommand : BaseCommand
{
    private Guid? _projectId;
    private readonly IRecordLinkageOrchestrator _recordLinkageOrchestrator;
    private readonly OrchestrationOptions? _orchestrationOptions;
    public MatchingCommand(IRecordLinkageOrchestrator recordLinkageOrchestrator, IProjectService projectService, IJobEventPublisher jobEventPublisher,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository, ILogger<MatchingCommand> logger, OrchestrationOptions? orchestrationOptions=null) :
        base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository, dataSourceRepository,
            logger)
    {
        _recordLinkageOrchestrator = recordLinkageOrchestrator;
        _orchestrationOptions = orchestrationOptions;
    }

    protected override int NumberOfSteps => 5;

    protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (!_projectId.HasValue)
        {
            throw new InvalidOperationException("ProjectId is required for match step");
        }        
        var result = await _recordLinkageOrchestrator.ExecuteRecordLinkageAsync(_projectId.GetValueOrDefault(), _orchestrationOptions, context);

        var outp = $"{result.OutputCollections.PairsCollection} | {result.OutputCollections.GroupsCollection} | {result.OutputCollections.GraphCollection}";
        return new StepData
        {
            Id = Guid.NewGuid(),
            StepJobId = step.Id,
            DataSourceId = _projectId.GetValueOrDefault(),
            CollectionName = outp
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
            throw new InvalidOperationException("PorjectId is required for match step");
        }

        return Task.CompletedTask;
    }
}
