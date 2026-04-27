using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

public class JobExecutor : IJobExecutor
{
    private readonly ICommandFactory _commandFactory;
    private readonly IGenericRepository<ProjectRun, Guid> _runRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepRepository;
    private readonly ILogger<JobExecutor> _logger;

    public JobExecutor(
        ICommandFactory commandFactory,
        IGenericRepository<ProjectRun, Guid> runRepository,
        IGenericRepository<StepJob, Guid> stepRepository,
        ILogger<JobExecutor> logger)
    {
        _commandFactory = commandFactory;
        _runRepository = runRepository;
        _stepRepository = stepRepository;
        _logger = logger;
    }

    public async Task ExecuteAsync(ProjectJobInfo job)
    {
        _logger.LogInformation(
            "Executing job {JobId} for project {ProjectId}, Step: {StepType}",
            job.JobId, job.ProjectId, job.CurrentStep.Type);

        // 1. Create context (you already have this class!)
        var context = new CommandContext(
            job.RunId,
            job.ProjectId,
            job.CurrentStep.Id,
            _runRepository,
            _stepRepository);

        // 2. Get command from factory
        var command = _commandFactory.GetCommand(job.CurrentStep.Type);

        // 3. Execute - BaseCommand handles EVERYTHING:
        //    - Status updates (InProgress → Completed/Failed)
        //    - Event publishing (Started/Completed/Failed)
        //    - Error handling (try/catch)
        //    - Calling CompleteStep
        await command.ExecuteAsync(context, job.CurrentStep);
    }
}