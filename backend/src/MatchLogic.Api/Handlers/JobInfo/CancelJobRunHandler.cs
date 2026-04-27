using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.JobInfo;

public class CancelJobRunHandler(
    IJobCancellationRegistry cancellationRegistry,
    IGenericRepository<ProjectRun, Guid> projectRunRepository,
    IGenericRepository<StepJob, Guid> stepJobRepository,
    ILogger<CancelJobRunHandler> logger)
    : IRequestHandler<CancelJobRunRequest, Result<CancelJobRunResponse>>
{
    public async Task<Result<CancelJobRunResponse>> Handle(CancelJobRunRequest request, CancellationToken cancellationToken)
    {
        var runId = request.RunId;

        var projectRun = await projectRunRepository.GetByIdAsync(runId, Constants.Collections.ProjectRuns);
        if (projectRun == null)
        {
            return Result.NotFound($"Run {runId} not found");
        }

        if (projectRun.Status != RunStatus.InProgress)
        {
            return Result<CancelJobRunResponse>.Success(new CancelJobRunResponse
            {
                Cancelled = false,
                Message = $"Run is not in progress (status: {projectRun.Status})"
            });
        }

        // Try to cancel via the registry (signals CancellationToken)
        var signalled = cancellationRegistry.TryCancel(runId);

        // Mark the run as cancelled in the database regardless
        projectRun.Status = RunStatus.Cancelled;
        projectRun.EndTime = DateTime.UtcNow;
        await projectRunRepository.UpdateAsync(projectRun, Constants.Collections.ProjectRuns);

        // Mark all InProgress steps as cancelled
        var steps = await stepJobRepository.QueryAsync(
            s => s.RunId == runId && s.Status == RunStatus.InProgress,
            Constants.Collections.StepJobs);

        foreach (var step in steps)
        {
            step.Status = RunStatus.Cancelled;
            step.EndTime = DateTime.UtcNow;
            await stepJobRepository.UpdateAsync(step, Constants.Collections.StepJobs);
        }

        logger.LogInformation(
            "Job run {RunId} cancelled. Token signalled: {Signalled}, Steps cancelled: {StepCount}",
            runId, signalled, steps.Count);

        return Result<CancelJobRunResponse>.Success(new CancelJobRunResponse
        {
            Cancelled = true,
            Message = signalled
                ? "Job cancellation signalled"
                : "Job marked as cancelled (was not actively running)"
        });
    }
}
