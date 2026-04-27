using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JobStatus = MatchLogic.Domain.Entities.Common.JobStatus;

namespace MatchLogic.Api.Handlers.JobInfo;

public class JobRunStatusHandler(IJobStatusRepository jobStatusRepository,
    IGenericRepository<StepJob, Guid> stepRepository,IGenericRepository<ProjectRun,Guid> projectRunRepository, ILogger<JobRunStatusHandler> logger) 
    : IRequestHandler<JobRunStatusRequest, Result<JobRunStatusResponse>>
{
    public async Task<Result<JobRunStatusResponse>> Handle(JobRunStatusRequest request, CancellationToken cancellationToken)
    {
        var runId = request.RunId;
        const string jobStatusCollection = Constants.Collections.JobStatus;

        var projectRun = await projectRunRepository.GetByIdAsync(runId, Constants.Collections.ProjectRuns);
        var stepStatuses = await stepRepository.QueryAsync(x => x.RunId == runId, Constants.Collections.StepJobs);
        var stepId = stepStatuses != null && stepStatuses.Count > 0 ? stepStatuses.Select(x => x.Id).ToList() : new List<Guid>();
        var jobStatuses = await jobStatusRepository.QueryAsync(x => stepId.Contains( x.JobId ), jobStatusCollection);

        if (jobStatuses == null || jobStatuses?.Count == 0)
        {
            logger.LogError("No Job Status found for  RunId: {RunId}", runId);
            return Result.NotFound($"No data found for RunId: {runId}");
        }

        logger.LogInformation("Job Status found successfully. RunId: {runId}", runId);
        return Result<JobRunStatusResponse>.Success(new JobRunStatusResponse() {JobStatuses = jobStatuses.ToList(), RunStatus = projectRun.Status.ToString()});
    }
}
