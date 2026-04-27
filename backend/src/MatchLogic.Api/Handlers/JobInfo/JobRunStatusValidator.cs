using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Project;
using FluentValidation;
using System;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.Handlers.JobInfo;

public class JobRunStatusValidator : AbstractValidator<JobRunStatusRequest>
{
    public JobRunStatusValidator(IGenericRepository<ProjectRun, Guid> jobStatusRepository)
    {
        RuleFor(x => x.RunId)
            .NotNull()
            .NotEmpty()
            .WithMessage(ValidationMessages.Required("RunId"));

        RuleFor(x => x.RunId)
            .MustAsync(async (runId, cancellation) =>
            {
                var jobStatus = await jobStatusRepository.GetByIdAsync(runId, Constants.Collections.ProjectRuns);
                return jobStatus != null;
            })
            .WithMessage(ValidationMessages.NotFoundFor("RunId"));
    }
}
