using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.MatchConfiguration.GetDataSources;

public class MatchConfigDataSourcesValidator : AbstractValidator<MatchConfigDataSourcesRequest>
{
    public MatchConfigDataSourcesValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));
    }
}
