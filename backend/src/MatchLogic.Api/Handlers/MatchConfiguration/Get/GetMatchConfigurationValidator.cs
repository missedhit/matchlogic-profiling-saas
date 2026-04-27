using FluentValidation;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Get;

public class GetMatchConfigurationValidator : AbstractValidator<GetMatchConfigurationRequest>
{
    public GetMatchConfigurationValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));
    }
}
