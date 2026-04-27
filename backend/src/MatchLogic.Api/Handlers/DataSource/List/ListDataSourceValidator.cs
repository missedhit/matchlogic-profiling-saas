using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.DataSource.List;

public class ListDataSourceValidator : AbstractValidator<ListDataSourceRequest>
{

    public ListDataSourceValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;
        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));
    }

}
