using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.Project.Delete;

public class DeleteProjectValidator : AbstractValidator<DeleteProjectRequest>
{
    public DeleteProjectValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;
        RuleFor(x => x.Guid)
            .SetValidator(new ProjectIdValidator(projectRepository));

    }
}
