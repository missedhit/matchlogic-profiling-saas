using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.Project.ById;

public class GetProjectValidator : AbstractValidator<GetProjectRequest>
{
    public GetProjectValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleFor(x => x.Id)
            .SetValidator(new ProjectIdValidator(projectRepository));

    }
}
