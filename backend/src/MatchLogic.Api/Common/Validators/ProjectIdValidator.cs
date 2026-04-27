using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Common.Validators;
public class ProjectIdValidator : AbstractValidator<Guid>
{
    public ProjectIdValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleFor(x => x)
            .NotNull().NotEmpty()
            .WithMessage(ValidationMessages.Required("Project ID"))
            .WithErrorCode(ErrorCodeConstants.Required);


        RuleFor(x => x).
            MustAsync(async (id, token) =>
            {
                //Check if the project exists in the repository
                var project = await projectRepository.GetByIdAsync(id, Constants.Collections.Projects);
                return project != null;
            })
            .WithMessage(ValidationMessages.NotExists("Project"))
            .WithErrorCode(ErrorCodeConstants.NotExists);
    }
}
