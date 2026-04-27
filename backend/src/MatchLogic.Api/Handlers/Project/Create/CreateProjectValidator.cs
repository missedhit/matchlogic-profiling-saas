using MatchLogic.Api.Common;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;
using System.Linq;

namespace MatchLogic.Api.Handlers.Project.Create;

public class CreateProjectValidator : AbstractValidator<CreateProjectRequest>
{
    public CreateProjectValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.Name)
            .NotNull()
            .NotEmpty()
            .WithMessage(ValidationMessages.Required("Project name"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .MaximumLength(Constants.FieldLength.NameMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Project name", Constants.FieldLength.NameMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        RuleFor(x => x.Name)
            .MustAsync(CheckProjectNameUniqueness)
            .WithMessage(ValidationMessages.AlreadyExists("Project Name"))
            .WithErrorCode(ErrorCodeConstants.Required);

        RuleFor(x => x.Description)
            .MaximumLength(Constants.FieldLength.DescriptionMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Project description", Constants.FieldLength.DescriptionMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);


        async Task<bool> CheckProjectNameUniqueness(string name, CancellationToken cancellationToken)
        {
            var project = await projectRepository.QueryAsync(x => x.Name.Equals(name), Constants.Collections.Projects);
            return project != null && project.Count == 0;
        }
    }
}
