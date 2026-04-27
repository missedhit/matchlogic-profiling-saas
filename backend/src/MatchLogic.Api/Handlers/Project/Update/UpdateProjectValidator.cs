using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Project;
using FluentValidation;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.Project.Update;

public class UpdateProjectValidator : AbstractValidator<UpdateProjectRequest>
{
    private readonly IGenericRepository<Domain.Project.Project, Guid> _projectRepository;
    public UpdateProjectValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        this._projectRepository = projectRepository;
        RuleLevelCascadeMode = ClassLevelCascadeMode;


        RuleFor(x => x.Id)
            .SetValidator(new ProjectIdValidator(projectRepository));

        RuleFor(x => x.Name)
            .NotNull().NotEmpty()
            .WithMessage(ValidationMessages.Required("Project name"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .MaximumLength(Constants.FieldLength.NameMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Project name", Constants.FieldLength.NameMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        RuleFor(x => x)
            .MustAsync(CheckProjectNameUniqueness)
            .WithMessage(ValidationMessages.AlreadyExists("Project Name"))
            .WithName("Name")
            .WithErrorCode(ErrorCodeConstants.Required);

        RuleFor(x => x.Description)
            .MaximumLength(Constants.FieldLength.DescriptionMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Project description", Constants.FieldLength.DescriptionMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

    }

    private async Task<bool> CheckProjectNameUniqueness(UpdateProjectRequest request, CancellationToken cancellationToken)
    {
        //Exclude the current project
        var project = await this._projectRepository.QueryAsync(x => x.Name.Equals(request.Name) && x.Id != request.Id, Constants.Collections.Projects);
        return project.Count == 0;
    }

}
