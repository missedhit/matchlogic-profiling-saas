using FluentValidation;
using System;
namespace MatchLogic.Api.Handlers.Survivorship.DetermineMaster;
public class DetermineMasterCommandValidator : AbstractValidator<DetermineMasterCommand>
{
    public DetermineMasterCommandValidator()
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        // Basic validations
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");
    }
}
