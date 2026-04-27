using FluentValidation;
using System;
namespace MatchLogic.Api.Handlers.Survivorship.DetermineOverwrite;
public class DetermineOverwriteCommandValidator : AbstractValidator<DetermineOverwriteCommand>
{
    public DetermineOverwriteCommandValidator()
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        // Basic validations
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");
    }
}
