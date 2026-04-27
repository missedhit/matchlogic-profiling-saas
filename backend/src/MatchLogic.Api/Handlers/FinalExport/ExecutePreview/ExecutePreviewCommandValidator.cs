using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.ExecutePreview;

public class ExecutePreviewCommandValidator : AbstractValidator<ExecutePreviewCommand>
{
    public ExecutePreviewCommandValidator()
    {
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");

        RuleFor(x => x.MaxGroups)
            .InclusiveBetween(1, 100)
            .WithMessage("MaxGroups must be between 1 and 100");
    }
}