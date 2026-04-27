using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.Create;

public class CreateFinalExportCommandValidator : AbstractValidator<CreateFinalExportCommand>
{
    public CreateFinalExportCommandValidator()
    {
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");
    }
}