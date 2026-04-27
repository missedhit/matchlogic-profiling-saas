using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.SaveSettings;

public class SaveFinalExportSettingsCommandValidator : AbstractValidator<SaveFinalExportSettingsCommand>
{
    public SaveFinalExportSettingsCommandValidator()
    {
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");

        RuleFor(x => x.ExportAction)
            .IsInEnum()
            .WithMessage("Invalid export action");

        RuleFor(x => x.SelectedAction)
            .IsInEnum()
            .WithMessage("Invalid selected action");
    }
}