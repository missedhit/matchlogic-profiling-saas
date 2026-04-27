using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.GetSettings;

public class GetFinalExportSettingsQueryValidator : AbstractValidator<GetFinalExportSettingsQuery>
{
    public GetFinalExportSettingsQueryValidator()
    {
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");
    }
}