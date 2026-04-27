using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.GetPreview;

public class GetFinalExportPreviewQueryValidator : AbstractValidator<GetFinalExportPreviewQuery>
{
    public GetFinalExportPreviewQueryValidator()
    {
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");

        RuleFor(x => x.PageNumber)
            .GreaterThan(0)
            .WithMessage("Page number must be greater than 0");

        RuleFor(x => x.PageSize)
            .InclusiveBetween(1, 1000)
            .WithMessage("Page size must be between 1 and 1000");
    }
}