using MatchLogic.Api.Common;
using FluentValidation;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.DataProfile.AdvanceDataPreview;

public class AdvanceDataPreviewValidator : AbstractValidator<AdvanceDataPreviewRequest>
{
    public AdvanceDataPreviewValidator(IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.DataSourceId)
            .SetValidator(new DataSourceIdValidator(dataSourceRepository));
                
        RuleFor(x => x.DocumentId)
            .NotNull()
            .NotEmpty()
            .NotEqual(Guid.Empty)
            .WithMessage(ValidationMessages.Required("Document Id"))
            .WithErrorCode(ErrorCodeConstants.Required);

    }
}
