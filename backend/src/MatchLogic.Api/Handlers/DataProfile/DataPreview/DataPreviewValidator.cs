using MatchLogic.Api.Common;
using FluentValidation;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.DataProfile.DataPreview;

public class DataPreviewValidator  : AbstractValidator<DataPreviewRequest>
{
    public DataPreviewValidator(IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
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
