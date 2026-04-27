using MatchLogic.Api.Common;
using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.DataSource.Data;

public class PreviewDataSourceValidator : AbstractValidator<PreviewDataSourceRequest>
{
    public PreviewDataSourceValidator(IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.Id)
            .SetValidator(new DataSourceIdValidator(_dataSourceRepository));


        RuleFor(x => x.PageNumber)
            .GreaterThan(0)
            .WithMessage("PageNumber must be greater than zero.")
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        RuleFor(x => x.PageSize)
            .GreaterThan(0)
            .WithMessage("PageSize must be greater than zero.")
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

    }
}
