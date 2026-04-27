using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.DataSource.GetHeaders;

public class GetHeadersDataSourceValidator : AbstractValidator<GetHeadersDataSourceRequest>
{
    public GetHeadersDataSourceValidator(IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.Id)
            .SetValidator(new DataSourceIdValidator(_dataSourceRepository));
    }
}
