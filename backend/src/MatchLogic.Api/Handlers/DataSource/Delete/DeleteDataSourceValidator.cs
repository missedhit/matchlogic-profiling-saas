using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.DataSource.Delete;

public class DeleteDataSourceValidator : AbstractValidator<DeleteDataSourceRequest>
{
    public DeleteDataSourceValidator(IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;
        
        RuleFor(x => x.Id)
            .SetValidator(new DataSourceIdValidator(_dataSourceRepository));
    }
}
