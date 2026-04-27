using FluentValidation;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Common.Validators;

public class DataSourceIdValidator : AbstractValidator<Guid>
{
    public DataSourceIdValidator(IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        
        RuleFor(x => x)
            .NotNull()
            .NotEmpty()
            .WithMessage(ValidationMessages.Required("DataSource Id"))
            .WithErrorCode(ErrorCodeConstants.Required);


        RuleFor(x => x)
            .MustAsync(async (id, token) =>
            {
                var dataSource = await dataSourceRepository.GetByIdAsync(id, Constants.Collections.DataSources);
                return dataSource != null;
            })
            .WithMessage(ValidationMessages.NotExists("DataSource"))
            .WithErrorCode(ErrorCodeConstants.NotExists);
    }
}
