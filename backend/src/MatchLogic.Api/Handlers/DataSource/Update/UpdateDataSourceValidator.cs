using MatchLogic.Api.Common;
using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Update;

public class UpdateDataSourceValidator : AbstractValidator<UpdateDataSourceRequest>
{
    public UpdateDataSourceValidator(IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository)
    {
        RuleLevelCascadeMode = RuleLevelCascadeMode;

        RuleFor(x => x.Id)
            .SetValidator(new DataSourceIdValidator(_dataSourceRepository));


        RuleFor(x => x.Name)
            .NotNull().NotEmpty()
            .WithMessage(ValidationMessages.Required("DataSource name"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .MaximumLength(Constants.FieldLength.NameMaxLength)
            .WithMessage(ValidationMessages.MaxLength("DataSource name", Constants.FieldLength.NameMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        RuleFor(x => x).
            MustAsync(DataSourceNameExists)
            .WithMessage(ValidationMessages.AlreadyExists("DataSource name"))
            .WithName("Name")
            .WithErrorCode(ErrorCodeConstants.NotExists);

        async Task<bool> DataSourceNameExists(UpdateDataSourceRequest data, CancellationToken cancellationToken)
        {
            var dataSource = await _dataSourceRepository.GetByIdAsync(data.Id, Constants.Collections.DataSources);
            if (dataSource == null) return false;
            var projectDataSources = await _dataSourceRepository.QueryAsync(x => x.ProjectId.Equals(dataSource.ProjectId), Constants.Collections.DataSources);
            return !projectDataSources.Any(x => x.Name.Equals(data.Name));
        }
    }
}
