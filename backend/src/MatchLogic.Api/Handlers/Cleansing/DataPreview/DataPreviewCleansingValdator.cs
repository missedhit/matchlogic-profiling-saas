
using FluentValidation;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Api.Common;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Cleansing.DataPreview;

public class DataPreviewCleansingValdator : AbstractValidator<DataPreviewCleansingRequest>
{
    public DataPreviewCleansingValdator(IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;
        RuleFor(x => x.DataSourceId)
            .NotNull()
            .NotEmpty()
            .NotEqual(Guid.Empty)
            .WithMessage("DataSource Id is required.").WithErrorCode(ErrorCodeConstants.Required);

        RuleFor(x => x.PageNumber)
            .GreaterThan(0)
            .WithMessage("PageNumber must be greater than zero.")
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        RuleFor(x => x.PageSize)
            .GreaterThan(0)
            .WithMessage("PageSize must be greater than zero.")
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        RuleFor(x => x.DataSourceId).
            MustAsync(DatasourceExits)
            .WithMessage("DataSource does not exist.")
            .WithErrorCode(ErrorCodeConstants.NotExists);

        async Task<bool> DatasourceExits(Guid dataSourceId, CancellationToken cancellationToken)
        {
            var dS = await _dataSourceRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);
            return dS != null;
        }
    }
}
