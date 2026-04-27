using MatchLogic.Api.Common;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Cleansing.GetRules;

public class GetCleansingRulesValidator : AbstractValidator<GetCleansingRulesRequest>
{
    public GetCleansingRulesValidator(IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository, IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleFor(x => x.ProjectId)
            .NotNull()
            .NotEmpty()
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id is required.")
            .WithErrorCode(ErrorCodeConstants.Required);

        RuleFor(x => x.DataSourceId)
            .NotNull()
            .NotEmpty()
            .NotEqual(Guid.Empty)
            .WithMessage("DataSource Id is required.")
            .WithErrorCode(ErrorCodeConstants.Required);

        RuleFor(x => x.ProjectId)
            .MustAsync(ProjectExists)
            .WithMessage("Project does not exist.")
            .WithErrorCode(ErrorCodeConstants.NotExists);

        RuleFor(x => x.DataSourceId)
            .MustAsync(DataSourceExist)
            .WithMessage("DataSource does not exist.")
            .WithErrorCode(ErrorCodeConstants.NotExists);


        async Task<bool> DataSourceExist(Guid dataSourceId, CancellationToken cancellationToken)
        {
            var dS = await _dataSourceRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);
            return dS != null;
        }
        async Task<bool> ProjectExists(Guid guid, CancellationToken cancellationToken)
        {
            var project = await projectRepository.GetByIdAsync(guid, Constants.Collections.Projects);
            return project != null;
        }


    }
}
