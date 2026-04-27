using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using MatchLogic.Application.Common;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.DataProfile.GenerateProfile;

public class GenerateDataProfileValidator : AbstractValidator<GenerateDataProfileRequest>
{
    public GenerateDataProfileValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));


        RuleFor(x => x.DataSourceIds)
            .NotNull()
            .NotEmpty()
            .WithMessage("At least one Data Source is required. Data Source list should not be empty.")
            .WithErrorCode(ErrorCodeConstants.Required);

        RuleFor(x => x.DataSourceIds)
            .Must(DataSourceNameExists)
            .WithMessage(ValidationMessages.MustBeUniqueInList("Data Source Id's"))
            .WithErrorCode(ErrorCodeConstants.NotExists);

        RuleFor(x => x)
           .MustAsync(CheckDataSourceExists)
           .WithMessage(ValidationMessages.InvalidForSpecified("Data Sources", "Project"))
           .WithName("DataSourceIds")
           .WithErrorCode(ErrorCodeConstants.NotExists);

        /*RuleForEach(x => x.DataSourceIds)
            .ChildRules(dataSource =>
            {
                dataSource.RuleFor(ds => ds)
                    .NotNull()
                    .NotEmpty()
                    .WithMessage("DataSource Id is required");

                dataSource.RuleFor(ds => ds)
                    .MustAsync(DatasourceExits)
                    .WithMessage("DataSource does not exist.")
                    .WithErrorCode(ErrorCodeConstants.NotExists);
            });


        async Task<bool> DatasourceExits(Guid dataSourceId, CancellationToken cancellationToken)
        {
            var dS = await dataSourceRepository.GetByIdAsync(dataSourceId, ApiConstants.Collections.DataSources);
            return dS != null;
        }*/

        bool DataSourceNameExists(List<Guid> list)
        {
            if (list == null || list.Count == 0) return true;
            return list.Distinct().Count() == list.Count;
        }

        async Task<bool> CheckDataSourceExists(GenerateDataProfileRequest request, CancellationToken cancellationToken)
        {
            if (request.ProjectId == Guid.Empty) return true;
            if (request.DataSourceIds.Count == 0) return true;

            var dataSources = await dataSourceRepository.QueryAsync(
                x => request.DataSourceIds.Contains(x.Id) && x.ProjectId == request.ProjectId,
                Constants.Collections.DataSources
            );
            return dataSources.Count != 0 || dataSources.Distinct().Count() != dataSources.Count;
        }
    }
}
