using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Linq;
using MatchLogic.Application.Common;
using MatchLogic.Api.Common.Validators;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.Handlers.DataProfile.GenerateAdvanceProfile;
public class GenerateAdvanceDataProfileValidator : AbstractValidator<GenerateAdvanceDataProfileRequest>
{
    public GenerateAdvanceDataProfileValidator(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));

        RuleFor(x => x.DataSourceIds)
            .NotNull()
            .NotEmpty()
            .WithMessage("At least one Data Source is required. Data Source list should not be empty.");

        RuleFor(x => x.DataSourceIds)
            .Must(list => list == null || list.Distinct().Count() == list.Count)
            .WithMessage(ValidationMessages.MustBeUniqueInList("Data Source Id's"));

        RuleFor(x => x)
           .MustAsync(CheckDataSourceExists)
           .WithMessage(ValidationMessages.InvalidForSpecified("Data Sources", "Project"))
           .WithName("DataSourceIds");


        async Task<bool> CheckDataSourceExists(GenerateAdvanceDataProfileRequest request, CancellationToken cancellationToken)
        {
            if (request.ProjectId == Guid.Empty) return true;
            if (request.DataSourceIds == null || request.DataSourceIds.Count == 0) return true;

            var dataSources = await dataSourceRepository.QueryAsync(
                x => request.DataSourceIds.Contains(x.Id) && x.ProjectId == request.ProjectId,
                Constants.Collections.DataSources
            );
            return dataSources.Count == request.DataSourceIds.Count;
        }
    }
}
