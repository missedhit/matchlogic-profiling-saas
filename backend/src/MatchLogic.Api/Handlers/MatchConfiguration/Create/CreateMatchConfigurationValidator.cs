using MatchLogic.Api.Common.Validators;
using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Create;

public class CreateMatchConfigurationValidator: AbstractValidator<CreateMatchConfigurationRequest>
{
    public CreateMatchConfigurationValidator(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
            RuleFor(x => x.ProjectId)
                .SetValidator(new ProjectIdValidator(projectRepository));

            RuleFor(x => x.Pairs)
                .NotEmpty()
                .WithMessage(ValidationMessages.CannotBeEmpty("Pairs"))
                .NotNull()
                .WithMessage(ValidationMessages.CannotBeNull("Pairs"));

            RuleForEach(x => x.Pairs)
                .MustAsync(ValidateDataSourcePair)
                .WithMessage("Each pair must reference valid data source IDs belonging to the specified project.")
                .WithErrorCode(ErrorCodeConstants.NotExists);


            // Check if the data source exists in the repository
            async Task<bool> DataSourceExist(Guid projectId, Guid dataSourceId, CancellationToken cancellationToken)
            {
                var dataSources = await dataSourceRepository.QueryAsync(
                x => x.Id == dataSourceId && x.ProjectId == projectId,
                    Constants.Collections.DataSources
                );
                return dataSources != null && dataSources.Count != 0;
            }

            // Check if the pair is valid
            async Task<bool> ValidateDataSourcePair(CreateMatchConfigurationRequest request, BaseDataSourcePairDTO pair, CancellationToken token)
            {
                if (!MustNotBeEmpty(pair, token))
                {
                    return false; // Invalid pair if either ID is empty
                }
                // Use the ProjectId from the request (the context object for the validator)
                var projectId = request.ProjectId;
                // Check if both data sources exist
                var dataSourceAExists = await DataSourceExist(projectId, pair.DataSourceA, token);
                var dataSourceBExists = await DataSourceExist(projectId, pair.DataSourceB, token);
                return dataSourceAExists && dataSourceBExists;
            }
            // Check if the pair must not be empty
            bool MustNotBeEmpty(BaseDataSourcePairDTO pair, CancellationToken token)
            {
                if (pair.DataSourceA == Guid.Empty || pair.DataSourceB == Guid.Empty)
                {
                    return false; // Invalid pair if either ID is empty
                }
                return true; // Valid pair if both IDs are non-empty
            }
        
    }
}
