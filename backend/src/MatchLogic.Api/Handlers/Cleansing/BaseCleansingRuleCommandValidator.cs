using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.Project.Delete;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;
using MatchLogic.Domain.CleansingAndStandaradization;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing;

public abstract class BaseCleansingRuleCommandValidator<T, TResponse> : AbstractValidator<T>
    where T : BaseCleansingRuleCommand<TResponse>
    where TResponse : ICleansingRuleResponse
{
    protected BaseCleansingRuleCommandValidator(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;

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

        // Prevent multiple case transformation rules for the same column
        RuleFor(x => x.StandardRules)
            .Must(rules =>
            {
                // Use a HashSet to track columns with a case transformation rule for O(1) lookup
                var seenColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var rule in rules)
                {
                    if (rule.RuleType == CleaningRuleType.UpperCase ||
                        rule.RuleType == CleaningRuleType.LowerCase ||
                        //rule.RuleType == CleaningRuleType.TitleCase ||
                        rule.RuleType == CleaningRuleType.ProperCase)
                    {
                        // If already seen, there is more than one case transformation rule for this column
                        if (!seenColumns.Add(rule.ColumnName))
                            return false;
                    }
                }
                return true;
            })
            .WithMessage("A column cannot have multiple case transformation rules (UpperCase, LowerCase, TitleCase, ProperCase).")
            .WithErrorCode(ErrorCodeConstants.Invalid);
        async Task<bool> ProjectExists(Guid guid, CancellationToken cancellationToken)
        {
            var project = await projectRepository.GetByIdAsync(guid, Constants.Collections.Projects);
            return project != null;
        }

        async Task<bool> DataSourceExist(Guid guid, CancellationToken cancellationToken)
        {
            var dataSource = await dataSourceRepository.GetByIdAsync(guid, Constants.Collections.DataSources);
            return dataSource != null;
        }
    }
}
