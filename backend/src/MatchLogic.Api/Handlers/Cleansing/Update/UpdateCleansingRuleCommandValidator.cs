using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.Update;

public class UpdateCleansingRuleCommandValidator
    : BaseCleansingRuleCommandValidator<UpdateCleansingRuleCommand, UpdateCleansingRuleResponse>
{
    public UpdateCleansingRuleCommandValidator(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IGenericRepository<EnhancedCleaningRules, Guid> cleansingRulesRepository)
        : base(projectRepository, dataSourceRepository)
    {
        // Add Update-specific validation rules
        RuleFor(x => x.Id)
            .NotNull()
            .NotEmpty()
            .NotEqual(Guid.Empty)
            .WithMessage("Cleansing Rule Id is required.")
            .WithErrorCode(ErrorCodeConstants.Required);

        // Optionally validate that the rule exists
        RuleFor(x => x.Id)
            .MustAsync(async (id, cancellationToken) =>
            {               
                // deletes and recreates rather than requiring the rule to exist
                var rule = await cleansingRulesRepository.GetByIdAsync(id, Constants.Collections.CleaningRules);
                return rule != null;
            })
            .WithMessage("Cleansing Rule does not exist.")
            .WithErrorCode(ErrorCodeConstants.NotExists);
    }
}