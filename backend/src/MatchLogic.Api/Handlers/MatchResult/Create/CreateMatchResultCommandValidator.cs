using FluentValidation;
using MatchLogic.Api.Handlers.MatchDefinition.Validators;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Domain.Entities;

namespace MatchLogic.Api.Handlers.MatchResult.Create;

public class CreateMatchResultCommandValidator : AbstractValidator<CreateMatchResultCommand>
{
    private readonly IMatchConfigurationService _matchConfiguration;    
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedRowRepository;

    public CreateMatchResultCommandValidator(
        IMatchConfigurationService matchConfiguration,
        IGenericRepository<MappedFieldsRow, Guid> mappedRowRepository)
    {
        _matchConfiguration = matchConfiguration;        
        _mappedRowRepository = mappedRowRepository;

        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("Project Id must be provided");

        RuleFor(x => x)
            .CustomAsync(ValidateMatchDefinitionAsync);
    }

    private async Task ValidateMatchDefinitionAsync(
        CreateMatchResultCommand command,
        ValidationContext<CreateMatchResultCommand> context,
        CancellationToken cancellationToken)
    {
        var matchDefinition = await _matchConfiguration
            .GetMappedRowConfigurationByProjectIdAsync(command.ProjectId);

        if (matchDefinition == null)
        {
            context.AddFailure("ProjectId",
                "No match definition found for the specified project");
            return;
        }

        var matchSettings = await _matchConfiguration
            .GetSettingsByProjectIdAsync(command.ProjectId);

        var validator = new MatchDefinitionDtoValidator(_mappedRowRepository, matchSettings);
        var result = await validator.ValidateAsync(matchDefinition, cancellationToken);

        if (!result.IsValid)
        {
            foreach (var error in result.Errors)
            {
                context.AddFailure(
                    $"MatchDefinition.{error.PropertyName}",
                    error.ErrorMessage);
            }
        }
    }
}