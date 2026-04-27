using MatchLogic.Api.Handlers.MatchDefinition.Create;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using MatchLogic.Application.Interfaces.Persistence;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.MatchConfiguration;

namespace MatchLogic.Api.Handlers.MatchDefinition.Update;
public class UpdateMatchDefinitionCommandValidator : AbstractValidator<UpdateMatchDefinitionCommand>
{
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly IMatchConfigurationService _matchConfigurationService;
    public UpdateMatchDefinitionCommandValidator(IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IMatchConfigurationService matchConfigurationService)
    {
        _matchConfigurationService = matchConfigurationService;
        _dataSourceRepository = dataSourceRepository;
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.MatchDefinition)
           .NotNull()
           .WithMessage("Match definition is required.");

        // Basic validations
        RuleFor(x => x.MatchDefinition.Id)
            .NotEqual(Guid.Empty)
            .WithMessage("Id not must be provided");

        When(x => x.MatchDefinition != null, () =>
        {

            // ProjectId validation
            RuleFor(x => x.MatchDefinition.ProjectId)
                .NotEqual(Guid.Empty)
                .WithMessage("ProjectId must be provided");

            // Name validation
            RuleFor(x => x.MatchDefinition.Name)
                .NotEmpty()
                .WithMessage("Name is required");

            // Definitions must exist and have at least one item
            RuleFor(x => x.MatchDefinition.Definitions)
                .NotEmpty()
                .WithMessage("At least one definition must be provided");

            RuleFor(x => x)
        .MustAsync(DataSourcePairsMustExistForProject)
        .WithMessage("No data source pairs are configured for this project. Please configure data source pairs before creating a match definition.");

            RuleForEach(x => x.MatchDefinition.Definitions)
             .SetValidator(new MatchDefinitionMappedRowValidator());
        });

        When(x => x.MatchSetting != null && x.MatchSetting.IsProbabilistic, () =>
        {
            RuleFor(x => x.MatchDefinition.Definitions)
                .Must(HaveRequiredProbabilisticCriteria)
                .WithMessage("Probabilistic matching requires at least one exact match criteria and one fuzzy match criteria");
        });

        When(x => x.MatchDefinition?.Definitions != null, () =>
        {
            RuleForEach(x => x.MatchDefinition.Definitions)
                .Must(NoDataSourceFieldDuplicatesWithinDefinition)
                .WithMessage("Each field can only be used once within the same definition");
        });
        When(x => x.MatchDefinition?.Definitions != null, () =>
        {
            RuleFor(x => x)
                .MustAsync(AllDataSourcesMustParticipateInCriteria)
                .WithMessage("All data sources must participate in each match definition criterion");
        });
    }

    private async Task<bool> DataSourcePairsMustExistForProject(
    UpdateMatchDefinitionCommand command,
    CancellationToken cancellationToken)
    {
        if (command.MatchDefinition?.ProjectId == null ||
            command.MatchDefinition.ProjectId == Guid.Empty)
            return true; // Let the ProjectId validation handle this

        var pairs = await _matchConfigurationService.GetDataSourcePairsByProjectIdAsync(command.MatchDefinition.ProjectId);

        return pairs != null && pairs.Count > 0;
    }
    private async Task<bool> AllDataSourcesMustParticipateInCriteria(
    UpdateMatchDefinitionCommand command,
    CancellationToken cancellationToken)
    {
        if (command.MatchDefinition?.Definitions == null || !command.MatchDefinition.Definitions.Any())
            return true;

        // Get all data sources for the project
        var dataSources = await _dataSourceRepository
            .QueryAsync(ds => ds.ProjectId == command.MatchDefinition.ProjectId, Constants.Collections.DataSources);

        var dataSourceCount = dataSources.Count();

        if (dataSourceCount == 0) return true; // No data sources to validate against

        // Check each definition's criteria
        foreach (var definition in command.MatchDefinition.Definitions)
        {
            if (definition?.Criteria == null) continue;

            foreach (var criterion in definition.Criteria)
            {
                if (criterion.MappedRow?.FieldsByDataSource == null) continue;

                // Check if the count matches
                if (criterion.MappedRow.FieldsByDataSource.Count != dataSourceCount)
                {
                    return false;
                }

                // Optional: Verify that all data source IDs match
                var dataSourceIds = dataSources.Select(ds => ds.Id).ToHashSet();
                var mappedDataSourceIds = criterion.MappedRow.FieldsByDataSource.Values
                    .Where(f => f != null)
                    .Select(f => f.DataSourceId)
                    .ToHashSet();

                if (!dataSourceIds.SetEquals(mappedDataSourceIds))
                {
                    return false;
                }
            }
        }

        return true;
    }
    private bool HaveRequiredProbabilisticCriteria(List<MatchDefinitionMappedRowDto> definitions)
    {
        if (definitions == null || !definitions.Any()) return false;

        // Get all criteria from all definitions
        var allCriteria = definitions
            .Where(d => d.Criteria != null)
            .SelectMany(d => d.Criteria);

        bool hasExactCriteria = allCriteria.Any(c => c.MatchingType == MatchingType.Exact);
        bool hasFuzzyCriteria = allCriteria.Any(c => c.MatchingType == MatchingType.Fuzzy);

        return hasExactCriteria && hasFuzzyCriteria;
    }

    private bool NoDataSourceFieldDuplicatesWithinDefinition(MatchDefinitionMappedRowDto definition)
    {
        if (definition?.Criteria == null) return true;

        var usedFields = new List<string>();

        foreach (var criterion in definition.Criteria)
        {
            if (criterion.MappedRow?.FieldsByDataSource == null) continue;

            // Add all data source fields from this criterion
            foreach (var fieldMapping in criterion.MappedRow.FieldsByDataSource)
            {
                if (fieldMapping.Value != null && !string.IsNullOrEmpty(fieldMapping.Value.Name))
                {
                    // Create unique identifier: DataSourceId:FieldName
                    var fieldIdentifier = $"{fieldMapping.Value.DataSourceId}:{fieldMapping.Value.Name.ToLowerInvariant()}";
                    usedFields.Add(fieldIdentifier);
                }
            }
        }

        // Check if the count of distinct fields equals the total count
        // If not, it means there are duplicate data source fields within this definition
        return usedFields.Count == usedFields.Distinct().Count();
    }
}
