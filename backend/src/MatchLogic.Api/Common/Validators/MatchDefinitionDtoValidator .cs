using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using MatchLogic.Api.Handlers.MatchDefinition.Create;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.MatchDefinition.Validators;

public class MatchDefinitionDtoValidator : AbstractValidator<MatchDefinitionCollectionMappedRowDto>
{
    private readonly MatchSettings _matchSetting;    
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedRowRepository;
    public MatchDefinitionDtoValidator(
        IGenericRepository<MappedFieldsRow, Guid> mappedRowRepository, MatchSettings matchSetting = null)
    {        
        _matchSetting = matchSetting;
        _mappedRowRepository = mappedRowRepository;

        RuleLevelCascadeMode = ClassLevelCascadeMode;

        // ProjectId validation
        RuleFor(x => x.ProjectId)
            .NotEqual(Guid.Empty)
            .WithMessage("ProjectId must be provided");

        // Name validation
        RuleFor(x => x.Name)
            .NotEmpty()
            .WithMessage("Name is required");

        // Definitions must exist and have at least one item
        RuleFor(x => x.Definitions)
            .NotEmpty()
            .WithMessage("At least one definition must be provided");

        // Validate each definition
        RuleForEach(x => x.Definitions)
            .SetValidator(new MatchDefinitionMappedRowValidator());

        // Probabilistic validation
        When(x => _matchSetting != null && _matchSetting.IsProbabilistic, () =>
        {
            RuleFor(x => x.Definitions)
                .Must(HaveRequiredProbabilisticCriteria)
                .WithMessage("Probabilistic matching requires at least one exact match criteria and one fuzzy match criteria");
        });

        // No duplicate fields within definition
        When(x => x.Definitions != null, () =>
        {
            RuleForEach(x => x.Definitions)
                .Must(NoDataSourceFieldDuplicatesWithinDefinition)
                .WithMessage("Each field can only be used once within the same definition");
        });

        When(x => x.Definitions != null, () =>
        {
            RuleFor(x => x)
                .MustAsync(AllMappedRowsMustExistInProject)
                .WithMessage("Match definition contains fields that do not exist in the project's MappedRows");
        });
    }    
    private bool HaveRequiredProbabilisticCriteria(List<MatchDefinitionMappedRowDto> definitions)
    {
        if (definitions == null || !definitions.Any()) return false;

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

            foreach (var fieldMapping in criterion.MappedRow.FieldsByDataSource)
            {
                if (fieldMapping.Value != null && !string.IsNullOrEmpty(fieldMapping.Value.Name))
                {
                    var fieldIdentifier = $"{fieldMapping.Value.DataSourceId}:{fieldMapping.Value.Name.ToLowerInvariant()}";
                    usedFields.Add(fieldIdentifier);
                }
            }
        }

        return usedFields.Count == usedFields.Distinct().Count();
    }

    private async Task<bool> AllMappedRowsMustExistInProject(
        MatchDefinitionCollectionMappedRowDto matchDefinition,
        CancellationToken cancellationToken)
    {
        if (matchDefinition.Definitions == null || !matchDefinition.Definitions.Any())
            return true;

        // Get all valid MappedRows for the project
        var projectMappedRows = await _mappedRowRepository
            .QueryAsync(mr => mr.ProjectId == matchDefinition.ProjectId, Constants.Collections.MappedFieldRows);

        // Build composite keys from project's master MappedRow list
        var projectMappedRowKeys = new HashSet<string>();
        var mappedrows = projectMappedRows.FirstOrDefault()?.MappedFields;
        foreach (var mappedRow in mappedrows)
        {
            if (mappedRow?.FieldByDataSource == null) continue;

            foreach (var field in mappedRow.FieldByDataSource)
            {
                if (field.Value != null && !string.IsNullOrEmpty(field.Value.FieldName))
                {
                    var key = $"{field.Value.DataSourceId}:{field.Value.FieldName.ToLowerInvariant()}";
                    projectMappedRowKeys.Add(key);
                }
            }
        }

        // Check all fields in match definition criteria
        foreach (var definition in matchDefinition.Definitions)
        {
            if (definition?.Criteria == null) continue;

            foreach (var criterion in definition.Criteria)
            {
                if (criterion?.MappedRow?.FieldsByDataSource == null) continue;

                foreach (var field in criterion.MappedRow.FieldsByDataSource)
                {
                    if (field.Value != null && !string.IsNullOrEmpty(field.Value.Name))
                    {
                        var key = $"{field.Value.DataSourceId}:{field.Value.Name.ToLowerInvariant()}";

                        if (!projectMappedRowKeys.Contains(key))
                        {
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }
}