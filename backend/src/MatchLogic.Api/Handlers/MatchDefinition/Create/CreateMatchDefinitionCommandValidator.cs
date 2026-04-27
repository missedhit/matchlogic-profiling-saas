using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Project;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.MatchConfiguration;

namespace MatchLogic.Api.Handlers.MatchDefinition.Create;

public class CreateMatchDefinitionCommandValidator : AbstractValidator<CreateMatchDefinitionCommand>
{
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly IMatchConfigurationService _matchConfigurationService;

    public CreateMatchDefinitionCommandValidator(IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IMatchConfigurationService matchConfigurationService)
    {
        _dataSourceRepository = dataSourceRepository;
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        _matchConfigurationService = matchConfigurationService;

        // MatchDefinition must exist
        RuleFor(x => x.MatchDefinition)
            .NotNull()
            .WithMessage("Match definition is required.");

        // Basic validations for MatchDefinition
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
        });

        // Validate Criteria inside each Definition
        RuleForEach(x => x.MatchDefinition.Definitions)
             .SetValidator(new MatchDefinitionMappedRowValidator());

        When(x => x.MatchSetting != null && x.MatchSetting.IsProbabilistic, () =>
        {
            RuleFor(x => x.MatchDefinition.Definitions)
                .Must(HaveRequiredProbabilisticCriteria)
                .WithMessage("Probabilistic matching requires at least one exact match criteria and one fuzzy match criteria");
        });

        When(x => x.MatchDefinition?.Definitions != null, () =>
        {
            RuleForEach(y => y.MatchDefinition.Definitions)
                .Must(NoDataSourceFieldDuplicatesWithinDefinition)
                .WithMessage("Each field can only be used once within the same definition");
        });

        // Validate that all data sources participate in each criterion
        When(x => x.MatchDefinition?.Definitions != null, () =>
        {
            RuleFor(x => x)
                .MustAsync(AllDataSourcesMustParticipateInCriteria)
                .WithMessage("All data sources must participate in each match definition criterion");
        });
    }

    private async Task<bool> DataSourcePairsMustExistForProject(
    CreateMatchDefinitionCommand command,
    CancellationToken cancellationToken)
    {
        if (command.MatchDefinition?.ProjectId == null ||
            command.MatchDefinition.ProjectId == Guid.Empty)
            return true; // Let the ProjectId validation handle this

        var pairs = await _matchConfigurationService.GetDataSourcePairsByProjectIdAsync(command.MatchDefinition.ProjectId);

        return pairs != null && pairs.Count > 0;
    }
    private async Task<bool> AllDataSourcesMustParticipateInCriteria(
        CreateMatchDefinitionCommand command,
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


public class MatchDefinitionMappedRowValidator : AbstractValidator<MatchDefinitionMappedRowDto>
{
    public MatchDefinitionMappedRowValidator()
    {

        // Criteria validation
        RuleFor(x => x.Criteria)
            .NotEmpty()
            .WithMessage("At least one criteria must be provided");

        // Validate each criterion
        RuleForEach(x => x.Criteria)
            .SetValidator(new MatchCriteriaValidator());
    }
}

public class MatchCriteriaValidator : AbstractValidator<MatchCriterionMappedRowDto>
{
    public MatchCriteriaValidator()
    {
        RuleFor(x => x.MappedRow)
            .NotEmpty()
            .WithMessage("Mapped row is required");

        When(x => x.MappedRow != null, () =>
        {
            RuleFor(x => x.MappedRow.FieldsByDataSource)
                .NotEmpty()
                .WithMessage("At least one field must be mapped");

            // Validate that all mapped fields have valid names
            RuleFor(x => x.MappedRow.FieldsByDataSource)
                .Must(HaveValidFieldNames)
                .WithMessage("All mapped fields must have valid names");
        });

        RuleFor(x => x.Weight)
            .NotEmpty()
            .Must(numericValue => numericValue > 0 && numericValue <= 1)
            .WithMessage("Weight must be between 0 and 1");

        // Fuzzy Text validation
        When(x => x.DataType == CriteriaDataType.Text && x.MatchingType == MatchingType.Fuzzy, () =>
        {
            RuleFor(x => x.Arguments)
                .Must(HaveFuzzyTextArguments)
                .WithMessage("For Fuzzy Text matching, Fast Level and Level arguments are required");

            RuleFor(x => x.Arguments)
                .Must(HaveValidFuzzyTextRanges)
                .WithMessage("Fast Level and Level must be between 0 and 1");
        });

        // Fuzzy Numeric validation
        When(x => x.DataType == CriteriaDataType.Number && x.MatchingType == MatchingType.Fuzzy, () =>
        {
            RuleFor(x => x.Arguments)
                .Must(HaveNumericFuzzyArguments)
                .WithMessage("For Fuzzy Numeric matching, LowerLimit and UpperLimit arguments are required");

            RuleFor(x => x.Arguments)
                .Must(HaveValidNumericRanges)
                .WithMessage("Lower Limit and Upper Limit must be valid numbers");
        });

        // Fuzzy Phonetic validation
        When(x => x.DataType == CriteriaDataType.Phonetic && x.MatchingType == MatchingType.Fuzzy, () =>
        {
            RuleFor(x => x.Arguments)
                .Must(HavePhoneticFuzzyArguments)
                .WithMessage("For Fuzzy Phonetic matching, Phonetic Rating argument is required");

            RuleFor(x => x.Arguments)
                .Must(HaveValidPhoneticRange)
                .WithMessage("Phonetic Rating must be between 0 and 1");
        });

        // Exact match validations (no arguments required)
        When(x => x.MatchingType == MatchingType.Exact, () =>
        {
            RuleFor(x => x.Arguments)
                .Must(BeEmptyOrNull)
                .WithMessage("No arguments should be provided for Exact matching");
        });
    }

    private bool HaveValidFieldNames(Dictionary<string, FieldDto> fieldsByDataSource)
    {
        if (fieldsByDataSource == null || !fieldsByDataSource.Any()) return false;

        return fieldsByDataSource.Values.All(field =>
            field != null && !string.IsNullOrEmpty(field.Name));
    }

    private bool BeEmptyOrNull(Dictionary<ArgsValue, string> arguments)
    {
        return arguments == null || !arguments.Any();
    }

    private bool HaveFuzzyTextArguments(Dictionary<ArgsValue, string> arguments)
    {
        return arguments != null &&
               arguments.ContainsKey(ArgsValue.FastLevel) &&
               arguments.ContainsKey(ArgsValue.Level);
    }

    private bool HaveValidFuzzyTextRanges(Dictionary<ArgsValue, string> arguments)
    {
        if (!HaveFuzzyTextArguments(arguments)) return false;

        return IsValidRange(arguments[ArgsValue.FastLevel]) &&
               IsValidRange(arguments[ArgsValue.Level]);
    }

    private bool HaveNumericFuzzyArguments(Dictionary<ArgsValue, string> arguments)
    {
        var usePercentage = Convert.ToBoolean(arguments[ArgsValue.UsePercentage]);

        return usePercentage ? arguments != null &&
               arguments.ContainsKey(ArgsValue.LowerPercentage) &&
               arguments.ContainsKey(ArgsValue.UpperPercentage) :
               arguments != null &&
               arguments.ContainsKey(ArgsValue.LowerLimit) &&
               arguments.ContainsKey(ArgsValue.UpperLimit);

    }

    private bool HaveValidNumericRanges(Dictionary<ArgsValue, string> arguments)
    {
        if (!HaveNumericFuzzyArguments(arguments)) return false;

        var usePercentage = Convert.ToBoolean(arguments[ArgsValue.UsePercentage]);

        return usePercentage ? double.TryParse(arguments[ArgsValue.LowerPercentage].ToString(), out _) &&
               double.TryParse(arguments[ArgsValue.UpperPercentage].ToString(), out _) :
                double.TryParse(arguments[ArgsValue.LowerLimit].ToString(), out _) &&
               double.TryParse(arguments[ArgsValue.UpperLimit].ToString(), out _);
    }

    private bool HavePhoneticFuzzyArguments(Dictionary<ArgsValue, string> arguments)
    {
        return arguments != null && arguments.ContainsKey(ArgsValue.PhoneticRating);
    }

    private bool HaveValidPhoneticRange(Dictionary<ArgsValue, string> arguments)
    {
        if (!HavePhoneticFuzzyArguments(arguments)) return false;

        return IsValidRange(arguments[ArgsValue.PhoneticRating]);
    }

    private bool IsValidRange(object value)
    {
        if (double.TryParse(value.ToString(), out double numericValue))
        {
            return numericValue >= 0 && numericValue <= 1;
        }
        return false;
    }
}
