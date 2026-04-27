using MatchLogic.Parsers;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.RegexRule;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;
using MatchLogic.Infrastructure.Transformation.Parsers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization;

/// <summary>
/// Factory for creating transformation rules from configuration
/// </summary>
public class RuleFactory : IRuleFactory
{
    private readonly ILogger<RuleFactory> _logger;
    private readonly WordSmithDictionaryLoader _wordSmithDictionaryLoader;
    private readonly IWordSmithDictionaryService _dictionaryService;
    private readonly FirstNameParser _firstNameParser;
    private readonly FullNameParserOptimized _fullNameParser;
    private readonly ProperCaseOptions _properCaseOptions;
    private readonly AbbreviationParser _abbreviationParser;
    /// <summary>
    /// Creates a new rule factory
    /// </summary>
    public RuleFactory(ILogger<RuleFactory> logger,
        WordSmithDictionaryLoader wordSmithDictionaryLoader,
        IWordSmithDictionaryService dictionaryService,
        FirstNameParser firstNameParser,
        FullNameParserOptimized fullNameParser,
        ProperCaseOptions properCaseOptions,
        AbbreviationParser abbreviationParser)
    {
        _logger = logger;
        _wordSmithDictionaryLoader = wordSmithDictionaryLoader;
        _dictionaryService = dictionaryService;
        _firstNameParser = firstNameParser;
        _fullNameParser = fullNameParser;
        _properCaseOptions = properCaseOptions;
        _abbreviationParser = abbreviationParser;

    }

    /// <summary>
    /// Creates a transformation rule from a cleaning rule
    /// </summary>
    public TransformationRule CreateFromCleaningRule(CleaningRule rule)
    {
        try
        {
            // Create text transformation rule for standard cleaning operations
            return new TextTransformationRule(
                rule.ColumnName,
                rule.RuleType,
                rule.Arguments,
                _properCaseOptions,
                _abbreviationParser);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating transformation rule from cleaning rule for column {Column}", rule.ColumnName);
            return null;
        }
    }

    /// <summary>
    /// Creates transformation rules from an extended cleaning rule
    /// </summary>
    public IEnumerable<TransformationRule> CreateFromExtendedCleaningRule(ExtendedCleaningRule rule)
    {
        try
        {
            var rules = new List<TransformationRule>();

            // Add column mappings (copy fields)
            foreach (var mapping in rule.ColumnMappings)
            {
                if (!string.IsNullOrEmpty(mapping.TargetColumn))
                {
                    // Create dependency on the text rule
                    var copyRule = new CopyFieldRule(
                        rule.ColumnName,
                        mapping.TargetColumn);

                    rules.Add(copyRule);
                }
            }

            return rules;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating transformation rules from extended cleaning rule for column {Column}", rule.ColumnName);
            return Enumerable.Empty<TransformationRule>();
        }
    }

    /// <summary>
    /// Creates a transformation rule from a mapping rule
    /// </summary>
    public TransformationRule CreateFromMappingRule(MappingRule rule)
    {
        try
        {
            switch (rule.OperationType)
            {
                case MappingOperationType.AddressParser:

                    return new AddressTransformationRule(
                        rule.SourceColumn.ToArray(),
                        rule.OutputColumns == null ? new Dictionary<string, string>() : rule.OutputColumns.ToDictionary(x => x),
                        _logger);
                case MappingOperationType.Zip:
                    return CreateZipCodeRule(rule);
                case MappingOperationType.FullNameParser:
                    return CreateFullNameRule(rule);
                case MappingOperationType.FirstNameExtractor:
                    return CreateFirstNameRule(rule);
                case MappingOperationType.RegexPattern:
                    if (rule.MappingConfig.TryGetValue("pattern", out var pattern))
                    {
                        // Create a RegexTransformationRule with all configuration parameters
                        return new RegexTransformationRule(
                            rule.SourceColumn,
                            pattern,
                            rule.OutputColumns.Count > 0 ? rule.OutputColumns : null,
                            rule.MappingConfig);
                    }
                    _logger.LogWarning("No pattern specified for regex rule on columns {SourceColumns}",
                        string.Join(", ", rule.SourceColumn));
                    return null;
                case MappingOperationType.WordSmith:
                    return CreateWordSmithRule(rule, _logger).Result;

                case MappingOperationType.MergeFields:
                    return CreateMergeFieldsRule(rule, _logger);

                default:
                    _logger.LogWarning("Unsupported mapping operation type: {OperationType}", rule.OperationType);
                    return null;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating transformation rule from mapping rule for column {Column}", rule.SourceColumn);
            return null;
        }
    }


    /// <summary>
    /// Creates a full name transformation rule from a mapping rule
    /// </summary>
    private TransformationRule CreateFullNameRule(MappingRule mappingRule)
    {
        if (mappingRule == null)
            throw new ArgumentNullException(nameof(mappingRule));

        if (mappingRule.SourceColumn == null || mappingRule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for FullName rule");
            return null;
        }

        try
        {
            var outputMappings = new Dictionary<string, string>();

            // Map output columns based on the order specified
            // Expected order: [Prefix, FirstName, MiddleName, LastName, Suffix, CommonName, Gender]
            var components = new[]
            {
            FullNameTransformationRule.PrefixComponent,
            FullNameTransformationRule.FirstNameComponent,
            FullNameTransformationRule.MiddleNameComponent,
            FullNameTransformationRule.LastNameComponent,
            FullNameTransformationRule.SuffixComponent,
            FullNameTransformationRule.CommonNameComponent,
            FullNameTransformationRule.GenderComponent
        };

            if (mappingRule.OutputColumns != null && mappingRule.OutputColumns.Count > 0)
            {
                // Map each output column to its corresponding component
                for (int i = 0; i < Math.Min(components.Length, mappingRule.OutputColumns.Count); i++)
                {
                    if (!string.IsNullOrEmpty(mappingRule.OutputColumns[i]))
                    {
                        outputMappings[components[i]] = mappingRule.OutputColumns[i];
                    }
                }
            }
            else
            {
                // Use default output column names if not specified
                var baseColumnName = mappingRule.SourceColumn.First();
                foreach (var component in components)
                {
                    outputMappings[component] = $"{baseColumnName}_{component}";
                }
            }

            var rule = new FullNameTransformationRule(
                mappingRule.SourceColumn.ToArray(),
            outputMappings,
                _fullNameParser,
                _firstNameParser,
                _logger);

            _logger.LogInformation(
                "Created FullName rule for columns [{SourceColumns}] with outputs: {Outputs}",
                string.Join(", ", mappingRule.SourceColumn),
                string.Join(", ", outputMappings.Values));

            return rule;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating FullName rule for columns {Columns}",
                string.Join(", ", mappingRule.SourceColumn));
            return null;
        }
    }

    /// <summary>
    /// Creates a first name transformation rule from a mapping rule
    /// </summary>
    private TransformationRule CreateFirstNameRule(MappingRule mappingRule)
    {
        if (mappingRule == null)
            throw new ArgumentNullException(nameof(mappingRule));

        if (mappingRule.SourceColumn == null || mappingRule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for FirstName rule");
            return null;
        }

        try
        {
            var sourceColumn = mappingRule.SourceColumn.First();
            var outputMappings = new Dictionary<string, string>();

            // Map output columns based on the order specified
            // Expected order: [CommonName, Gender]
            if (mappingRule.OutputColumns != null && mappingRule.OutputColumns.Count > 0)
            {
                // First output column is for CommonName
                if (mappingRule.OutputColumns.Count > 0 && !string.IsNullOrEmpty(mappingRule.OutputColumns[0]))
                {
                    outputMappings[FirstNameTransformationRule.CommonNameComponent] = mappingRule.OutputColumns[0];
                }

                // Second output column is for Gender
                if (mappingRule.OutputColumns.Count > 1 && !string.IsNullOrEmpty(mappingRule.OutputColumns[1]))
                {
                    outputMappings[FirstNameTransformationRule.GenderComponent] = mappingRule.OutputColumns[1];
                }
            }
            else
            {
                // Use default output column names if not specified
                outputMappings[FirstNameTransformationRule.CommonNameComponent] = $"{sourceColumn}_CommonName";
                outputMappings[FirstNameTransformationRule.GenderComponent] = $"{sourceColumn}_Gender";
            }

            var rule = new FirstNameTransformationRule(
                sourceColumn,
                outputMappings,
                _firstNameParser,
                _logger);

            _logger.LogInformation(
                "Created FirstName rule for column {SourceColumn} with outputs: {Outputs}",
                sourceColumn,
                string.Join(", ", outputMappings.Values));

            return rule;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating FirstName rule for column {Column}",
                string.Join(", ", mappingRule.SourceColumn));
            return null;
        }
    }
    /// </summary>
    private async Task<TransformationRule> CreateWordSmithRule(MappingRule mappingRule, ILogger logger)
    {
        if (mappingRule == null)
            throw new ArgumentNullException(nameof(mappingRule));

        if (mappingRule.SourceColumn == null || mappingRule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for WordSmith rule");
            return null;
        }

        try
        {
            var config = mappingRule.MappingConfig ?? new Dictionary<string, string>();

            // Extract configuration values
            string separators = config.GetValueOrDefault("separators", " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`");
            int.TryParse(config.GetValueOrDefault("maxWordCount", "3"), out int maxWordCount);
            bool.TryParse(config.GetValueOrDefault("flagMode", "false"), out bool flagMode);
            bool.TryParse(config.GetValueOrDefault("includeFullText", "true"), out bool includeFullText);

            // Create enhanced WordSmith rule builder with dictionary service
            var builder = new WordSmithRuleBuilder(
                mappingRule.SourceColumn.First(),
                _logger,
                _wordSmithDictionaryLoader,
                _dictionaryService) // NEW: Pass dictionary service
                .WithSeparators(separators)
                .WithMaxWordCount(maxWordCount)
                .WithReplacementType(flagMode ? ReplacementType.Flag : ReplacementType.Full)
                .WithIncludeFullText(includeFullText);

            // Load dictionary - check for dictionary ID first, then file path
            // Load dictionary from database using dictionary ID
            if (config.TryGetValue("dictionaryId", out var dictionaryIdStr) &&
                Guid.TryParse(dictionaryIdStr, out var dictionaryId))
            {
                // Load rules from database using dictionary ID
                await builder.WithDictionaryIdAsync(dictionaryId);
            }
            else
            {
                _logger.LogError("No dictionary ID specified for WordSmith rule. Dictionary ID is required.");
                throw new InvalidOperationException("Dictionary ID is required for WordSmith rules. Please specify 'dictionaryId' in the mapping configuration.");
            }

            // Add output columns if specified
            if (mappingRule.OutputColumns != null && mappingRule.OutputColumns.Count > 0)
            {
                foreach (var outputColumn in mappingRule.OutputColumns)
                {
                    if (!string.IsNullOrEmpty(outputColumn))
                    {
                        // Add a placeholder word for new column creation
                        builder.AddNewColumn($"placeholder_{Guid.NewGuid():N}", outputColumn);
                    }
                }
            }

            // Build and return the enhanced rule
            var rule = builder.Build();

            _logger.LogInformation(
                "Created WordSmith rule for column {SourceColumn} using dictionary {DictionaryId}",
                mappingRule.SourceColumn.First(),
                dictionaryId);

            return rule;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating enhanced WordSmith rule for column {Column}",
                string.Join(", ", mappingRule.SourceColumn));
            return null;
        }
    }

    /// <summary>
    /// Creates a zip code transformation rule from a mapping rule
    /// </summary>
    private TransformationRule CreateZipCodeRule(MappingRule mappingRule)
    {
        if (mappingRule == null)
            throw new ArgumentNullException(nameof(mappingRule));

        if (mappingRule.SourceColumn == null || mappingRule.SourceColumn.Count == 0)
        {
            _logger.LogWarning("Source column is required for ZipCode rule");
            return null;
        }

        try
        {
            var sourceColumn = mappingRule.SourceColumn.First();
            var outputMappings = new Dictionary<string, string>();

            // Map output columns based on the order specified
            // Expected order: [Zip5, Zip4, FullZip, IsValid]
            var components = new[]
            {
                ZipCodeTransformationRule.ZipAComponent,
                ZipCodeTransformationRule.ZipBComponent,
                ZipCodeTransformationRule.CountryComponent,
            };

            if (mappingRule.OutputColumns != null && mappingRule.OutputColumns.Count > 0)
            {
                // Map each output column to its corresponding component
                for (int i = 0; i < Math.Min(components.Length, mappingRule.OutputColumns.Count); i++)
                {
                    if (!string.IsNullOrEmpty(mappingRule.OutputColumns[i]))
                    {
                        outputMappings[components[i]] = mappingRule.OutputColumns[i];
                    }
                }
            }
            else
            {
                // Use default output column names if not specified
                foreach (var component in components)
                {
                    outputMappings[component] = $"{sourceColumn}_{component}";
                }
            }


            var rule = new ZipCodeTransformationRule(
                sourceColumn,
                outputMappings,
                logger: _logger);

            _logger.LogInformation(
                "Created ZipCode rule for column {SourceColumn} with outputs: {Outputs}",
                sourceColumn,
                string.Join(", ", outputMappings.Values));

            return rule;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating ZipCode rule for column {Column}",
                string.Join(", ", mappingRule.SourceColumn));
            return null;
        }
    }

    /// <summary>
    /// Creates a merge fields transformation rule from a MappingRule
    /// </summary>
    /// <param name="factory">The rule factory instance</param>
    /// <param name="rule">Mapping rule with OperationType = MergeFields</param>
    /// <param name="logger">Logger for diagnostic information</param>
    /// <returns>MergeFieldsTransformationRule or null</returns>
    private TransformationRule CreateMergeFieldsRule(
        MappingRule rule,
        ILogger logger)
    {
        try
        {
            // Validate input
            if (rule.SourceColumn == null || rule.SourceColumn.Count == 0)
            {
                logger?.LogWarning("No source columns specified for merge field rule");
                return null;
            }

            // Get output column name (use first output column or generate one)
            string outputColumn = rule.OutputColumns?.FirstOrDefault()
                ?? $"Merged_{string.Join("_", rule.SourceColumn.Take(3))}";

            // Extract configuration from MappingConfig
            var config = rule.MappingConfig ?? new Dictionary<string, string>();

            string delimiter = config.GetValueOrDefault("delimiter", " ");
            int onlyFirstNotEmptyCount = 0;
            bool skipEmpty = true;
            bool trimValues = true;

            if (config.TryGetValue("onlyFirstNotEmptyCount", out var countStr))
            {
                int.TryParse(countStr, out onlyFirstNotEmptyCount);
            }

            if (config.TryGetValue("skipEmpty", out var skipEmptyStr))
            {
                bool.TryParse(skipEmptyStr, out skipEmpty);
            }

            if (config.TryGetValue("trimValues", out var trimStr))
            {
                bool.TryParse(trimStr, out trimValues);
            }

            logger?.LogInformation(
                "Creating MergeFieldsTransformationRule: Sources=[{Sources}], Output={Output}",
                string.Join(", ", rule.SourceColumn),
                outputColumn);

            return new MergeFieldsTransformationRule(
                rule.SourceColumn,
                outputColumn,
                delimiter,
                onlyFirstNotEmptyCount,
                skipEmpty,
                trimValues,
                logger);
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Error creating merge fields transformation rule");
            return null;
        }
    }
}
