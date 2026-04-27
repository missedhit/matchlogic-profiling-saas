using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.RegexRule;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
/// <summary>
/// Enhanced rule for performing regex-based transformations
/// </summary>
public class EnhancedRegexTransformationRule : EnhancedTransformationRule
{
    private readonly List<string> _sourceColumns;
    private readonly string _regexPattern;
    private readonly List<string> _outputColumns;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly Regex _regex;
    private readonly IRegexProcessingStrategy _processingStrategy;
    private readonly ILogger _logger;

    /// <summary>
    /// Columns that this rule reads from (inputs)
    /// </summary>
    public override IEnumerable<string> InputColumns => _sourceColumns;

    /// <summary>
    /// Columns that this rule writes to (outputs)
    /// </summary>
    public override IEnumerable<string> OutputColumns => _outputColumns;

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => _sourceColumns.Concat(_outputColumns);

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule (regex transformations should happen after basic text cleaning)
    /// </summary>
    public override int Priority { get; set; } = 200;

    /// <summary>
    /// Whether this rule creates new columns
    /// </summary>
    public override bool CreatesNewColumns => _outputColumns.Any();

    /// <summary>
    /// Creates a new enhanced regex transformation rule
    /// </summary>
    public EnhancedRegexTransformationRule(
        List<string> sourceColumns,
        string regexPattern,
        ILogger logger,
        List<string> outputColumns = null,
        Dictionary<string, string> configParams = null,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumns = sourceColumns ?? new List<string>();
        _regexPattern = regexPattern ?? throw new ArgumentNullException(nameof(regexPattern));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        try
        {
            _regex = new Regex(regexPattern, RegexOptions.IgnorePatternWhitespace | RegexOptions.ExplicitCapture);

            // If no output columns provided, use group names from the regex pattern
            if (outputColumns == null || outputColumns.Count == 0)
            {
                var groupNames = _regex.GetGroupNames();
                // Skip the first group (which is the entire match)
                _outputColumns = new List<string>();
                for (int i = 1; i < groupNames.Length; i++)
                {
                    _outputColumns.Add(groupNames[i]);
                }
            }
            else
            {
                _outputColumns = outputColumns;
            }

            // Initialize processing strategy
            _processingStrategy = CreateProcessingStrategy(configParams);
        }
        catch (ArgumentException ex)
        {
            var message = $"Invalid regex pattern '{regexPattern}': {ex.Message}";
            _logger.LogError(ex, message);
            throw new InvalidOperationException(message, ex);
        }

        _dependencies = dependencies ?? Enumerable.Empty<Guid>();
    }

    /// <summary>
    /// Creates the appropriate processing strategy based on configuration
    /// </summary>
    private IRegexProcessingStrategy CreateProcessingStrategy(Dictionary<string, string> configParams)
    {
        // Check for advanced functionality
        bool useAdvancedFunctionality = false;
        Dictionary<int, int> columnMapping = null;
        Dictionary<int, string> namedGroupMapping = null;
        string outputFormat = null;

        if (configParams != null)
        {
            // Check for advanced functionality flag
            if (configParams.TryGetValue("useAdvancedFunctionality", out var advFlag))
            {
                bool.TryParse(advFlag, out useAdvancedFunctionality);
            }

            // Get output format
            if (configParams.TryGetValue("outputFormat", out var format))
            {
                outputFormat = format;
            }

            // Process column mappings
            if (useAdvancedFunctionality)
            {
                columnMapping = new Dictionary<int, int>();
                namedGroupMapping = new Dictionary<int, string>();

                for (int i = 0; i < _outputColumns.Count; i++)
                {
                    string mappingKey = $"mapping_{i}";
                    if (configParams.TryGetValue(mappingKey, out var mappingStr))
                    {
                        // Check if mapping refers to a named group
                        if (mappingStr.StartsWith("name:"))
                        {
                            string groupName = mappingStr.Substring(5);
                            namedGroupMapping[i] = groupName;
                        }
                        else if (int.TryParse(mappingStr, out int groupIndex))
                        {
                            columnMapping[i] = groupIndex;
                        }
                        else
                        {
                            columnMapping[i] = i + 1; // Default to sequential mapping
                        }
                    }
                    else
                    {
                        columnMapping[i] = i + 1; // Default to sequential mapping
                    }
                }
            }
        }

        // Create appropriate strategy based on configuration
        if (useAdvancedFunctionality)
        {
            return new AdvancedRegexProcessingStrategy(
                _outputColumns,
                columnMapping,
                namedGroupMapping,
                outputFormat);
        }
        else
        {
            return new BasicRegexProcessingStrategy(_outputColumns);
        }
    }

    /// <summary>
    /// Determines if this rule can be applied to the given record
    /// </summary>
    public override bool CanApply(Record record)
    {
        // Can apply if at least one source column exists
        return _sourceColumns.Any(record.HasColumn);
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public override void Apply(Record record)
    {
        try
        {
            bool matchFound = false;

            // Process each source column
            foreach (var sourceColumn in _sourceColumns)
            {
                if (!record.HasColumn(sourceColumn))
                {
                    _logger.LogDebug("Source column {SourceColumn} not found in record", sourceColumn);
                    continue;
                }

                var column = record[sourceColumn];
                if (column?.Value == null)
                {
                    _logger.LogDebug("Source column {SourceColumn} has null value", sourceColumn);
                    continue;
                }

                var value = column.Value.ToString();

                if (string.IsNullOrEmpty(value))
                {
                    _logger.LogDebug("Source column {SourceColumn} has empty value", sourceColumn);
                    continue;
                }

                // Apply regex to this column
                var match = _regex.Match(value);

                if (!match.Success)
                {
                    _logger.LogDebug("Regex pattern did not match value in column {SourceColumn}", sourceColumn);
                    continue;
                }

                // Use strategy to process match
                _processingStrategy.ProcessMatch(record, match);
                matchFound = true;

                // We only need one successful match, so break after first processed column
                break;
            }

            // If no match found across all columns, set output columns to null or empty
            if (!matchFound)
            {
                _logger.LogDebug("No regex matches found in any source columns");
                AddEmptyOutputColumns(record);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying regex transformation rule with pattern {Pattern}", _regexPattern);

            // Ensure output columns are created even on error
            AddEmptyOutputColumns(record);
        }
    }

    /// <summary>
    /// Adds empty values for all output columns
    /// </summary>
    private void AddEmptyOutputColumns(Record record)
    {
        foreach (var outputColumn in _outputColumns)
        {
            if (!record.HasColumn(outputColumn))
            {
                record.AddColumn(outputColumn, null);
            }
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"EnhancedRegexTransformationRule: [{string.Join(", ", _sourceColumns)}] -> {string.Join(", ", _outputColumns)} (Pattern: '{_regexPattern}') (ID: {Id})";
    }
}
