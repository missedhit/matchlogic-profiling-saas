using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.RegexRule;

/// <summary>
/// Rule for applying regular expressions to extract data from fields
/// </summary>
public class RegexTransformationRule : TransformationRule
{
    private readonly List<string> _sourceColumns;
    private readonly string _regexPattern;
    private readonly List<string> _outputColumns;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly Regex _regex;
    private readonly IRegexProcessingStrategy _processingStrategy;

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => _sourceColumns;

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    public override IEnumerable<string> OutputColumns => _outputColumns;
    /// <summary>
    /// Gets the priority of this rule (regex transformations should happen after basic text cleaning)
    /// </summary>
    public override int Priority => 200;

    /// <summary>
    /// Creates a new regex transformation rule
    /// </summary>
    public RegexTransformationRule(
        List<string> sourceColumns,
        string regexPattern,
        List<string> outputColumns = null,
        Dictionary<string, string> configParams = null,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumns = sourceColumns ?? new List<string>();
        _regexPattern = regexPattern;

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
                _processingStrategy = new AdvancedRegexProcessingStrategy(
                    _outputColumns,
                    columnMapping,
                    namedGroupMapping,
                    outputFormat);
            }
            else
            {
                _processingStrategy = new BasicRegexProcessingStrategy(_outputColumns);
            }
        }
        catch (ArgumentException ex)
        {
            throw new InvalidOperationException($"Invalid regex pattern: {ex.Message}", ex);
        }

        _dependencies = dependencies ?? Enumerable.Empty<Guid>();
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
        // Process each source column
        foreach (var sourceColumn in _sourceColumns)
        {
            if (!record.HasColumn(sourceColumn))
                continue;

            var column = record[sourceColumn];

            if (column?.Value == null)
            {
                foreach (var outputColumn in _outputColumns)
                {
                    record.AddColumn(outputColumn, null);
                }
                continue;
            }

            var value = column.Value.ToString();

            if (string.IsNullOrEmpty(value))
            {
                foreach (var outputColumn in _outputColumns)
                {
                    record.AddColumn(outputColumn, null);
                }
                continue;
            }

            // Apply regex to this column
            var match = _regex.Match(value);

            if (!match.Success)
            {
                // No match found, set output columns to null or empty
                foreach (var outputColumn in _outputColumns)
                {
                    record.AddColumn(outputColumn, null);
                }
                continue;
            }

            // Use strategy to process match
            _processingStrategy.ProcessMatch(record, match);

            // We only need one successful match, so break after first processed column
            break;
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"RegexTransformationRule: [{string.Join(", ", _sourceColumns)}] -> {string.Join(", ", _outputColumns)} (Pattern: '{_regexPattern}') (ID: {Id})";
    }
}
