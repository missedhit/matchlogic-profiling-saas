using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;

/// <summary>
/// Rule for merging multiple source columns into a single output column with a delimiter
/// </summary>
public class MergeFieldsTransformationRule : TransformationRule
{
    private readonly List<string> _sourceColumns;
    private readonly string _outputColumn;
    private readonly string _delimiter;
    private readonly int _onlyFirstNotEmptyCount;
    private readonly bool _skipEmpty;
    private readonly bool _trimValues;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly ILogger _logger;

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => _sourceColumns;

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule (merge operations should happen after basic text cleaning)
    /// </summary>
    public override int Priority => 250;

    public override IEnumerable<string> OutputColumns => new[] { _outputColumn };

    /// <summary>
    /// Creates a new merge fields transformation rule
    /// </summary>
    /// <param name="sourceColumns">List of source columns to merge</param>
    /// <param name="outputColumn">Name of the output column</param>
    /// <param name="delimiter">Delimiter to use between merged values (default: space)</param>
    /// <param name="onlyFirstNotEmptyCount">Limit of non-empty values to merge (0 = no limit)</param>
    /// <param name="skipEmpty">Whether to skip empty/null values (default: true)</param>
    /// <param name="trimValues">Whether to trim values before merging (default: true)</param>
    /// <param name="logger">Logger for diagnostic information</param>
    /// <param name="dependencies">Optional dependencies on other transformation rules</param>
    public MergeFieldsTransformationRule(
        List<string> sourceColumns,
        string outputColumn,
        string delimiter = " ",
        int onlyFirstNotEmptyCount = 0,
        bool skipEmpty = true,
        bool trimValues = true,
        ILogger logger = null,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumns = sourceColumns ?? throw new ArgumentNullException(nameof(sourceColumns));
        _outputColumn = outputColumn ?? throw new ArgumentNullException(nameof(outputColumn));
        _delimiter = delimiter ?? " ";
        _onlyFirstNotEmptyCount = onlyFirstNotEmptyCount;
        _skipEmpty = skipEmpty;
        _trimValues = trimValues;
        _logger = logger;
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        if (_sourceColumns.Count == 0)
        {
            throw new ArgumentException("At least one source column must be specified", nameof(sourceColumns));
        }

        _logger?.LogDebug(
            "Created MergeFieldsTransformationRule: Sources=[{Sources}], Output={Output}, Delimiter='{Delimiter}'",
            string.Join(", ", _sourceColumns),
            _outputColumn,
            _delimiter);
    }

    /// <summary>
    /// Determines if this rule can be applied to the given record
    /// </summary>
    public override bool CanApply(Record record)
    {
        // Can apply if at least one source column exists
        return true;
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public override void Apply(Record record)
    {
        try
        {
            var values = new List<string>();
            var collectedCount = 0;

            // Collect values from source columns
            foreach (var sourceColumn in _sourceColumns)
            {
                // Check if we've reached the limit
                if (_onlyFirstNotEmptyCount > 0 && collectedCount >= _onlyFirstNotEmptyCount)
                {
                    break;
                }

                if (record.HasColumn(sourceColumn))
                {
                    var column = record[sourceColumn];
                    if (column?.Value != null)
                    {
                        var value = column.Value.ToString();

                        // Trim if requested
                        if (_trimValues)
                        {
                            value = value.Trim();
                        }

                        // Skip empty values if requested
                        if (_skipEmpty && string.IsNullOrEmpty(value))
                        {
                            continue;
                        }

                        values.Add(value);

                        // Only increment counter for non-empty values
                        if (!string.IsNullOrEmpty(value))
                        {
                            collectedCount++;
                        }
                    }
                    else if (!_skipEmpty)
                    {
                        // Include null/empty if not skipping
                        values.Add(string.Empty);
                    }
                }
            }

            // Merge values with delimiter
            var mergedValue = string.Join(_delimiter, values);

            // Create or update output column
            record.AddColumn(_outputColumn, mergedValue);

            _logger?.LogTrace(
                "Merged {SourceCount} columns into {OutputColumn}: '{MergedValue}'",
                values.Count,
                _outputColumn,
                mergedValue);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error applying MergeFieldsTransformationRule to record");
            throw;
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        var sourceList = string.Join(", ", _sourceColumns);
        return $"MergeFieldsTransformationRule: [{sourceList}] -> {_outputColumn} (Delimiter: '{_delimiter}', ID: {Id})";
    }
}
