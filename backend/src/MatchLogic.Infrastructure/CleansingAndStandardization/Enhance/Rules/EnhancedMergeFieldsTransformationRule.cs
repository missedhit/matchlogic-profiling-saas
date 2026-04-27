using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
/// <summary>
/// Enhanced rule for merging multiple columns into a single column
/// </summary>
public class EnhancedMergeFieldsTransformationRule : EnhancedTransformationRule
{
    private readonly List<string> _sourceColumns;
    private readonly string _targetColumn;
    private readonly string _delimiter;
    private readonly bool _skipEmpty;
    private readonly bool _trimValues;
    private readonly int _onlyFirstNotEmptyCount;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly ILogger _logger;

    /// <summary>
    /// Columns that this rule reads from (inputs)
    /// </summary>
    public override IEnumerable<string> InputColumns => _sourceColumns;

    /// <summary>
    /// Columns that this rule writes to (outputs)
    /// </summary>
    public override IEnumerable<string> OutputColumns => new[] { _targetColumn };

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => _sourceColumns.Concat(new[] { _targetColumn });

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule (MergeFields runs after other transformations)
    /// </summary>
    public override int Priority { get; set; } = 250;

    /// <summary>
    /// Whether this rule creates new columns
    /// </summary>
    public override bool CreatesNewColumns => true;

    /// <summary>
    /// Creates a new enhanced merge fields rule
    /// </summary>
    /// <param name="sourceColumns">List of columns to merge</param>
    /// <param name="targetColumn">Target column name for merged result</param>
    /// <param name="logger">Logger instance</param>
    /// <param name="delimiter">Delimiter to use between values (default: space)</param>
    /// <param name="skipEmpty">Whether to skip empty values (default: true)</param>
    /// <param name="trimValues">Whether to trim values before merging (default: true)</param>
    /// <param name="onlyFirstNotEmptyCount">If > 0, only take first N non-empty values (default: 0 = take all)</param>
    /// <param name="dependencies">Optional rule dependencies</param>
    public EnhancedMergeFieldsTransformationRule(
        List<string> sourceColumns,
        string targetColumn,
        ILogger logger,
        string delimiter = " ",
        bool skipEmpty = true,
        bool trimValues = true,
        int onlyFirstNotEmptyCount = 0,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumns = sourceColumns ?? throw new ArgumentNullException(nameof(sourceColumns));
        _targetColumn = targetColumn ?? throw new ArgumentNullException(nameof(targetColumn));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _delimiter = delimiter ?? " ";
        _skipEmpty = skipEmpty;
        _trimValues = trimValues;
        _onlyFirstNotEmptyCount = onlyFirstNotEmptyCount;
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        if (_sourceColumns.Count == 0)
        {
            throw new ArgumentException("At least one source column is required", nameof(sourceColumns));
        }
    }

    /// <summary>
    /// Creates a new enhanced merge fields rule from configuration dictionary
    /// </summary>
    public EnhancedMergeFieldsTransformationRule(
        List<string> sourceColumns,
        string targetColumn,
        Dictionary<string, string> config,
        ILogger logger,
        IEnumerable<Guid> dependencies = null)
        : this(
            sourceColumns,
            targetColumn,
            logger,
            config?.GetValueOrDefault("delimiter", " ") ?? " ",
            ParseBool(config?.GetValueOrDefault("skipEmpty"), true),
            ParseBool(config?.GetValueOrDefault("trimValues"), true),
            ParseInt(config?.GetValueOrDefault("onlyFirstNotEmptyCount"), 0),
            dependencies)
    {
    }

    private static bool ParseBool(string value, bool defaultValue)
    {
        if (string.IsNullOrEmpty(value))
            return defaultValue;
        return bool.TryParse(value, out var result) ? result : defaultValue;
    }

    private static int ParseInt(string value, int defaultValue)
    {
        if (string.IsNullOrEmpty(value))
            return defaultValue;
        return int.TryParse(value, out var result) ? result : defaultValue;
    }

    /// <summary>
    /// Determines if this rule can be applied to the given record
    /// </summary>
    public override bool CanApply(Record record)
    {
        // Can apply if at least one source column exists
        return _sourceColumns.Any(column => record.HasColumn(column));
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public override void Apply(Record record)
    {
        try
        {
            var values = new List<string>();
            int notEmptyCount = 0;

            foreach (var columnName in _sourceColumns)
            {
                // Check if we've reached the limit for non-empty values
                if (_onlyFirstNotEmptyCount > 0 && notEmptyCount >= _onlyFirstNotEmptyCount)
                {
                    break;
                }

                string value = null;

                if (record.HasColumn(columnName))
                {
                    var column = record[columnName];
                    if (column?.Value != null)
                    {
                        value = column.Value.ToString();

                        if (_trimValues)
                        {
                            value = value.Trim();
                        }
                    }
                }

                bool isEmpty = string.IsNullOrEmpty(value);

                if (_skipEmpty && isEmpty)
                {
                    continue;
                }

                if (!isEmpty)
                {
                    notEmptyCount++;
                }

                values.Add(value ?? string.Empty);
            }

            // Join values with delimiter
            var mergedValue = string.Join(_delimiter, values);

            // Add or update target column
            record.AddColumn(_targetColumn, mergedValue);

            _logger.LogTrace(
                "Merged {SourceCount} columns into {TargetColumn}: '{MergedValue}'",
                _sourceColumns.Count,
                _targetColumn,
                mergedValue.Length > 50 ? mergedValue.Substring(0, 50) + "..." : mergedValue);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error merging columns [{SourceColumns}] into {TargetColumn}",
                string.Join(", ", _sourceColumns),
                _targetColumn);

            // Ensure target column exists even on error
            if (!record.HasColumn(_targetColumn))
            {
                record.AddColumn(_targetColumn, string.Empty);
            }
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"EnhancedMergeFieldsTransformationRule: [{string.Join(", ", _sourceColumns)}] -> {_targetColumn} (Delimiter: '{_delimiter}') (ID: {Id})";
    }
}
