using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.Transformation.Parsers;
using MatchLogic.Parsers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Application.Common;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;

/// <summary>
/// Rule for parsing full names into components (Prefix, First, Middle, Last, Suffix, CommonName, Gender)
/// Optimized for high-throughput parallel processing using object pooling
/// NO LOCKS during Apply() - processes millions of records efficiently
/// </summary>
public class FullNameTransformationRule : TransformationRule
{
    private readonly string[] _sourceColumns;
    private readonly Dictionary<string, string> _outputMappings;
    private readonly FullNameParserOptimized _fullNameParser;
    private readonly FirstNameParser _firstNameParser;
    private readonly ILogger _logger;

    // Output component names
    public const string PrefixComponent = "Prefix";
    public const string FirstNameComponent = "FirstName";
    public const string MiddleNameComponent = "MiddleName";
    public const string LastNameComponent = "LastName";
    public const string SuffixComponent = "Suffix";
    public const string CommonNameComponent = "CommonName";
    public const string GenderComponent = "Gender";

    private static readonly string[] FullNameComponents = new[]
    {
        PrefixComponent,
        FirstNameComponent,
        MiddleNameComponent,
        LastNameComponent,
        SuffixComponent,
        CommonNameComponent,
        GenderComponent
    };

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => _sourceColumns;

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => Enumerable.Empty<Guid>();

    /// <summary>
    /// Gets the priority of this rule (name parsers run after basic text cleaning)
    /// </summary>
    public override int Priority => 200;

    public override IEnumerable<string> OutputColumns => _outputMappings.Values;

    /// <summary>
    /// Creates a new full name transformation rule
    /// </summary>
    public FullNameTransformationRule(
        string[] sourceColumns,
        Dictionary<string, string> outputMappings,
        FullNameParserOptimized fullNameParser,
        FirstNameParser firstNameParser,
        ILogger logger)
    {
        _sourceColumns = sourceColumns ?? throw new ArgumentNullException(nameof(sourceColumns));
        _outputMappings = outputMappings ?? new Dictionary<string, string>();
        _fullNameParser = fullNameParser ?? throw new ArgumentNullException(nameof(fullNameParser));
        _firstNameParser = firstNameParser ?? throw new ArgumentNullException(nameof(firstNameParser));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Ensure we have default output mappings if not specified
        if (_outputMappings.Count == 0)
        {
            var baseColumnName = _sourceColumns.Length > 0 ? _sourceColumns[0] : "FullName";
            foreach (var component in FullNameComponents)
            {
                _outputMappings[component] = $"{baseColumnName}_{component}";
            }
        }

        _logger.LogDebug(
            "Created FullNameTransformationRule for columns [{SourceColumns}] with {MappingCount} output mappings",
            string.Join(", ", _sourceColumns), _outputMappings.Count);
    }

    /// <summary>
    /// Determines if this rule can be applied to the given record
    /// </summary>
    public override bool CanApply(Record record)
    {
        // Check if at least one source column exists
        return _sourceColumns.Any(column => record.HasColumn(column));
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// Thread-safe and lock-free - uses object pooling for maximum performance
    /// </summary>
    public override void Apply(Record record)
    {
        try
        {
            // Collect and concatenate values from all source columns
            var fullNameBuilder = new StringBuilder();

            foreach (var columnName in _sourceColumns)
            {
                if (record.HasColumn(columnName))
                {
                    var column = record[columnName];
                    if (column?.Value != null)
                    {
                        var value = column.Value.ToString();
                        if (!string.IsNullOrWhiteSpace(value))
                        {
                            if (fullNameBuilder.Length > 0)
                            {
                                fullNameBuilder.Append(" ");
                            }
                            fullNameBuilder.Append(value.Trim());
                        }
                    }
                }
            }

            var fullName = fullNameBuilder.ToString().Trim();

            if (string.IsNullOrWhiteSpace(fullName))
            {
                AddNullOutputs(record);
                return;
            }

            try
            {
                // Parse the full name (NO LOCK - uses object pooling internally)
                var parseResult = _fullNameParser.Parse(fullName);

                // Get common name and gender from first name
                string commonName = string.Empty;
                string gender = Constants.GenderConstants.GenderUndefined;

                if (!string.IsNullOrWhiteSpace(parseResult.FirstName))
                {
                    // NO LOCK - FirstNameParser uses thread-safe read-only dictionaries
                    commonName = _firstNameParser.GetCommonName(parseResult.FirstName);
                    gender = _firstNameParser.GetGender(parseResult.FirstName);
                }

                // If gender is still undefined, try to determine from prefix
                if (gender == Constants.GenderConstants.GenderUndefined &&
                    !string.IsNullOrWhiteSpace(parseResult.Prefix))
                {
                    // Static method - thread-safe
                    gender = _fullNameParser.GetGenderFromPrefix(parseResult.Prefix);
                }

                // Add all output columns
                AddOutputValue(record, PrefixComponent, parseResult.Prefix);
                AddOutputValue(record, FirstNameComponent, parseResult.FirstName);
                AddOutputValue(record, MiddleNameComponent, parseResult.MiddleName);
                AddOutputValue(record, LastNameComponent, parseResult.LastName);
                AddOutputValue(record, SuffixComponent, parseResult.Suffix);
                AddOutputValue(record, CommonNameComponent, commonName);
                AddOutputValue(record, GenderComponent, gender);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error parsing full name: '{FullName}'", fullName);
                AddNullOutputs(record);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying FullNameTransformationRule");
        }
    }

    /// <summary>
    /// Adds an output value to the record for a specific component
    /// </summary>
    private void AddOutputValue(Record record, string componentName, string value)
    {
        if (_outputMappings.TryGetValue(componentName, out var targetColumn) &&
            !string.IsNullOrEmpty(targetColumn))
        {
            record.AddColumn(targetColumn, value);
        }
    }

    /// <summary>
    /// Adds null values for all outputs when parsing fails or input is invalid
    /// </summary>
    private void AddNullOutputs(Record record)
    {
        foreach (var component in FullNameComponents)
        {
            AddOutputValue(record, component, null);
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"FullNameTransformationRule: [{string.Join(", ", _sourceColumns)}] -> {_outputMappings.Count} outputs (ID: {Id})";
    }
}
