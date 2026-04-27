using MatchLogic.Application.Common;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.Transformation.Parsers;
using MatchLogic.Parsers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
/// <summary>
/// Enhanced rule for parsing full names into components
/// Outputs: Prefix, FirstName, MiddleName, LastName, Suffix, CommonName, Gender
/// Uses lock-free design with object pooling for high-throughput parallel processing
/// </summary>
public class EnhancedFullNameTransformationRule : EnhancedTransformationRule
{
    private readonly string _sourceColumn;
    private readonly Dictionary<string, string> _outputMappings;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly ILogger _logger;
    private readonly FullNameParserOptimized _fullNameParser;
    private readonly FirstNameParser _firstNameParser;

    // Output component names
    public const string PrefixComponent = "Prefix";
    public const string FirstNameComponent = "FirstName";
    public const string MiddleNameComponent = "MiddleName";
    public const string LastNameComponent = "LastName";
    public const string SuffixComponent = "Suffix";
    public const string CommonNameComponent = "CommonName";
    public const string GenderComponent = "Gender";

    /// <summary>
    /// Standard output component names for full name parsing
    /// </summary>
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
    /// Columns that this rule reads from (inputs)
    /// </summary>
    public override IEnumerable<string> InputColumns => new[] { _sourceColumn };

    /// <summary>
    /// Columns that this rule writes to (outputs)
    /// </summary>
    public override IEnumerable<string> OutputColumns => _outputMappings.Values;

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns =>
        new[] { _sourceColumn }.Concat(_outputMappings.Values);

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule (parsers run after basic text cleaning)
    /// </summary>
    public override int Priority { get; set; } = 200;

    /// <summary>
    /// Whether this rule creates new columns
    /// </summary>
    public override bool CreatesNewColumns => true;

    /// <summary>
    /// Creates a new enhanced full name transformation rule
    /// </summary>
    /// <param name="sourceColumn">Source column containing the full name</param>
    /// <param name="outputMappings">Dictionary mapping component names to output column names</param>
    /// <param name="fullNameParser">Thread-safe full name parser with object pooling</param>
    /// <param name="firstNameParser">Thread-safe first name parser for CommonName/Gender</param>
    /// <param name="logger">Logger instance</param>
    /// <param name="dependencies">Optional rule dependencies</param>
    public EnhancedFullNameTransformationRule(
        string sourceColumn,
        Dictionary<string, string> outputMappings,
        FullNameParserOptimized fullNameParser,
        FirstNameParser firstNameParser,
        ILogger logger,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _fullNameParser = fullNameParser ?? throw new ArgumentNullException(nameof(fullNameParser));
        _firstNameParser = firstNameParser ?? throw new ArgumentNullException(nameof(firstNameParser));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _outputMappings = outputMappings ?? new Dictionary<string, string>();
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        // Ensure default output mappings for all components
        if (_outputMappings.Count == 0)
        {
            var baseColumnName = _sourceColumn ?? "FullName";
            foreach (var component in FullNameComponents)
            {
                _outputMappings[component] = $"{baseColumnName}_{component}";
            }
        }
    }

    /// <summary>
    /// Determines if this rule can be applied to the given record
    /// </summary>
    public override bool CanApply(Record record)
    {
        return record.HasColumn(_sourceColumn);
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// Lock-free implementation for parallel processing
    /// </summary>
    public override void Apply(Record record)
    {
        try
        {
            // Collect and concatenate values from all source columns
            var fullNameBuilder = new StringBuilder();

            //foreach (var columnName in _sourceColumn)
            //{
                if (record.HasColumn(_sourceColumn))
                {
                    var column = record[_sourceColumn];
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
            //}

            var fullName = fullNameBuilder.ToString().Trim();

            if (string.IsNullOrWhiteSpace(fullName))
            {
                AddEmptyOutputColumns(record);
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
                AddEmptyOutputColumns(record);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying FullNameTransformationRule");
        }
    }    

    /// <summary>
    /// Adds an output value to the record
    /// </summary>
    private void AddOutputValue(Record record, string componentName, string value)
    {
        if (_outputMappings.TryGetValue(componentName, out var targetColumn) &&
            !string.IsNullOrEmpty(targetColumn))
        {
            record.AddColumn(targetColumn, value ?? string.Empty);
        }
    }

    /// <summary>
    /// Adds empty values for all output columns
    /// </summary>
    private void AddEmptyOutputColumns(Record record)
    {
        foreach (var component in FullNameComponents)
        {
            AddOutputValue(record, component, string.Empty);
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"EnhancedFullNameTransformationRule: {_sourceColumn} -> {_outputMappings.Count} outputs (ID: {Id})";
    }
}
