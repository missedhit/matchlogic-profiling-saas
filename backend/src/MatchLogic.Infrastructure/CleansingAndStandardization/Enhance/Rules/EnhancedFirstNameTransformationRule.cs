using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.Transformation.Parsers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
/// <summary>
/// Enhanced rule for extracting first name information (CommonName and Gender)
/// Uses lock-free design with thread-safe read-only dictionaries
/// </summary>
public class EnhancedFirstNameTransformationRule : EnhancedTransformationRule
{
    private readonly string _sourceColumn;
    private readonly Dictionary<string, string> _outputMappings;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly ILogger _logger;
    private readonly FirstNameParser _firstNameParser;

    public const string CommonNameComponent = "CommonName";
    public const string GenderComponent = "Gender";

    /// <summary>
    /// Standard output component names for first name extraction
    /// </summary>
    private static readonly string[] FirstNameComponents = new[]
   {
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
    /// Creates a new enhanced first name transformation rule
    /// </summary>
    /// <param name="sourceColumn">Source column containing the first name</param>
    /// <param name="outputMappings">Dictionary mapping component names to output column names</param>
    /// <param name="firstNameParser">Thread-safe first name parser</param>
    /// <param name="logger">Logger instance</param>
    /// <param name="dependencies">Optional rule dependencies</param>
    public EnhancedFirstNameTransformationRule(
        string sourceColumn,
        Dictionary<string, string> outputMappings,
        FirstNameParser firstNameParser,
        ILogger logger,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _firstNameParser = firstNameParser ?? throw new ArgumentNullException(nameof(firstNameParser));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _outputMappings = outputMappings ?? new Dictionary<string, string>();
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        // Ensure default output mappings for all components
        if (_outputMappings.Count == 0)
        {
            _outputMappings[CommonNameComponent] = $"{sourceColumn}_{CommonNameComponent}";
            _outputMappings[GenderComponent] = $"{sourceColumn}_{GenderComponent}";
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
        if (!record.HasColumn(_sourceColumn))
        {
            AddEmptyOutputColumns(record);
            return;
        }

        var column = record[_sourceColumn];
        if (column?.Value == null)
        {
            AddEmptyOutputColumns(record);
            return;
        }

        var firstName = column.Value.ToString().Trim();

        if (string.IsNullOrWhiteSpace(firstName))
        {
            AddEmptyOutputColumns(record);
            return;
        }

        try
        {
            // NO LOCK NEEDED - Parser is thread-safe for reads
            // This allows millions of records to be processed in parallel
            var commonName = _firstNameParser.GetCommonName(firstName);
            var gender = _firstNameParser.GetGender(firstName);

            // Add output columns
            AddOutputValue(record, CommonNameComponent, commonName);
            AddOutputValue(record, GenderComponent, gender);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error parsing first name '{FirstName}'", firstName);
            AddEmptyOutputColumns(record);
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
        foreach (var component in FirstNameComponents)
        {
            AddOutputValue(record, component, string.Empty);
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"EnhancedFirstNameTransformationRule: {_sourceColumn} -> {_outputMappings.Count} outputs (ID: {Id})";
    }
}
