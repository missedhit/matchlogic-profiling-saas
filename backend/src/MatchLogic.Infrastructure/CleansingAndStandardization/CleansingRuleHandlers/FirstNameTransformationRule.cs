using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.Transformation.Parsers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;

/// <summary>
/// Rule for parsing first names to extract common name and gender
/// Optimized for high-throughput parallel processing - NO LOCKS during Apply()
/// </summary>
public class FirstNameTransformationRule : TransformationRule
{
    private readonly string _sourceColumn;
    private readonly Dictionary<string, string> _outputMappings;
    private readonly FirstNameParser _firstNameParser;
    private readonly ILogger _logger;

    // Output component names
    public const string CommonNameComponent = "CommonName";
    public const string GenderComponent = "Gender";

    private static readonly string[] FirstNameComponents = new[]
    {
        CommonNameComponent,
        GenderComponent
    };

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => new[] { _sourceColumn };

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => Enumerable.Empty<Guid>();

    /// <summary>
    /// Gets the priority of this rule
    /// </summary>
    public override int Priority => 200;

    public override IEnumerable<string> OutputColumns => _outputMappings.Values;

    /// <summary>
    /// Creates a new first name transformation rule
    /// </summary>
    public FirstNameTransformationRule(
        string sourceColumn,
        Dictionary<string, string> outputMappings,
        FirstNameParser firstNameParser,
        ILogger logger)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _outputMappings = outputMappings ?? new Dictionary<string, string>();
        _firstNameParser = firstNameParser ?? throw new ArgumentNullException(nameof(firstNameParser));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Ensure we have default output mappings if not specified
        if (_outputMappings.Count == 0)
        {
            _outputMappings[CommonNameComponent] = $"{sourceColumn}_{CommonNameComponent}";
            _outputMappings[GenderComponent] = $"{sourceColumn}_{GenderComponent}";
        }

        _logger.LogDebug(
            "Created FirstNameTransformationRule for column '{SourceColumn}' with {MappingCount} output mappings",
            _sourceColumn, _outputMappings.Count);
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
    /// Thread-safe and lock-free for maximum performance
    /// </summary>
    public override void Apply(Record record)
    {
        if (!record.HasColumn(_sourceColumn))
        {
            AddNullOutputs(record);
            return;
        }

        var column = record[_sourceColumn];
        if (column?.Value == null)
        {
            AddNullOutputs(record);
            return;
        }

        var firstName = column.Value.ToString().Trim();

        if (string.IsNullOrWhiteSpace(firstName))
        {
            AddNullOutputs(record);
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
            AddNullOutputs(record);
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
        foreach (var component in FirstNameComponents)
        {
            AddOutputValue(record, component, null);
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"FirstNameTransformationRule: {_sourceColumn} -> {_outputMappings.Count} outputs (ID: {Id})";
    }
}