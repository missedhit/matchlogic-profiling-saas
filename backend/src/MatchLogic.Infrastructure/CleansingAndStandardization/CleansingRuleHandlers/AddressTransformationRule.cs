using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.Parser.AddressParser;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;

/// <summary>
/// Rule for parsing an address from multiple input columns and outputting to multiple address component fields
/// </summary>
public class AddressTransformationRule : TransformationRule, IDisposable
{
    private readonly string[] _sourceColumns;
    private readonly Dictionary<string, string> _outputMappings;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly ILogger _logger;

    // Thread-safe instance of AddressParser
    private readonly AddressParser _addressParser;
    private readonly SemaphoreSlim _parserLock = new SemaphoreSlim(1, 1);

    // Output mapping constants to match AddressOutputParts from the AddressBlock
    private static readonly string[] AddressComponents = new[]
    {
        "Recipient",
        "StreetNumber",
        "PreDirection",
        "Street",
        "PostDirection",
        "StreetSuffix",
        "SecondaryAddressUnit",
        "SecondaryAddressUnitNumber",
        "City",
        "State",
        "ZipCode",
        "Zip9Code",
        "Country",
        "Box",
        "BoxNumber"
    };

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => _sourceColumns;

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule (address parsers generally run after basic text cleaning)
    /// </summary>
    public override int Priority => 200;
    public override IEnumerable<string> OutputColumns => _outputMappings.Values;

    /// <summary>
    /// Creates a new address transformation rule
    /// </summary>
    /// <param name="sourceColumns">The source columns containing address information</param>
    /// <param name="outputMappings">Dictionary mapping address component names to target column names</param>
    /// <param name="logger">Logger for diagnostic information</param>
    /// <param name="dependencies">Optional dependencies on other transformation rules</param>
    public AddressTransformationRule(
        string[] sourceColumns,
        Dictionary<string, string> outputMappings,
        ILogger logger,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumns = sourceColumns ?? throw new ArgumentNullException(nameof(sourceColumns));
        _outputMappings = outputMappings ?? new Dictionary<string, string>();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        // Create a single reusable AddressParser instance
        _addressParser = new AddressParser();

        // Ensure we have default output mappings for components if not specified
        foreach (var component in AddressComponents)
        {
            if (!_outputMappings.ContainsKey(component))
            {
                _outputMappings[component] = $"Address_{component}";
            }
        }

        _logger.LogDebug("Created AddressTransformationRule with {SourceCount} source columns and {MappingCount} output mappings",
            _sourceColumns.Length, _outputMappings.Count);
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
    /// </summary>
    public override void Apply(Record record)
    {
        try
        {
            // Collect values from source columns
            var addressLines = new List<string>();

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
                            addressLines.Add(value);
                        }
                    }
                }
            }

            if (addressLines.Count == 0)
            {
                AddNullOutputs(record);
                return;
            }
            // Parse address if we have at least one line
           
                try
                {
                    // Acquire lock to ensure thread safety when using the parser
                    _parserLock.Wait();

                    try
                    {
                        // Reset parser state before parsing
                        _addressParser.ResetValues();

                        // Parse the address
                        _addressParser.Parse(addressLines, 30);

                        // Map address components to output columns
                        MapAddressComponentsToOutputColumns(record);
                    }
                    finally
                    {
                        // Always release the lock
                        _parserLock.Release();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while parsing address with lines: {AddressLines}",
                        string.Join(", ", addressLines));
                    throw;
                }
            
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying AddressTransformationRule");
        }
    }

    /// <summary>
    /// Maps the parsed address components to the specified output columns
    /// </summary>
    private void MapAddressComponentsToOutputColumns(Record record)
    {
        // Map each component to its target column
        AddOutputValue(record, "Recipient", _addressParser.Recipient);
        AddOutputValue(record, "StreetNumber", _addressParser.StreetNumber);
        AddOutputValue(record, "PreDirection", _addressParser.PreDirection);
        AddOutputValue(record, "Street", _addressParser.Street);
        AddOutputValue(record, "PostDirection", _addressParser.PostDirection);
        AddOutputValue(record, "StreetSuffix", _addressParser.StreetSuffix);
        AddOutputValue(record, "SecondaryAddressUnit", _addressParser.SecondaryAddressUnit);
        AddOutputValue(record, "SecondaryAddressUnitNumber", _addressParser.SecondaryAddressUnitNumber);
        AddOutputValue(record, "City", _addressParser.City);
        AddOutputValue(record, "State", _addressParser.State);
        AddOutputValue(record, "ZipCode", _addressParser.ZipCode);
        AddOutputValue(record, "Zip9Code", _addressParser.Zip9Code);
        AddOutputValue(record, "Country", _addressParser.Country);
        AddOutputValue(record, "Box", _addressParser.Box);
        AddOutputValue(record, "BoxNumber", _addressParser.BoxNumber);
    }

    /// <summary>
    /// Adds an output value to the record for a specific address component
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
    /// Disposes of resources used by this rule
    /// </summary>
    public void Dispose()
    {
        _parserLock?.Dispose();
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"AddressTransformationRule: [{string.Join(", ", _sourceColumns)}] -> {_outputMappings.Count} outputs (ID: {Id})";
    }

    private void AddNullOutputs(Record record)
    {
        foreach (var outputColumn in _outputMappings.Values)
        {
            record.AddColumn(outputColumn, null);
        }
    }
}
