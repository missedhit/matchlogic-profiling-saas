using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.Parser.AddressParser;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
/// <summary>
/// Enhanced address transformation rule
/// </summary>
public class EnhancedAddressTransformationRule : EnhancedTransformationRule, IDisposable
{
    private readonly string[] _sourceColumns;
    private readonly Dictionary<string, string> _outputMappings;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly AddressParser _addressParser;
    private readonly SemaphoreSlim _parserLock = new SemaphoreSlim(1, 1);
    private readonly ILogger _logger;

    public override IEnumerable<string> InputColumns => _sourceColumns;
    public override IEnumerable<string> OutputColumns => _outputMappings.Values;
    public override IEnumerable<string> AffectedColumns => _sourceColumns.Concat(_outputMappings.Values);
    public override IEnumerable<Guid> DependsOn => _dependencies;
    public override int Priority => 200;
    public override bool CreatesNewColumns => true;

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
    public EnhancedAddressTransformationRule(
        string[] sourceColumns,
        Dictionary<string, string> outputMappings,
        ILogger logger,
        IEnumerable<Guid> dependencies = null,
        string outputPrefix = "")
    {
        _sourceColumns = sourceColumns;
        _outputMappings = outputMappings;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        _addressParser = new AddressParser();

        // Ensure we have default output mappings for components if not specified
        foreach (var component in AddressComponents)
        {
            if (!_outputMappings.ContainsKey(component))
            {
                // If outputPrefix is provided, use: {outputPrefix}_Address_{component}
                // Otherwise, use default: Address_{component}
                if (!string.IsNullOrEmpty(outputPrefix))
                {
                    _outputMappings[component] = $"{outputPrefix}_Address_{component}";
                }
                else
                {
                    _outputMappings[component] = $"Address_{component}";
                }
            }
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

            // Parse address if we have at least one line
            if (addressLines.Count > 0)
            {
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
                    string.Join(", ", addressLines);
                    throw;
                }
            }
            else
            {

            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying AddressTransformationRule");
        }
    }

    public override bool CanApply(Record record)
    {
        // Check if at least one source column exists
        return _sourceColumns.Any(column => record.HasColumn(column));
    }
    public void Dispose()
    {
        _parserLock?.Dispose();
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"EnhancedAddressTransformationRule: [{string.Join(", ", _sourceColumns)}] -> {_outputMappings.Count} outputs (ID: {Id})";
    }
}
