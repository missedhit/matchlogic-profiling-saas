using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
internal class EnhancedZipCodeTransformationRule: EnhancedTransformationRule

{
    private readonly string _sourceColumn;
    private readonly Dictionary<string, string> _outputMappings;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly ILogger _logger;


    // Country regex patterns
    private readonly List<CountryRegex> _countryRegexes;

    // Output component names
    public const string ZipAComponent = "ZipA";
    public const string ZipBComponent = "ZipB";
    public const string CountryComponent = "Country";

    private static readonly string[] ZipCodeComponent = new[]
   {
        ZipAComponent,
        ZipBComponent,
        CountryComponent
    };
    /// <summary>
    /// Columns that this rule reads from (inputs)
    /// </summary>
    public override IEnumerable<string> InputColumns => new[] { _sourceColumn };

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
    public override int Priority => 200;

    public override IEnumerable<string> OutputColumns => _outputMappings.Values;

    /// <summary>
    /// Whether this rule creates new columns
    /// </summary>
    public override bool CreatesNewColumns => true;
    /// <summary>
    /// Creates a new zip code transformation rule
    /// </summary>
    public EnhancedZipCodeTransformationRule(
         string sourceColumn,
        Dictionary<string, string> outputMappings,
        ILogger logger,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));        
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        // Default output columns if not provided
        _outputMappings ??= new Dictionary<string, string>
        {
            { ZipAComponent, $"{sourceColumn}_{ZipAComponent}" },
            { ZipBComponent, $"{sourceColumn}_{ZipBComponent}" },
            { CountryComponent, $"{sourceColumn}_{CountryComponent}" }
        };


        // Initialize country regex patterns
        _countryRegexes = new List<CountryRegex>
        {
            // USA: 5 digits or 5+4 format (e.g., 12345 or 12345-6789)
            new CountryRegex("USA", @"(\d{4,5}-\d{4})"),
            new CountryRegex("USA", @"(\d{4,5})"),
            
            // Canada: Format A1A 1A1 (letter-digit-letter space digit-letter-digit)
            new CountryRegex("Canada", @"([A-Z]\d[A-Z] *\d[A-Z]\d)"),
            
            // UK: Complex format with various patterns
            new CountryRegex("UK",
                @"([A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]? {1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA)")
        };

        _logger?.LogDebug(
            "Created ZipCodeTransformationRule for column '{SourceColumn}' with {OutputCount} outputs",
            _sourceColumn, _outputMappings.Count);
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public override void Apply(Record record)
    {
        if (!record.HasColumn(_sourceColumn))
            return;

        var column = record[_sourceColumn];
        if (column?.Value == null)
        {
            AddNullOutputs(record);
            return;
        }

        try
        {
            var inputValue = column.Value.ToString();

            if (string.IsNullOrWhiteSpace(inputValue))
            {
                AddNullOutputs(record);
                return;
            }

            // Extract zip code and determine country
            var zip = ExtractZip(inputValue, out string country, out bool extracted);

            if (!extracted)
            {
                AddNullOutputs(record);
                return;
            }

            // Parse based on country
            if (country.Equals("USA", StringComparison.OrdinalIgnoreCase))
            {
                ParseUSAZipCode(record, zip);
            }
            else if (country.Equals("Canada", StringComparison.OrdinalIgnoreCase))
            {
                ParseCanadaZipCode(record, zip);
            }
            else
            {
                // For UK and other countries, store in ZipA
                AddOutputValue(record, ZipAComponent, zip);
                AddOutputValue(record, ZipBComponent, null);
            }

            // Always add country
            AddOutputValue(record, CountryComponent, country);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error parsing zip code from column '{Column}'", _sourceColumn);
            AddNullOutputs(record);
        }
    }

    /// <summary>
    /// Parses USA zip codes into ZipA (5 digits) and ZipB (4 digits for ZIP+4)
    /// </summary>
    private void ParseUSAZipCode(Record record, string zip)
    {
        // Split USA zip code into parts (12345-6789 -> 12345 and 6789)
        string[] parts = zip.Split(new[] { '-', ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length > 0)
        {
            string partA = parts[0];

            // Pad with leading zero if 4 digits (e.g., 0123)
            if (partA.Length == 4)
            {
                partA = $"0{partA}";
            }

            AddOutputValue(record, ZipAComponent, partA); // ZipA
        }

        if (parts.Length > 1)
        {
            string partB = parts[1];

            // Pad ZIP+4 with leading zeros to make it 4 digits
            while (partB.Length < 4)
            {
                partB = $"0{partB}";
            }

            AddOutputValue(record, ZipBComponent, partB); // ZipB
        }
        else
        {
            AddOutputValue(record, ZipBComponent, null); // No ZIP+4
        }
    }

    /// <summary>
    /// Parses Canadian postal codes and ensures proper format (A1A 1A1)
    /// </summary>
    private void ParseCanadaZipCode(Record record, string zip)
    {
        // For Canada, ensure space separator (A1A1A1 -> A1A 1A1)
        if (!zip.Contains(" ") && zip.Length == 6)
        {
            zip = zip.Insert(3, " ");
        }

        AddOutputValue(record, ZipAComponent, zip); // ZipA
        AddOutputValue(record, ZipBComponent, null); // Canada doesn't use ZipB
    }

    /// <summary>
    /// Extracts zip code from input value and determines country
    /// </summary>
    private string ExtractZip(string value, out string country, out bool extracted)
    {
        value = value.ToUpper();

        string zip = string.Empty;
        country = string.Empty;
        extracted = false;

        // Pad short values with leading zeros for USA zip codes
        if (value.Length >= 3 && value.Length < 5 && value.All(char.IsDigit))
        {
            while (value.Length < 5)
            {
                value = $"0{value}";
            }
        }

        // Try each country regex pattern
        foreach (var countryRegex in _countryRegexes)
        {
            zip = countryRegex.ExtractZip(value, out extracted);

            if (extracted)
            {
                country = countryRegex.CountryName;
                break;
            }
        }

        return zip;
    }

    /// <summary>
    /// Adds an output value to the record
    /// </summary>
    private void AddOutputValue(Record record, string componentName, string value)
    {
        //if (index < _outputMappings.Count)
        //{
        var outputName = _outputMappings[componentName];
        if (!string.IsNullOrEmpty(outputName))
        {
            record.AddColumn(outputName, value);
        }
        //}
    }

    /// <summary>
    /// Adds null values for all outputs when parsing fails
    /// </summary>
    private void AddNullOutputs(Record record)
    {

        for (int i = 0; i < ZipCodeComponent.Length; i++)
        {
            AddOutputValue(record, ZipCodeComponent[i], null);
        }
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"ZipCodeTransformationRule: {_sourceColumn} -> [{string.Join(", ", _outputMappings)}] (ID: {Id})";
    }
}
