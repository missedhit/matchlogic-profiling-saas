using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Common;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Transformation.Parsers;

/// <summary>
/// Thread-safe parser for first names with gender and common name lookup
/// Optimized for high-throughput parallel processing
/// </summary>
public class FirstNameParser
{
    private readonly ILogger<FirstNameParser> _logger;
    private readonly SemaphoreSlim _loadLock = new SemaphoreSlim(1, 1);

    // Use IReadOnlyDictionary for thread-safe concurrent reads
    private IReadOnlyDictionary<string, string> _commonNamesDictionary;
    private IReadOnlyDictionary<string, string> _genderDictionary;

    private volatile bool _dictionariesLoaded = false;

    

    // Resource names matching legacy system
    private const string CommonNamesResourceName = "MatchLogic.Infrastructure.Resources.CommonNames.xml";
    private const string GenderResourceName = "MatchLogic.Infrastructure.Resources.Gender.xml";

    public FirstNameParser(ILogger<FirstNameParser> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Gets the common (full) form of a first name
    /// Thread-safe - no locks needed for reads
    /// </summary>
    public string GetCommonName(string firstName)
    {
        if (string.IsNullOrWhiteSpace(firstName))
            return string.Empty;

        EnsureDictionariesLoaded();

        var key = firstName.Trim();

        // Direct dictionary access - thread-safe for concurrent reads
        if (_commonNamesDictionary.TryGetValue(key, out var commonName))
        {
            return ApplyProperCase(commonName);
        }

        // Return the original name in proper case if not found
        return ApplyProperCase(key);
    }

    /// <summary>
    /// Gets the gender associated with a first name
    /// Thread-safe - no locks needed for reads
    /// </summary>
    public string GetGender(string firstName)
    {
        if (string.IsNullOrWhiteSpace(firstName))
            return Constants.GenderConstants.GenderUndefined;

        EnsureDictionariesLoaded();

        var key = firstName.Trim();

        // Direct dictionary access - thread-safe for concurrent reads
        if (_genderDictionary.TryGetValue(key, out var gender))
        {
            return gender;
        }

        return Constants.GenderConstants.GenderUndefined;
    }

    /// <summary>
    /// Ensures dictionaries are loaded (thread-safe initialization)
    /// Only locks during initial load, not during reads
    /// </summary>
    public async Task LoadDictionariesAsync()
    {
        if (_dictionariesLoaded)
            return;

        await _loadLock.WaitAsync();
        try
        {
            // Double-check after acquiring lock
            if (_dictionariesLoaded)
                return;

            _logger.LogInformation("Loading first name dictionaries from encrypted resources...");

            var sw = System.Diagnostics.Stopwatch.StartNew();

            // Load into temporary dictionaries
            var commonNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var genders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Try loading from custom file first (if exists)
            string customCommonNamesFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Data", "CommonNames.txt");

            if (File.Exists(customCommonNamesFile))
            {
                _logger.LogDebug("Loading common names from custom file: {FileName}", customCommonNamesFile);
                await LoadCommonNamesFromFileAsync(commonNames, customCommonNamesFile);
            }
            else
            {
                _logger.LogDebug("Loading common names from encrypted resource: {ResourceName}", CommonNamesResourceName);
                await LoadCommonNamesFromResourceAsync(commonNames);
            }

            // Load gender dictionary (always from resource)
            _logger.LogDebug("Loading gender dictionary from encrypted resource: {ResourceName}", GenderResourceName);
            await LoadGenderDictionaryAsync(genders);

            // Assign as readonly - this makes them truly immutable and thread-safe
            _commonNamesDictionary = commonNames;
            _genderDictionary = genders;

            // Mark as loaded (volatile ensures visibility across threads)
            _dictionariesLoaded = true;

            sw.Stop();

            _logger.LogInformation(
                "Successfully loaded {CommonNamesCount} common names and {GenderCount} gender entries in {ElapsedMs}ms",
                _commonNamesDictionary.Count,
                _genderDictionary.Count,
                sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load first name dictionaries");

            // Initialize empty dictionaries to prevent crashes
            _commonNamesDictionary = new Dictionary<string, string>();
            _genderDictionary = new Dictionary<string, string>();
            _dictionariesLoaded = true;

            throw;
        }
        finally
        {
            _loadLock.Release();
        }
    }

    private void EnsureDictionariesLoaded()
    {
        if (!_dictionariesLoaded)
        {
            LoadDictionariesAsync().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Loads common names from a plain text file
    /// </summary>
    private async Task LoadCommonNamesFromFileAsync(Dictionary<string, string> dictionary, string fileName)
    {
        await Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(fileName))
                {
                    string line;
                    int lineNumber = 0;

                    while ((line = reader.ReadLine()) != null)
                    {
                        lineNumber++;

                        // Skip comments and empty lines
                        if (line.StartsWith("#") || string.IsNullOrWhiteSpace(line))
                            continue;

                        var parts = line.Split(',');
                        if (parts.Length >= 2)
                        {
                            var firstName = parts[0].Trim();
                            var formOf = parts[1].Trim();

                            if (!string.IsNullOrEmpty(firstName) && !string.IsNullOrEmpty(formOf))
                            {
                                dictionary[firstName] = formOf;
                            }
                        }
                        else
                        {
                            _logger.LogWarning("Invalid line {LineNumber} in {FileName}: {Line}",
                                lineNumber, fileName, line);
                        }
                    }
                }

                _logger.LogDebug("Loaded {Count} common names from file", dictionary.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading common names file: {FileName}", fileName);
                throw;
            }
        });
    }

    /// <summary>
    /// Loads common names from encrypted, compressed resource using legacy ResourceHelper pattern
    /// </summary>
    private async Task LoadCommonNamesFromResourceAsync(Dictionary<string, string> dictionary)
    {
        await Task.Run(() =>
        {
            try
            {
                var table = new DataTable("CommonNames");

                // Use legacy ResourceHelper to load encrypted/compressed resource
                if (!ResourceHelper.LoadTableFromResourcesXml(table, CommonNamesResourceName))
                {
                    throw new InvalidOperationException(
                        $"Cannot find or load resource: {CommonNamesResourceName}");
                }

                // Parse the DataTable into dictionary
                foreach (DataRow row in table.Rows)
                {
                    try
                    {
                        if (row["FirstName"] != DBNull.Value && row["Form Of"] != DBNull.Value)
                        {
                            var firstName = row["FirstName"].ToString().Trim();
                            var formOf = row["Form Of"].ToString().Trim();

                            if (!string.IsNullOrEmpty(firstName) && !string.IsNullOrEmpty(formOf))
                            {
                                // Use case-insensitive key
                                dictionary[firstName] = formOf;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Error parsing common name row");
                    }
                }

                _logger.LogDebug("Loaded {Count} common names from resource", dictionary.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading common names from resource: {ResourceName}",
                    CommonNamesResourceName);
                throw;
            }
        });
    }

    /// <summary>
    /// Loads gender dictionary from encrypted, compressed resource using legacy ResourceHelper pattern
    /// </summary>
    private async Task LoadGenderDictionaryAsync(Dictionary<string, string> dictionary)
    {
        await Task.Run(() =>
        {
            try
            {
                var table = new DataTable("Gender");

                // Use legacy ResourceHelper to load encrypted/compressed resource
                if (!ResourceHelper.LoadTableFromResourcesXml(table, GenderResourceName))
                {
                    throw new InvalidOperationException(
                        $"Cannot find or load resource: {GenderResourceName}");
                }

                // Parse the DataTable into dictionary
                foreach (DataRow row in table.Rows)
                {
                    try
                    {
                        if (row["FirstName"] != DBNull.Value && row["Gender"] != DBNull.Value)
                        {
                            var firstName = row["FirstName"].ToString().Trim();
                            var gender = row["Gender"].ToString().Trim();

                            if (!string.IsNullOrEmpty(firstName) && !string.IsNullOrEmpty(gender))
                            {
                                // Use case-insensitive key
                                dictionary[firstName] = gender;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Error parsing gender row");
                    }
                }

                _logger.LogDebug("Loaded {Count} gender entries from resource", dictionary.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading gender dictionary from resource: {ResourceName}",
                    GenderResourceName);
                throw;
            }
        });
    }

    /// <summary>
    /// Applies proper case to a name (thread-safe, no state)
    /// </summary>
    private static string ApplyProperCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        System.Globalization.TextInfo text = System.Globalization.CultureInfo.CurrentCulture.TextInfo;
        String result = text.ToTitleCase(input.ToLower());

        return result;
    }
}