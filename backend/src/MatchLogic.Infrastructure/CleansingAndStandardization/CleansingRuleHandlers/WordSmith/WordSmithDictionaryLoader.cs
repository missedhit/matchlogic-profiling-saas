using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;

/// <summary>
/// Handles loading WordSmith dictionaries from files
/// </summary>
public class WordSmithDictionaryLoader
{
    private readonly ILogger<WordSmithDictionaryLoader> _logger;

    // Expected number of columns in a WordSmith dictionary
    private const int ExpectedColumnCount = 6;

    public WordSmithDictionaryLoader(ILogger<WordSmithDictionaryLoader> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Result of loading a WordSmith dictionary
    /// </summary>
    public class LoadDictionaryResult
    {
        /// <summary>
        /// Whether the dictionary was loaded successfully
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Dictionary of replacing steps by word
        /// </summary>
        public Dictionary<string, ReplacingStep> ReplacementsDictionary { get; set; }

        /// <summary>
        /// Dictionary of replacing steps that create new columns
        /// </summary>
        public Dictionary<string, ReplacingStep> NewColumnsDictionary { get; set; }

        /// <summary>
        /// Error messages encountered during loading
        /// </summary>
        public List<string> ErrorMessages { get; set; }
    }

    /// <summary>
    /// Loads a WordSmith dictionary from a file
    /// </summary>
    /// <param name="filePath">Path to the dictionary file</param>
    /// <param name="encoding">Encoding to use for reading the file (defaults to Unicode)</param>
    /// <param name="firstLineContainsHeaders">Whether the first line contains headers</param>
    /// <returns>Result containing the dictionaries and any error messages</returns>
    public LoadDictionaryResult LoadDictionary(
        string filePath,
        Encoding encoding = null,
        bool firstLineContainsHeaders = true)
    {
        var result = new LoadDictionaryResult
        {
            Success = false,
            ReplacementsDictionary = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase),
            NewColumnsDictionary = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase),
            ErrorMessages = new List<string>()
        };

        try
        {
            _logger.LogInformation($"Loading WordSmith dictionary from {filePath}");

            if (!File.Exists(filePath))
            {
                string errorMessage = $"Dictionary file not found: {filePath}";
                result.ErrorMessages.Add(errorMessage);
                _logger.LogError(errorMessage);
                return result;
            }

            // Use Unicode encoding by default (matching the original code)
            encoding = encoding ?? Encoding.Unicode;

            // Track duplicate keys to handle them properly
            var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int errorCount = 0;
            int maxErrorsToCollect = 500; // Match original code limit
            int rowIndex = 0;

            // Use StreamReader for better control over processing
            using (var streamReader = new StreamReader(filePath, encoding))
            {
                string line;
                bool firstLineSkipped = !firstLineContainsHeaders;

                while ((line = streamReader.ReadLine()) != null)
                {
                    // Skip header line if needed
                    if (!firstLineSkipped && firstLineContainsHeaders)
                    {
                        firstLineSkipped = true;
                        continue;
                    }

                    // Split line by tabs
                    var values = line.Split('\t');

                    // Check if line has the expected number of columns
                    if (values.Length == ExpectedColumnCount)
                    {
                        // Process the word (first column)
                        string word = values[0]?.Trim() ?? string.Empty;
                        word = word.ToLower(); // Convert to lowercase as in original code

                        // Check for duplicate keys
                        if (!string.IsNullOrEmpty(word))
                        {
                            if (keys.Contains(word))
                            {
                                // Log duplicate key
                                if (errorCount < maxErrorsToCollect)
                                {
                                    string errorMessage = $"Row No.{rowIndex} is duplicate. Another row with the same key already was loaded. This row will be passed. {line}";
                                    result.ErrorMessages.Add(errorMessage);
                                    _logger.LogWarning(errorMessage);
                                    errorCount++;
                                }

                                rowIndex++;
                                continue;
                            }

                            // Add to keys set
                            keys.Add(word);

                            // Create replacing step from values
                            var replacingStep = new ReplacingStep
                            {
                                Words = word
                            };

                            // Add replacement text (second column)
                            if (values.Length > 1 && values[1] != null)
                            {
                                replacingStep.Replacement = values[1].Trim();
                            }

                            // Add new column name (third column)
                            if (values.Length > 2 && values[2] != null)
                            {
                                replacingStep.NewColumnName = values[2].Trim();
                            }

                            // Parse ToDelete flag (fourth column)
                            if (values.Length > 3 && values[3] != null)
                            {
                                string toDeleteStr = values[3].Trim();

                                if (!string.IsNullOrEmpty(toDeleteStr) &&
                                    (toDeleteStr == "1" || toDeleteStr.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                                     toDeleteStr == "✓"))
                                {
                                    replacingStep.ToDelete = true;
                                }
                            }

                            // Parse priority (fifth column)
                            if (values.Length > 4 && values[4] != null)
                            {
                                string priorityStr = values[4].Trim();

                                if (!string.IsNullOrEmpty(priorityStr) && int.TryParse(priorityStr, out int priority))
                                {
                                    replacingStep.Priority = priority;
                                }
                            }

                            // Set character length property explicitly
                            // This isn't used directly in the rule but can be useful for debugging
                            var charLength = word.Length;

                            // Add to dictionaries
                            result.ReplacementsDictionary[word] = replacingStep;

                            if (!string.IsNullOrEmpty(replacingStep.NewColumnName))
                            {
                                result.NewColumnsDictionary[word] = replacingStep;
                            }
                        }
                    }
                    else
                    {
                        // Line doesn't have expected number of columns
                        if (errorCount < maxErrorsToCollect)
                        {
                            string errorMessage = $"Incorrect row No.{rowIndex} (number of columns is not equal to {ExpectedColumnCount}): {line}";
                            result.ErrorMessages.Add(errorMessage);
                            _logger.LogWarning(errorMessage);
                            errorCount++;
                        }
                    }

                    rowIndex++;
                }
            }

            // Mark as successful if we processed at least one row
            result.Success = result.ReplacementsDictionary.Count > 0;

            _logger.LogInformation($"Loaded {result.ReplacementsDictionary.Count} entries from dictionary with {result.ErrorMessages.Count} errors");
        }
        catch (Exception ex)
        {
            result.ErrorMessages.Add($"Error loading dictionary: {ex.Message}");
            _logger.LogError(ex, $"Error loading dictionary from {filePath}");
        }

        return result;
    }

    /// <summary>
    /// Loads a WordSmith dictionary from a file and returns the dictionaries directly
    /// (Simplified version for backward compatibility)
    /// </summary>
    /// <param name="filePath">Path to the dictionary file</param>
    /// <returns>A tuple containing the replacements dictionary and new columns dictionary</returns>
    public (Dictionary<string, ReplacingStep>, Dictionary<string, ReplacingStep>) LoadDictionarySimple(string filePath)
    {
        var result = LoadDictionary(filePath);
        return (result.ReplacementsDictionary, result.NewColumnsDictionary);
    }

    /// <summary>
    /// Saves a WordSmith dictionary to a file
    /// </summary>
    /// <param name="filePath">Path to save the dictionary</param>
    /// <param name="replacementsDictionary">Dictionary of replacements</param>
    /// <param name="encoding">Encoding to use for writing the file (defaults to Unicode)</param>
    /// <returns>True if saved successfully, false otherwise</returns>
    public bool SaveDictionary(
        string filePath,
        Dictionary<string, ReplacingStep> replacementsDictionary,
        Encoding encoding = null)
    {
        try
        {
            _logger.LogInformation($"Saving WordSmith dictionary to {filePath}");

            if (replacementsDictionary == null || replacementsDictionary.Count == 0)
            {
                _logger.LogWarning("No entries to save");
                return false;
            }

            // Use Unicode encoding by default (matching the original code)
            encoding = encoding ?? Encoding.Unicode;

            using (var writer = new StreamWriter(filePath, false, encoding))
            {
                // Write header
                writer.WriteLine("Words\tReplacement\tNewColumn\tToDelete\tPriority\tCount");

                // Write each entry
                foreach (var entry in replacementsDictionary.Values)
                {
                    writer.WriteLine(
                        $"{entry.Words}\t" +
                        $"{entry.Replacement}\t" +
                        $"{entry.NewColumnName}\t" +
                        $"{(entry.ToDelete ? "1" : "0")}\t" +
                        $"{entry.Priority}\t" +
                        $"1");
                }
            }

            _logger.LogInformation($"Saved {replacementsDictionary.Count} entries to dictionary");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error saving dictionary to {filePath}");
            return false;
        }
    }

    /// <summary>
    /// Parses a string value and converts it to the appropriate type
    /// </summary>
    private object ConvertStringToType(string value, Type type)
    {
        if (string.IsNullOrEmpty(value))
        {
            return type == typeof(string) ? string.Empty : null;
        }

        try
        {
            if (type == typeof(string))
            {
                return value;
            }
            else if (type == typeof(int) || type == typeof(int?))
            {
                return int.TryParse(value, out int result) ? result : 0;
            }
            else if (type == typeof(bool) || type == typeof(bool?))
            {
                return value == "1" ||
                       value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                       value == "✓";
            }

            return value;
        }
        catch
        {
            return type == typeof(string) ? value : null;
        }
    }
}
