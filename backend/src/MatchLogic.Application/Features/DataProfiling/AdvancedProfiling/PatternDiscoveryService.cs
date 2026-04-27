using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;

public class PatternDiscoveryService : IPatternDiscoveryService
{
    private readonly ILogger<PatternDiscoveryService> _logger;
    private readonly AdvancedProfilingOptions _options;
    private readonly Dictionary<char, string> _characterClassMap;
    private readonly Dictionary<string, System.Text.RegularExpressions.Regex> _commonPatterns;

    public PatternDiscoveryService(
        ILogger<PatternDiscoveryService> logger,
        IOptions<AdvancedProfilingOptions> options = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? new AdvancedProfilingOptions();

        // Character class mappings for pattern discovery
        _characterClassMap = new Dictionary<char, string>
        {
            { 'a', "[a-z]" },      // Lowercase letter
            { 'A', "[A-Z]" },      // Uppercase letter
            { '9', "[0-9]" },      // Digit
            { '.', "[.]" },        // Period
            { '@', "[@]" },        // At sign
            { '#', "[#]" },        // Hash
            { '$', "[$]" },        // Dollar sign
            { '%', "[%]" },        // Percent
            { '^', "[\\^]" },      // Caret
            { '&', "[&]" },        // Ampersand
            { '*', "[*]" },        // Asterisk
            { '(', "[(]" },        // Left parenthesis
            { ')', "[)]" },        // Right parenthesis
            { '-', "[-]" },        // Hyphen
            { '_', "[_]" },        // Underscore
            { '+', "[+]" },        // Plus
            { '=', "[=]" },        // Equals
            { '{', "[{]" },        // Left brace
            { '}', "[}]" },        // Right brace
            { '[', "[\\[]" },      // Left bracket
            { ']', "[\\]]" },      // Right bracket
            { '|', "[|]" },        // Vertical bar
            { '\\', "[\\\\]" },    // Backslash
            { ':', "[:]" },        // Colon
            { ';', "[;]" },        // Semicolon
            { '"', "[\"]" },       // Double quote
            { '\'', "[']" },       // Single quote
            { '<', "[<]" },        // Less than
            { '>', "[>]" },        // Greater than
            { '/', "[/]" },        // Forward slash
            { '?', "[?]" },        // Question mark
            { '`', "[`]" },        // Backtick
            { '~', "[~]" },        // Tilde
            { ' ', "[ ]" },        // Space
            { 'X', "[A-Za-z0-9]" } // Alphanumeric
        };

        // Common pattern regexes
        _commonPatterns = new Dictionary<string, System.Text.RegularExpressions.Regex>
        {
            ["Email"] = new System.Text.RegularExpressions.Regex(@"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", RegexOptions.Compiled),
            ["URL"] = new System.Text.RegularExpressions.Regex(@"^(https?:\/\/)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$", RegexOptions.Compiled),
            ["PhoneUS"] = new System.Text.RegularExpressions.Regex(@"^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$", RegexOptions.Compiled),
            ["ZipCodeUS"] = new System.Text.RegularExpressions.Regex(@"^\d{5}(-\d{4})?$", RegexOptions.Compiled),
            ["SSN"] = new System.Text.RegularExpressions.Regex(@"^\d{3}-\d{2}-\d{4}$", RegexOptions.Compiled),
            ["CreditCard"] = new System.Text.RegularExpressions.Regex(@"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12})$", RegexOptions.Compiled),
            ["UUID"] = new System.Text.RegularExpressions.Regex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", RegexOptions.Compiled),
            ["Date1"] = new System.Text.RegularExpressions.Regex(@"^\d{4}-\d{2}-\d{2}$", RegexOptions.Compiled), // YYYY-MM-DD
            ["Date2"] = new System.Text.RegularExpressions.Regex(@"^\d{2}/\d{2}/\d{4}$", RegexOptions.Compiled), // MM/DD/YYYY
            ["Date3"] = new System.Text.RegularExpressions.Regex(@"^\d{2}-\d{2}-\d{4}$", RegexOptions.Compiled), // MM-DD-YYYY
            ["Time"] = new System.Text.RegularExpressions.Regex(@"^\d{2}:\d{2}(:\d{2})?$", RegexOptions.Compiled), // HH:MM:SS
            ["IPAddress"] = new System.Text.RegularExpressions.Regex(@"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", RegexOptions.Compiled)
        };
    }

    /// <summary>
    /// Discovers patterns in string data
    /// </summary>
    public async Task<List<DiscoveredPattern>> DiscoverPatternsAsync(
        IEnumerable<string> values,
        int maxPatterns = 10)
    {
        var result = new List<DiscoveredPattern>();
        var valuesList = values?.Where(v => !string.IsNullOrEmpty(v)).ToList() ?? new List<string>();

        if (valuesList.Count == 0)
        {
            return result;
        }

        try
        {
            // 1. Try to match common patterns first
            var commonPatternMatches = MatchCommonPatterns(valuesList);
            result.AddRange(commonPatternMatches.Take(maxPatterns / 2));

            // 2. Generate custom patterns for the data
            var customPatterns = await GenerateCustomPatternsAsync(valuesList, maxPatterns - result.Count);

            // Add non-duplicate custom patterns
            foreach (var pattern in customPatterns)
            {
                if (!result.Any(p => p.Pattern == pattern.Pattern))
                {
                    result.Add(pattern);
                }
            }

            // 3. Find patterns in clusters if we have room for more patterns
            if (result.Count < maxPatterns && valuesList.Count > 10)
            {
                var clusterPatterns = await FindClusterPatternsAsync(valuesList, maxPatterns - result.Count);

                foreach (var pattern in clusterPatterns)
                {
                    if (!result.Any(p => p.Pattern == pattern.Pattern))
                    {
                        result.Add(pattern);
                    }
                }
            }

            // 4. Find common affixes if we still have room
            if (result.Count < maxPatterns && valuesList.Count > 5)
            {
                var affixPatterns = FindAffixPatterns(valuesList, maxPatterns - result.Count);

                foreach (var pattern in affixPatterns)
                {
                    if (!result.Any(p => p.Pattern == pattern.Pattern))
                    {
                        result.Add(pattern);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in pattern discovery");
        }

        return result.OrderByDescending(p => p.Coverage).Take(maxPatterns).ToList();
    }

    /// <summary>
    /// Matches common predefined patterns
    /// </summary>
    private List<DiscoveredPattern> MatchCommonPatterns(List<string> values)
    {
        var result = new List<DiscoveredPattern>();
        var concurrentResults = new ConcurrentBag<DiscoveredPattern>();

        // Process patterns in parallel
        Parallel.ForEach(_commonPatterns, pattern =>
        {
            var matchingValues = new List<string>();

            foreach (var value in values)
            {
                try
                {
                    if (pattern.Value.IsMatch(value))
                    {
                        matchingValues.Add(value);
                    }
                }
                catch (RegexMatchTimeoutException)
                {
                    // Skip complex regex that times out
                    continue;
                }
            }

            // Only include patterns with reasonable coverage
            double coverage = (double)matchingValues.Count / values.Count * 100;
            if (coverage >= 5.0) // At least 5% coverage
            {
                concurrentResults.Add(new DiscoveredPattern
                {
                    Pattern = pattern.Key,
                    Count = matchingValues.Count,
                    Coverage = coverage,
                    Examples = matchingValues.Take(5).Select(m => new RowReference { Value = m }).ToList()
                });
            }
        });

        // Convert concurrent results to a sorted list
        result = concurrentResults.OrderByDescending(p => p.Coverage).ToList();
        return result;
    }

    /// <summary>
    /// Generates custom patterns based on character classes
    /// </summary>
    private async Task<List<DiscoveredPattern>> GenerateCustomPatternsAsync(List<string> values, int maxPatterns)
    {
        var result = new List<DiscoveredPattern>();

        // Generate abstract patterns for each value
        var patterns = new ConcurrentDictionary<string, ConcurrentBag<string>>();

        // Process values in parallel to improve performance
        await Task.Run(() =>
        {
            Parallel.ForEach(values, value =>
            {
                // Skip very long values to avoid performance issues
                if (value.Length > 100)
                    return;

                string abstractPattern = GenerateAbstractPattern(value);

                // Add value to the pattern bucket
                patterns.AddOrUpdate(
                    abstractPattern,
                    new ConcurrentBag<string>(new[] { value }),
                    (_, patternValues) =>
                    {
                        patternValues.Add(value);
                        return patternValues;
                    });
            });
        });

        // Convert to result format, preferring patterns with higher coverage
        foreach (var pattern in patterns
            .OrderByDescending(p => p.Value.Count)
            .Take(maxPatterns))
        {
            double coverage = (double)pattern.Value.Count / values.Count * 100;
            string regexPattern = GenerateRegexFromAbstractPattern(pattern.Key);

            // Only include patterns with reasonable coverage
            if (coverage >= 5.0) // At least 5% coverage
            {
                result.Add(new DiscoveredPattern
                {
                    Pattern = regexPattern,
                    Count = pattern.Value.Count,
                    Coverage = coverage,
                    Examples = pattern.Value.Take(5).Select(v => new RowReference { Value = v }).ToList()
                });
            }
        }

        return result;
    }

    /// <summary>
    /// Generates an abstract pattern from a string value
    /// </summary>
    private string GenerateAbstractPattern(string value)
    {
        var pattern = new StringBuilder();

        foreach (char c in value)
        {
            if (char.IsLower(c))
            {
                pattern.Append('a');
            }
            else if (char.IsUpper(c))
            {
                pattern.Append('A');
            }
            else if (char.IsDigit(c))
            {
                pattern.Append('9');
            }
            else
            {
                pattern.Append(c);
            }
        }

        return pattern.ToString();
    }

    /// <summary>
    /// Generates a regex pattern from an abstract pattern
    /// </summary>
    private string GenerateRegexFromAbstractPattern(string abstractPattern)
    {
        // Group consecutive same characters with quantifiers
        var regex = new StringBuilder();
        int count = 1;
        char prev = '\0';

        for (int i = 0; i < abstractPattern.Length; i++)
        {
            char current = abstractPattern[i];

            if (i > 0 && current == prev)
            {
                count++;
            }
            else
            {
                if (i > 0)
                {
                    // Append previous character with count
                    AppendCharacterClass(regex, prev, count);
                }

                count = 1;
                prev = current;
            }
        }

        // Append the last character
        if (abstractPattern.Length > 0)
        {
            AppendCharacterClass(regex, prev, count);
        }

        return regex.ToString();
    }

    /// <summary>
    /// Appends a character class with quantifier to the regex
    /// </summary>
    private void AppendCharacterClass(StringBuilder regex, char c, int count)
    {
        if (_characterClassMap.TryGetValue(c, out string charClass))
        {
            regex.Append(charClass);
        }
        else
        {
            // Escape special regex characters
            if (IsRegexSpecialChar(c))
            {
                regex.Append("\\").Append(c);
            }
            else
            {
                regex.Append(c);
            }
        }

        // Add quantifier if more than one occurrence
        if (count > 1)
        {
            regex.Append("{").Append(count).Append("}");
        }
    }

    /// <summary>
    /// Checks if a character is a special character in regex syntax
    /// </summary>
    private bool IsRegexSpecialChar(char c)
    {
        return "\\^$.|?*+()[{".Contains(c);
    }

    /// <summary>
    /// Analyzes cluster patterns to find commonalities
    /// </summary>
    private string GenerateClusterPattern(List<string> clusterValues)
    {
        // Use dynamic programming to find the longest common subsequence
        // This is a simplified version that just looks at individual characters

        if (clusterValues.Count <= 0 || string.IsNullOrEmpty(clusterValues[0]))
            return string.Empty;

        // Start with the first value's pattern
        string pattern = GenerateAbstractPattern(clusterValues[0]);

        // Refine the pattern based on other values in the cluster
        for (int i = 1; i < clusterValues.Count; i++)
        {
            string currentPattern = GenerateAbstractPattern(clusterValues[i]);

            // If the pattern has a different length, we can't easily merge them
            if (currentPattern.Length != pattern.Length)
                continue;

            StringBuilder mergedPattern = new StringBuilder(pattern.Length);

            // Compare each character position
            for (int j = 0; j < pattern.Length; j++)
            {
                // If characters are the same, keep them
                if (pattern[j] == currentPattern[j])
                {
                    mergedPattern.Append(pattern[j]);
                }
                else
                {
                    // If different, use the wildcard 'X' which will represent any alphanumeric
                    mergedPattern.Append('X');
                }
            }

            pattern = mergedPattern.ToString();
        }

        return pattern;
    }

    /// <summary>
    /// Finds common prefixes and suffixes in a set of strings
    /// </summary>
    private (string Prefix, string Suffix) FindCommonAffixes(List<string> values)
    {
        if (values.Count <= 1)
            return (string.Empty, string.Empty);

        // Start with the first string
        string commonPrefix = values[0];
        string commonSuffix = values[0];

        for (int i = 1; i < values.Count; i++)
        {
            string current = values[i];

            // Find common prefix
            int prefixLength = 0;
            while (prefixLength < commonPrefix.Length &&
                   prefixLength < current.Length &&
                   commonPrefix[prefixLength] == current[prefixLength])
            {
                prefixLength++;
            }

            commonPrefix = commonPrefix.Substring(0, prefixLength);

            // Find common suffix
            int suffixLength = 0;
            while (suffixLength < commonSuffix.Length &&
                   suffixLength < current.Length &&
                   commonSuffix[commonSuffix.Length - 1 - suffixLength] ==
                   current[current.Length - 1 - suffixLength])
            {
                suffixLength++;
            }

            commonSuffix = commonSuffix.Substring(commonSuffix.Length - suffixLength);

            // If we've lost all common elements, exit early
            if (commonPrefix.Length == 0 && commonSuffix.Length == 0)
                break;
        }

        return (commonPrefix, commonSuffix);
    }

    /// <summary>
    /// Finds patterns in clusters of similar strings
    /// </summary>
    private async Task<List<DiscoveredPattern>> FindClusterPatternsAsync(List<string> values, int maxPatterns)
    {
        var result = new List<DiscoveredPattern>();

        // Group values by length to find potential clusters
        var lengthGroups = values
            .Where(v => v.Length > 0)
            .GroupBy(v => v.Length)
            .Where(g => g.Count() >= Math.Max(3, values.Count / 20)) // At least 3 values or 5% of the data
            .OrderByDescending(g => g.Count())
            .Take(5) // Look at the top 5 length groups
            .ToList();

        if (!lengthGroups.Any())
            return result;

        await Task.Run(() =>
        {
            foreach (var group in lengthGroups)
            {
                var clusterValues = group.ToList();

                // Get a representative sample to analyze (max 100 values)
                var sampleSize = Math.Min(100, clusterValues.Count);
                var sample = clusterValues
                    .OrderBy(_ => Guid.NewGuid()) // Simple random sampling
                    .Take(sampleSize)
                    .ToList();

                // Generate a cluster pattern
                string clusterPattern = GenerateClusterPattern(sample);

                if (!string.IsNullOrEmpty(clusterPattern))
                {
                    // Count how many values match this pattern
                    int matchCount = 0;
                    List<string> examples = new List<string>();

                    // Test the pattern against all values in the original dataset
                    string regexPattern = "^" + GenerateRegexFromAbstractPattern(clusterPattern) + "$";

                    try
                    {
                        var regex = new System.Text.RegularExpressions.Regex(regexPattern, RegexOptions.Compiled);

                        foreach (var value in values)
                        {
                            try
                            {
                                if (regex.IsMatch(value))
                                {
                                    matchCount++;
                                    if (examples.Count < 5)
                                    {
                                        examples.Add(value);
                                    }
                                }
                            }
                            catch (RegexMatchTimeoutException)
                            {
                                // Skip if this match times out
                                continue;
                            }
                        }
                    }
                    catch (Exception)
                    {
                        // Skip if the regex compilation fails
                        continue;
                    }

                    double coverage = (double)matchCount / values.Count * 100;
                    if (coverage >= 5.0) // At least 5% coverage
                    {
                        result.Add(new DiscoveredPattern
                        {
                            //Pattern = GenerateHumanReadableClusterName(clusterPattern, group.Key, examples),
                            Pattern = $"Cluster_{group.Key}_{clusterPattern}",
                            Count = matchCount,
                            Coverage = coverage,
                            Examples = examples.Select(e => new RowReference { Value = e }).ToList()
                        });
                    }
                }
            }
        });

        return result.OrderByDescending(p => p.Coverage).Take(maxPatterns).ToList();
    }

    /// <summary>
    /// Finds patterns based on common prefixes and suffixes
    /// </summary>
    private List<DiscoveredPattern> FindAffixPatterns(List<string> values, int maxPatterns)
    {
        var result = new List<DiscoveredPattern>();

        // Find common affixes
        var (prefix, suffix) = FindCommonAffixes(values);

        // Only consider meaningful affixes (at least 2 characters)
        if (!string.IsNullOrEmpty(prefix) && prefix.Length >= 2)
        {
            // Count values that start with this prefix
            var prefixMatches = values
                .Where(v => v.StartsWith(prefix))
                .ToList();

            double coverage = (double)prefixMatches.Count / values.Count * 100;
            if (coverage >= 10.0) // At least 10% coverage for affixes
            {
                result.Add(new DiscoveredPattern
                {
                    Pattern = $"Prefix: {prefix}",
                    Count = prefixMatches.Count,
                    Coverage = coverage,
                    Examples = prefixMatches.Take(5).Select(v => new RowReference { Value = v }).ToList()
                });
            }
        }

        if (!string.IsNullOrEmpty(suffix) && suffix.Length >= 2)
        {
            // Count values that end with this suffix
            var suffixMatches = values
                .Where(v => v.EndsWith(suffix))
                .ToList();

            double coverage = (double)suffixMatches.Count / values.Count * 100;
            if (coverage >= 10.0) // At least 10% coverage for affixes
            {
                result.Add(new DiscoveredPattern
                {
                    Pattern = $"Suffix: {suffix}",
                    Count = suffixMatches.Count,
                    Coverage = coverage,
                    Examples = suffixMatches.Take(5).Select(v => new RowReference { Value = v }).ToList()
                });
            }
        }

        return result.OrderByDescending(p => p.Coverage).Take(maxPatterns).ToList();
    }

    /// <summary>
    /// Generates a human-readable name for cluster patterns
    /// </summary>
    private string GenerateHumanReadableClusterName(string clusterPattern, int length, List<string> examples)
    {
        if (string.IsNullOrEmpty(clusterPattern))
            return $"Mixed Format ({length} chars)";

        var description = TranslatePatternToDescription(clusterPattern);

        // Try to infer semantic meaning from examples
        var semanticHint = InferSemanticMeaning(examples, clusterPattern);

        if (!string.IsNullOrEmpty(semanticHint))
            return $"{semanticHint} ({description})";

        return $"Format: {description}";
    }

    /// <summary>
    /// Translates abstract pattern to human-readable description
    /// </summary>
    private string TranslatePatternToDescription(string pattern)
    {
        var result = new StringBuilder();
        int count = 1;
        char prev = pattern[0];

        for (int i = 1; i <= pattern.Length; i++)
        {
            char current = i < pattern.Length ? pattern[i] : '\0';

            if (i < pattern.Length && current == prev)
            {
                count++;
            }
            else
            {
                if (result.Length > 0)
                    result.Append(" + ");

                AppendCharacterDescription(result, prev, count);

                if (i < pattern.Length)
                {
                    count = 1;
                    prev = current;
                }
            }
        }

        return result.ToString();
    }

    /// <summary>
    /// Appends human-readable character description
    /// </summary>
    private void AppendCharacterDescription(StringBuilder result, char c, int count)
    {
        string description = c switch
        {
            'a' => count == 1 ? "lowercase letter" : $"{count} lowercase letters",
            'A' => count == 1 ? "uppercase letter" : $"{count} uppercase letters",
            '9' => count == 1 ? "digit" : $"{count} digits",
            'X' => count == 1 ? "alphanumeric" : $"{count} alphanumeric",
            ' ' => count == 1 ? "space" : $"{count} spaces",
            '-' => count == 1 ? "hyphen" : $"{count} hyphens",
            '_' => count == 1 ? "underscore" : $"{count} underscores",
            '.' => count == 1 ? "period" : $"{count} periods",
            '@' => count == 1 ? "at sign" : $"{count} at signs",
            ':' => count == 1 ? "colon" : $"{count} colons",
            '/' => count == 1 ? "slash" : $"{count} slashes",
            _ => count == 1 ? $"'{c}'" : $"{count} '{c}'"
        };

        result.Append(description);
    }

    /// <summary>
    /// Attempts to infer semantic meaning from examples
    /// </summary>
    private string InferSemanticMeaning(List<string> examples, string pattern)
    {
        if (examples.Count == 0)
            return string.Empty;

        // Check for common data types based on pattern and examples
        var firstExample = examples[0].ToLower();

        // Product codes (letters + numbers)
        if (pattern.Contains('A') && pattern.Contains('9'))
        {
            if (firstExample.StartsWith("sku") || firstExample.StartsWith("prod"))
                return "Product Code";
            if (pattern.Length >= 6 && pattern.Length <= 12)
                return "ID Code";
        }

        // User handles/IDs
        if (pattern.StartsWith("a") || pattern.StartsWith("A"))
        {
            if (pattern.Length >= 6 && pattern.Length <= 20 && (pattern.Contains('9') || pattern.Contains('X')))
                return "User ID";
        }

        // File extensions
        if (pattern.StartsWith("."))
            return "File Extension";

        // Codes with separators
        if (pattern.Contains("-") || pattern.Contains("_"))
        {
            if (pattern.Count(c => c == '9') >= 4)
                return "Reference Code";
        }

        // Timestamps or dates (if contains colons or slashes)
        if (pattern.Contains(":"))
            return "Time Format";
        if (pattern.Contains("/") && pattern.Contains('9'))
            return "Date Format";

        // Generic classifications
        if (pattern.All(c => c == 'A'))
            return "Uppercase Text";
        if (pattern.All(c => c == 'a'))
            return "Lowercase Text";
        if (pattern.All(c => c == '9'))
            return "Numeric Code";

        return string.Empty;
    }
}
