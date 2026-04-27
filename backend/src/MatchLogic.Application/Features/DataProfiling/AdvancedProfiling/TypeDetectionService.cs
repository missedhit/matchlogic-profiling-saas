using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;

public class TypeDetectionService : ITypeDetectionService
{
    private readonly ILogger<TypeDetectionService> _logger;
    private readonly AdvancedProfilingOptions _options;
    private readonly Dictionary<string, System.Text.RegularExpressions.Regex> _semanticTypePatterns;
    private readonly int _minSamplesForAdvancedDetection = 50;

    public TypeDetectionService(
        ILogger<TypeDetectionService> logger,
        IOptions<AdvancedProfilingOptions> options = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? new AdvancedProfilingOptions();

        // Initialize common semantic type patterns
        _semanticTypePatterns = new Dictionary<string, System.Text.RegularExpressions.Regex>
        {
            ["EmailAddress"] = new System.Text.RegularExpressions.Regex(@"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", RegexOptions.Compiled),
            // Fixed: More flexible phone number pattern supporting international formats
            ["PhoneNumber"] = new System.Text.RegularExpressions.Regex(@"^(\+?\d{1,4}[\s.-]?)?\(?[0-9]{3,4}\)?[\s.-]?[0-9]{3,4}[\s.-]?[0-9]{3,5}$", RegexOptions.Compiled),
            // Fixed: Better URL pattern with optional protocols and more flexible domains
            //^(https?:\/\/)?([\w\-]+\.)+[\w\-]+(\/[\w\-./?%&=]*)?$   New
            ["URL"] = new System.Text.RegularExpressions.Regex(@"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?(\?[;&a-z\d%_\.~+=\-]*)?(\#[a-z\d_-]*)?$", RegexOptions.Compiled | RegexOptions.IgnoreCase),
            ["IPAddress"] = new System.Text.RegularExpressions.Regex(@"^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$", RegexOptions.Compiled),
            // Fixed: More comprehensive postal code pattern for international support
            ["ZipCode"] = new System.Text.RegularExpressions.Regex(@"^\d{5}(-\d{4})?$", RegexOptions.Compiled),
            ["PostalCode"] = new System.Text.RegularExpressions.Regex(@"^(\d{5}(-\d{4})?|[A-Z]\d[A-Z] ?\d[A-Z]\d|[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}|\d{4,5})$", RegexOptions.Compiled | RegexOptions.IgnoreCase),
            ["CreditCard"] = new System.Text.RegularExpressions.Regex(@"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12})$", RegexOptions.Compiled),
            ["SSN"] = new System.Text.RegularExpressions.Regex(@"^\d{3}-\d{2}-\d{4}$", RegexOptions.Compiled),
            // Enhanced: More comprehensive currency pattern with international support
            ["Currency"] = new System.Text.RegularExpressions.Regex(@"^-?[\$€£¥₹₽]?\s?-?\d{1,3}(,\d{3})*(\.\d{1,4})?(\s?[\$€£¥₹₽])?$", RegexOptions.Compiled),
            // Fixed: More flexible person name pattern
            ["PersonName"] = new System.Text.RegularExpressions.Regex(@"^[A-Z][a-z]+(\s([A-Z][a-z]*\.?|[A-Z]\.)){0,3}(\s[A-Z][a-z]+)*(\s(Jr\.?|Sr\.?|II|III|IV))?$", RegexOptions.Compiled),
            ["UUID"] = new System.Text.RegularExpressions.Regex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", RegexOptions.Compiled),
            // Fixed: More specific hash patterns
            ["MD5Hash"] = new System.Text.RegularExpressions.Regex(@"^[0-9a-fA-F]{32}$", RegexOptions.Compiled),
            ["SHA1Hash"] = new System.Text.RegularExpressions.Regex(@"^[0-9a-fA-F]{40}$", RegexOptions.Compiled),
            ["SHA256Hash"] = new System.Text.RegularExpressions.Regex(@"^[0-9a-fA-F]{64}$", RegexOptions.Compiled),
            // New: Age detection pattern
            ["Age"] = new System.Text.RegularExpressions.Regex(@"^(1[0-2][0-9]|[1-9]?[0-9])(\s?(years?|yrs?|y\.?)(\s?old)?)?$", RegexOptions.Compiled | RegexOptions.IgnoreCase)
        };
    }

    /// <summary>
    /// Detects the most likely data type for a column
    /// </summary>
    public async Task<List<TypeDetectionResult>> DetectTypeAsync(IEnumerable<object> values)
    {
        var results = new List<TypeDetectionResult>();
        var samples = values.Where(v => v != null).Take(1000).ToList();

        if (samples.Count == 0)
        {
            return new List<TypeDetectionResult>
                {
                    new TypeDetectionResult { DataType = "Null", Confidence = 1.0 }
                };
        }

        try
        {
            // Basic counting approach 
            var typeDetectionResults = DetectTypeFromSamples(samples);

            // Use advanced approach if enough samples and close competition between types
            if (samples.Count >= _minSamplesForAdvancedDetection)
            {
                // Only use advanced detection when there's ambiguity in basic detection
                var topType = typeDetectionResults.OrderByDescending(t => t.Confidence).First();
                var secondType = typeDetectionResults.OrderByDescending(t => t.Confidence).Skip(1).FirstOrDefault();

                if (secondType != null && topType.Confidence - secondType.Confidence < 0.3)
                {
                    var advancedResults = await DetectTypeWithAdvancedTechniquesAsync(samples);

                    // Combine basic and advanced results with advanced having higher weight
                    results = CombineTypeDetectionResults(typeDetectionResults, advancedResults, 0.3, 0.7);
                }
                else
                {
                    results = typeDetectionResults;
                }
            }
            else
            {
                results = typeDetectionResults;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in type detection");
            results = DetectTypeFromSamples(samples); // Fallback to basic detection
        }

        return results.OrderByDescending(r => r.Confidence).ToList();
    }

    /// <summary>
    /// Detects semantic types (e.g., email, phone number) for a column
    /// </summary>
    public async Task<List<SemanticType>> DetectSemanticTypeAsync(IEnumerable<string> values)
    {
        var results = new List<SemanticType>();
        var nonEmptyValues = values.Where(v => !string.IsNullOrWhiteSpace(v)).ToList();

        if (nonEmptyValues.Count == 0)
        {
            return results;
        }

        try
        {
            // Sample values for pattern matching
            var sampleSize = Math.Min(nonEmptyValues.Count, 100);
            var samples = nonEmptyValues.Take(sampleSize).ToList();

            // Test each semantic type pattern
            foreach (var pattern in _semanticTypePatterns)
            {
                int matchCount = 0;

                foreach (var sample in samples)
                {
                    try
                    {
                        if (pattern.Value.IsMatch(sample.Trim()))
                        {
                            matchCount++;
                        }
                    }
                    catch (RegexMatchTimeoutException)
                    {
                        // Skip complex regex that times out
                        continue;
                    }
                }

                double confidence = (double)matchCount / sampleSize;

                // Only include types with reasonable confidence
                if (confidence >= 0.5)
                {
                    results.Add(new SemanticType
                    {
                        Type = pattern.Key,
                        Confidence = confidence
                    });
                }
            }

            // Use advanced semantic type detection for more complex types if we have enough samples
            if (nonEmptyValues.Count >= _minSamplesForAdvancedDetection)
            {
                var advancedResults = await DetectAdvancedSemanticTypesAsync(nonEmptyValues);
                results.AddRange(advancedResults.Where(r => !results.Any(existing => existing.Type == r.Type)));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in semantic type detection");
        }

        return results.OrderByDescending(r => r.Confidence).ToList();
    }

    /// <summary>
    /// Detect data types using basic sample analysis
    /// </summary>
    private List<TypeDetectionResult> DetectTypeFromSamples(List<object> samples)
    {
        int intCount = 0;
        int decimalCount = 0;
        int boolCount = 0;
        int dateTimeCount = 0;
        int guidCount = 0;
        int stringCount = 0;
        int jsonCount = 0;
        int xmlCount = 0;

        foreach (var sample in samples)
        {
            if (sample == null) continue;

            // Direct type checks
            if (sample is int or byte or short or long or sbyte or ushort or uint or ulong)
            {
                intCount++;
                continue;
            }

            if (sample is double or float or decimal)
            {
                decimalCount++;
                continue;
            }

            if (sample is bool)
            {
                boolCount++;
                continue;
            }

            if (sample is DateTime or DateTimeOffset or DateOnly or TimeOnly)
            {
                dateTimeCount++;
                continue;
            }

            if (sample is Guid)
            {
                guidCount++;
                continue;
            }

            // String parsing
            string strValue = sample.ToString();

            if (int.TryParse(strValue, out _))
            {
                intCount++;
            }
            else if (decimal.TryParse(strValue, out _) || double.TryParse(strValue, out _))
            {
                decimalCount++;
            }
            else if (bool.TryParse(strValue, out _) ||
                     strValue.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("no", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("y", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("n", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("t", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("f", StringComparison.OrdinalIgnoreCase))
            {
                boolCount++;
            }
            else if (DateTime.TryParse(strValue, out _))
            {
                dateTimeCount++;
            }
            else if (Guid.TryParse(strValue, out _))
            {
                guidCount++;
            }
            else if ((strValue.StartsWith("{") && strValue.EndsWith("}")) ||
                     (strValue.StartsWith("[") && strValue.EndsWith("]")))
            {
                jsonCount++;
            }
            else if (strValue.StartsWith("<") && strValue.EndsWith(">"))
            {
                xmlCount++;
            }
            else
            {
                stringCount++;
            }
        }

        // Calculate total counts and confidence
        int totalCount = samples.Count;
        var results = new List<TypeDetectionResult>();

        if (intCount > 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "Integer",
                Confidence = (double)intCount / totalCount
            });
        }

        if (decimalCount > 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "Decimal",
                Confidence = (double)decimalCount / totalCount
            });
        }

        if (boolCount > 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "Boolean",
                Confidence = (double)boolCount / totalCount
            });
        }

        if (dateTimeCount > 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "DateTime",
                Confidence = (double)dateTimeCount / totalCount
            });
        }

        if (guidCount > 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "Guid",
                Confidence = (double)guidCount / totalCount
            });
        }

        if (jsonCount > 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "JSON",
                Confidence = (double)jsonCount / totalCount
            });
        }

        if (xmlCount > 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "XML",
                Confidence = (double)xmlCount / totalCount
            });
        }

        if (stringCount > 0 || (totalCount > 0 && results.Sum(r => r.Confidence * totalCount) < totalCount))
        {
            double stringConfidence = (double)stringCount / totalCount;
            if (stringConfidence < 0.01) stringConfidence = 0.01; // Minimum confidence

            results.Add(new TypeDetectionResult
            {
                DataType = "String",
                Confidence = stringConfidence
            });
        }

        // If no results (unlikely), add string as default
        if (results.Count == 0)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "String",
                Confidence = 1.0
            });
        }

        return results.OrderByDescending(r => r.Confidence).ToList();
    }

    /// <summary>
    /// Detect data types using advanced techniques
    /// </summary>
    private async Task<List<TypeDetectionResult>> DetectTypeWithAdvancedTechniquesAsync(List<object> samples)
    {
        // Advanced type detection using sophisticated analysis
        var results = new List<TypeDetectionResult>();

        // Convert samples to string for processing
        var stringValues = samples.Select(s => s?.ToString() ?? string.Empty).ToList();

        // Format consistency analysis
        var formatFingerprints = AnalyzeFormatFingerprints(stringValues);

        // Extract features and detect types based on patterns
        Dictionary<string, double> typeScores = new Dictionary<string, double>();

        // Check for Integer pattern consistency
        var integerPattern = new System.Text.RegularExpressions.Regex(@"^-?\d+$");
        double integerScore = stringValues.Count(s => integerPattern.IsMatch(s)) / (double)stringValues.Count;
        if (integerScore > 0.8)
        {
            typeScores["Integer"] = integerScore;
        }

        // Check for Decimal pattern consistency
        var decimalPattern = new System.Text.RegularExpressions.Regex(@"^-?\d+\.\d+$");
        double decimalScore = stringValues.Count(s => decimalPattern.IsMatch(s)) / (double)stringValues.Count;
        if (decimalScore > 0.7)
        {
            typeScores["Decimal"] = decimalScore;
        }

        // Check for Boolean consistency
        var boolPattern = new System.Text.RegularExpressions.Regex(@"^(true|false|yes|no|y|n|t|f|1|0)$", RegexOptions.IgnoreCase);
        double boolScore = stringValues.Count(s => boolPattern.IsMatch(s)) / (double)stringValues.Count;
        if (boolScore > 0.9)
        {
            typeScores["Boolean"] = boolScore;
        }

        // Check for Date pattern consistency
        double dateScore = DetectDateFormatConsistency(stringValues);
        if (dateScore > 0.7)
        {
            typeScores["DateTime"] = dateScore;
        }

        // Check for GUID pattern
        var guidPattern = new System.Text.RegularExpressions.Regex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
        double guidScore = stringValues.Count(s => guidPattern.IsMatch(s)) / (double)stringValues.Count;
        if (guidScore > 0.9)
        {
            typeScores["Guid"] = guidScore;
        }

        // Add scores to results
        foreach (var score in typeScores)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = score.Key,
                Confidence = score.Value
            });
        }

        // Always add String as a fallback
        if (!typeScores.Any() || typeScores.Values.Max() < 0.7)
        {
            results.Add(new TypeDetectionResult
            {
                DataType = "String",
                Confidence = 1 - (typeScores.Any() ? typeScores.Values.Max() : 0)
            });
        }

        return await Task.FromResult(results);
    }

    /// <summary>
    /// Detect semantic types using advanced techniques
    /// </summary>
    private async Task<List<SemanticType>> DetectAdvancedSemanticTypesAsync(List<string> values)
    {
        var results = new List<SemanticType>();

        // Analyze string length distributions
        var lengthDistribution = values
            .GroupBy(v => v.Length)
            .ToDictionary(g => g.Key, g => g.Count());

        // Check for fixed-length fields (potential codes/IDs)
        if (lengthDistribution.Count == 1)
        {
            var length = lengthDistribution.First().Key;
            var count = lengthDistribution.First().Value;

            // Fixed length fields might be codes
            if (length >= 5 && length <= 20)
            {
                // Check character composition
                bool allUppercase = values.All(v => v.All(c => char.IsUpper(c) || char.IsDigit(c) || c == '-' || c == '_'));
                bool containsDigits = values.All(v => v.Any(char.IsDigit));

                if (allUppercase && containsDigits)
                {
                    results.Add(new SemanticType
                    {
                        Type = "Code",
                        Confidence = 0.8
                    });
                }
            }

            // Check for potential boolean fields
            if (length == 1)
            {
                var oneCharValues = values.Distinct().ToList();
                if (oneCharValues.Count <= 2 &&
                    oneCharValues.All(v => v == "Y" || v == "N" || v == "T" || v == "F" || v == "1" || v == "0"))
                {
                    results.Add(new SemanticType
                    {
                        Type = "Boolean",
                        Confidence = 0.9
                    });
                }
            }
        }

        // Enhanced: Age detection with more sophisticated logic
        var ageDetectionResult = DetectAgeValues(values);
        if (ageDetectionResult.Confidence > 0.6)
        {
            results.Add(ageDetectionResult);
        }

        // Enhanced: Currency detection with better pattern recognition
        var currencyDetectionResult = DetectCurrencyValues(values);
        if (currencyDetectionResult.Confidence > 0.6)
        {
            results.Add(currencyDetectionResult);
        }

        // Check for name patterns
        bool possiblyNames = values.Count(v => System.Text.RegularExpressions.Regex.IsMatch(v, @"^[A-Z][a-z]+(\s([A-Z][a-z]*\.?|[A-Z]\.)){0,3}(\s[A-Z][a-z]+)*(\s(Jr\.?|Sr\.?|II|III|IV))?$")) > values.Count * 0.7;
        if (possiblyNames)
        {
            results.Add(new SemanticType
            {
                Type = "PersonName",
                Confidence = 0.7
            });
        }

        // Check for potential locations
        if (ContainsCommonLocationWords(values))
        {
            results.Add(new SemanticType
            {
                Type = "Location",
                Confidence = 0.6
            });
        }

        return await Task.FromResult(results);
    }

    /// <summary>
    /// Detect age values with sophisticated pattern analysis
    /// </summary>
    private SemanticType DetectAgeValues(List<string> values)
    {
        int ageMatchCount = 0;
        int numericAgeCount = 0;

        foreach (var value in values)
        {
            var trimmedValue = value.Trim();

            // Check explicit age patterns (e.g., "25 years old", "30 yrs", "45 y")
            if (_semanticTypePatterns["Age"].IsMatch(trimmedValue))
            {
                ageMatchCount++;
                continue;
            }

            // Check for simple numeric ages (realistic range)
            if (int.TryParse(trimmedValue, out int age) && age >= 0 && age <= 150)
            {
                numericAgeCount++;
            }
        }

        // Calculate confidence based on different patterns
        double explicitAgeConfidence = (double)ageMatchCount / values.Count;
        double numericAgeConfidence = (double)numericAgeCount / values.Count;

        // Weighted confidence: explicit age patterns have higher weight
        double overallConfidence = (explicitAgeConfidence * 0.8) + (numericAgeConfidence * 0.4);

        // Additional heuristics for numeric-only data
        if (ageMatchCount == 0 && numericAgeCount > 0)
        {
            // Check if the numeric values fall within typical age distributions
            var numericValues = values
                .Where(v => int.TryParse(v.Trim(), out int age) && age >= 0 && age <= 150)
                .Select(v => int.Parse(v.Trim()))
                .ToList();

            if (numericValues.Any())
            {
                var avg = numericValues.Average();
                var min = numericValues.Min();
                var max = numericValues.Max();

                // Typical age distribution characteristics
                if (min >= 0 && max <= 120 && avg >= 10 && avg <= 80)
                {
                    overallConfidence = Math.Max(overallConfidence, 0.7);
                }
            }
        }

        return new SemanticType
        {
            Type = "Age",
            Confidence = Math.Min(overallConfidence, 1.0)
        };
    }

    /// <summary>
    /// Detect currency values with enhanced pattern recognition
    /// </summary>
    private SemanticType DetectCurrencyValues(List<string> values)
    {
        int currencyMatchCount = 0;
        int possibleCurrencyCount = 0;

        foreach (var value in values)
        {
            var trimmedValue = value.Trim();

            // Check explicit currency patterns
            if (_semanticTypePatterns["Currency"].IsMatch(trimmedValue))
            {
                currencyMatchCount++;
                continue;
            }

            // Check for currency-like numeric values
            if (decimal.TryParse(trimmedValue.Replace(",", ""), out decimal amount))
            {
                // Look for typical currency characteristics
                string cleanValue = trimmedValue.Replace(",", "").Replace(" ", "");

                // Has currency symbols
                if (cleanValue.Any(c => "€£¥₹₽$¢".Contains(c)))
                {
                    possibleCurrencyCount++;
                }
                // Has decimal places typical for currency
                else if (cleanValue.Contains(".") && cleanValue.Split('.')[1].Length <= 4)
                {
                    possibleCurrencyCount++;
                }
                // Has thousand separators
                else if (trimmedValue.Contains(","))
                {
                    possibleCurrencyCount++;
                }
            }
        }

        double explicitCurrencyConfidence = (double)currencyMatchCount / values.Count;
        double possibleCurrencyConfidence = (double)possibleCurrencyCount / values.Count;

        // Weighted confidence
        double overallConfidence = (explicitCurrencyConfidence * 0.9) + (possibleCurrencyConfidence * 0.5);

        return new SemanticType
        {
            Type = "Currency",
            Confidence = Math.Min(overallConfidence, 1.0)
        };
    }

    /// <summary>
    /// Analyze format patterns in string values
    /// </summary>
    private Dictionary<string, int> AnalyzeFormatFingerprints(List<string> values)
    {
        var fingerprints = new Dictionary<string, int>();

        foreach (var value in values)
        {
            string fingerprint = GenerateFormatFingerprint(value);

            if (fingerprints.ContainsKey(fingerprint))
            {
                fingerprints[fingerprint]++;
            }
            else
            {
                fingerprints[fingerprint] = 1;
            }
        }

        return fingerprints;
    }

    /// <summary>
    /// Generate a format fingerprint for a string value
    /// </summary>
    private string GenerateFormatFingerprint(string value)
    {
        var fingerprint = new StringBuilder();

        foreach (char c in value)
        {
            if (char.IsLetter(c))
            {
                if (char.IsUpper(c))
                    fingerprint.Append('A');
                else
                    fingerprint.Append('a');
            }
            else if (char.IsDigit(c))
            {
                fingerprint.Append('9');
            }
            else
            {
                fingerprint.Append(c);
            }
        }

        return fingerprint.ToString();
    }

    /// <summary>
    /// Detect date format consistency across string values
    /// </summary>
    private double DetectDateFormatConsistency(List<string> values)
    {
        int dateCount = 0;
        var dateFormats = new Dictionary<string, int>();

        foreach (var value in values)
        {
            if (DateTime.TryParse(value, out DateTime date))
            {
                dateCount++;

                // Determine the likely format
                string format = DetermineDateFormat(value, date);

                if (dateFormats.ContainsKey(format))
                {
                    dateFormats[format]++;
                }
                else
                {
                    dateFormats[format] = 1;
                }
            }
        }

        // Calculate date format consistency
        if (dateCount > 0)
        {
            var mostCommonFormat = dateFormats.OrderByDescending(kv => kv.Value).FirstOrDefault();
            double formatConsistency = (double)mostCommonFormat.Value / dateCount;

            // Return overall date confidence
            return (double)dateCount / values.Count * formatConsistency;
        }

        return 0;
    }

    /// <summary>
    /// Determine the likely date format for a string value
    /// </summary>
    private string DetermineDateFormat(string value, DateTime date)
    {
        // Check common date formats
        if (System.Text.RegularExpressions.Regex.IsMatch(value, @"^\d{4}-\d{2}-\d{2}$"))
            return "yyyy-MM-dd";

        if (System.Text.RegularExpressions.Regex.IsMatch(value, @"^\d{1,2}/\d{1,2}/\d{4}$"))
            return "MM/dd/yyyy";

        if (System.Text.RegularExpressions.Regex.IsMatch(value, @"^\d{1,2}/\d{1,2}/\d{2}$"))
            return "MM/dd/yy";

        if (System.Text.RegularExpressions.Regex.IsMatch(value, @"^\d{1,2}-\d{1,2}-\d{4}$"))
            return "MM-dd-yyyy";

        if (System.Text.RegularExpressions.Regex.IsMatch(value, @"^\d{1,2}\.\d{1,2}\.\d{4}$"))
            return "MM.dd.yyyy";

        if (System.Text.RegularExpressions.Regex.IsMatch(value, @"^\d{4}/\d{1,2}/\d{1,2}$"))
            return "yyyy/MM/dd";

        // If format can't be determined from pattern, check if it has time component
        if (value.Contains(":"))
            return "DateTime";

        return "Custom";
    }

    /// <summary>
    /// Check if values contain common location words
    /// </summary>
    private bool ContainsCommonLocationWords(List<string> values)
    {
        // Common words found in addresses and locations
        string[] locationWords = {
                "street", "road", "ave", "avenue", "blvd", "boulevard", "ln", "lane",
                "drive", "dr", "court", "ct", "plaza", "square", "suite", "apt",
                "apartment", "room", "floor", "state", "country", "city"
            };

        int locationMatches = 0;

        foreach (var value in values)
        {
            foreach (var word in locationWords)
            {
                if (value.Contains(word, StringComparison.OrdinalIgnoreCase))
                {
                    locationMatches++;
                    break;
                }
            }
        }

        return (double)locationMatches / values.Count >= 0.3;
    }

    /// <summary>
    /// Combine results from multiple detection methods
    /// </summary>
    private List<TypeDetectionResult> CombineTypeDetectionResults(
        List<TypeDetectionResult> basicResults,
        List<TypeDetectionResult> advancedResults,
        double basicWeight,
        double advancedWeight)
    {
        var combined = new Dictionary<string, double>();

        // Add basic results
        foreach (var result in basicResults)
        {
            combined[result.DataType] = result.Confidence * basicWeight;
        }

        // Add advanced results
        foreach (var result in advancedResults)
        {
            if (combined.ContainsKey(result.DataType))
            {
                combined[result.DataType] += result.Confidence * advancedWeight;
            }
            else
            {
                combined[result.DataType] = result.Confidence * advancedWeight;
            }
        }

        // Normalize confidence
        var totalConfidence = combined.Values.Sum();
        if (totalConfidence > 0)
        {
            foreach (var key in combined.Keys.ToList())
            {
                combined[key] /= totalConfidence;
            }
        }

        // Convert back to results
        return combined
            .Select(kvp => new TypeDetectionResult
            {
                DataType = kvp.Key,
                Confidence = kvp.Value
            })
            .OrderByDescending(r => r.Confidence)
            .ToList();
    }
}
