using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Application.Features.DataProfiling;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static MatchLogic.Application.Common.Constants;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;

public partial class AdvancedColumnAnalyzer
{
    /// <summary>
    /// Build an enhanced column profile with advanced analytics
    /// </summary>
    public async Task<AdvancedColumnProfile> BuildEnhancedColumnProfileAsync(
        AdvancedProfilingOptions options,
        ITypeDetectionService typeDetectionService = null,
        IOutlierDetectionService outlierDetectionService = null,
        IPatternDiscoveryService patternDiscoveryService = null,
        IValidationRuleService validationRuleService = null)
    {
        // Determine the dominant pattern
        string dominantPattern = "Unclassified";
        int maxValidCount = 0;

        foreach (var pattern in _patternStats)
        {
            if (pattern.Key != "Unclassified" && pattern.Value.ValidCount > maxValidCount)
            {
                dominantPattern = pattern.Key;
                maxValidCount = pattern.Value.ValidCount;
            }
        }

        _dominantPattern = dominantPattern;

        // Calculate total valid and invalid counts across all patterns
        long totalValid = 0;//_patternStats.Sum(p => p.Value.ValidCount);
        long totalInvalid = 0;// _patternStats.Sum(p => p.Value.InvalidCount) -
                              //(_patternStats.Count - 1) * (_totalCount - _nullCount - _emptyCount); // Adjust for double-counting

        // Build the pattern information
        var patternInfoList = new List<PatternInfo>();

        foreach (var patternEntry in _patternStats)
        {
            if (patternEntry.Value.TotalCount > 0)
            {
                patternInfoList.Add(new PatternInfo
                {
                    Pattern = patternEntry.Key,
                    Description = patternEntry.Value.Description,
                    Count = patternEntry.Value.ValidCount,
                    MatchPercentage = patternEntry.Value.TotalCount > 0
                        ? (double)patternEntry.Value.ValidCount / (_totalCount - _nullCount - _emptyCount) * 100
                        : 0
                });
            }
        }

        // Build row references for patterns
        var patternRows = new Dictionary<string, List<RowReference>>();

        foreach (var patternEntry in _patternStats)
        {
            var validRows = patternEntry.Value.ValidRows.Take(options.MaxRowsPerCategory).ToList();
            var invalidRows = patternEntry.Value.InvalidRows.Take(options.MaxRowsPerCategory).ToList();

            patternRows[$"{patternEntry.Key}_Valid"] = validRows;
            patternRows[$"{patternEntry.Key}_Invalid"] = invalidRows;
        }

        // Calculate statistical values for numeric data
        string mean = string.Empty;
        string median = string.Empty;
        string mode = string.Empty;
        string stdDev = string.Empty;
        string skewness = string.Empty;
        string kurtosis = string.Empty;
        string interquartileRange = string.Empty;

        if (_numericValues.Count > 0)
        {
            // Calculate mean
            double meanValue = _sum / _numericValues.Count;
            mean = meanValue.ToString(CultureInfo.InvariantCulture);

            // Calculate standard deviation
            double variance = (_sumOfSquares / _numericValues.Count) - (meanValue * meanValue);
            double stdDevValue = Math.Sqrt(Math.Max(0, variance));
            stdDev = stdDevValue.ToString(CultureInfo.InvariantCulture);

            // Calculate skewness
            if (_numericValues.Count > 0 && stdDevValue > 0)
            {
                double m3 = _sumOfCubes / _numericValues.Count;
                double skewnessVal = (m3 - 3 * meanValue * variance - Math.Pow(meanValue, 3)) / Math.Pow(stdDevValue, 3);
                skewness = skewnessVal.ToString(CultureInfo.InvariantCulture);
            }

            // Calculate kurtosis
            if (_numericValues.Count > 0 && stdDevValue > 0)
            {
                double m4 = _sumOfQuads / _numericValues.Count;
                double kurtosisVal = (m4 - 4 * meanValue * _sumOfCubes / _numericValues.Count + 6 * meanValue * meanValue * variance
                                      - 3 * Math.Pow(meanValue, 4)) / Math.Pow(stdDevValue, 4) - 3;
                kurtosis = kurtosisVal.ToString(CultureInfo.InvariantCulture);
            }

            // Calculate median and IQR (requires sorting)
            double medianValue = 0;
            double q1 = 0;
            double q3 = 0;

            lock (_numericStatsLock)
            {
                var sorted = _numericValues.OrderBy(v => v).ToList();
                int middle = sorted.Count / 2;

                if (sorted.Count % 2 == 0)
                {
                    medianValue = (sorted[middle - 1] + sorted[middle]) / 2;
                }
                else
                {
                    medianValue = sorted[middle];
                }

                // Calculate quartiles
                int q1Index = sorted.Count / 4;
                int q3Index = sorted.Count * 3 / 4;

                q1 = sorted.Count % 4 == 0
                    ? (sorted[q1Index - 1] + sorted[q1Index]) / 2
                    : sorted[q1Index];

                q3 = sorted.Count % 4 == 0
                    ? (sorted[q3Index - 1] + sorted[q3Index]) / 2
                    : sorted[q3Index];
            }

            median = medianValue.ToString(CultureInfo.InvariantCulture);
            double iqr = q3 - q1;
            interquartileRange = iqr.ToString(CultureInfo.InvariantCulture);

            // Calculate mode
            var mostCommon = _valueDistribution
                .OrderByDescending(v => v.Value)
                .FirstOrDefault();

            mode = mostCommon.Key?.ToString() ?? string.Empty;
        }

        // Build the base column profile
        var baseProfile = new ColumnProfile
        {
            FieldName = _fieldName,
            Type = _type,
            Length = _length,
            Pattern = _dominantPattern,

            // Counts
            Total = _totalCount,
            Valid = totalValid,
            Invalid = totalInvalid,
            Filled = _totalCount - _nullCount - _emptyCount,
            Null = _nullCount + _emptyCount, // Combining null and empty for simplicity
            Distinct = _valueDistribution.Count,

            // Character statistics
            Numbers = _digitsCount,
            NumbersOnly = _digitsOnlyCount,
            Letters = _lettersCount,
            LettersOnly = _lettersOnlyCount,
            NumbersAndLetters = _alphanumericCount,
            Punctuation = _punctuationCount,
            LeadingSpaces = _leadingSpacesCount,
            NonPrintableCharacters = _nonPrintableCount,

            // Statistical values
            Min = _minValue?.ToString() ?? string.Empty,
            Max = _maxValue?.ToString() ?? string.Empty,
            Mean = mean,
            Median = median,
            Mode = mode,
            Extreme = stdDev,  // Using Extreme field for standard deviation

            // Pattern information
            Patterns = patternInfoList,

            //// Row references                
            CharacteristicRowDocumentIds = new Dictionary<ProfileCharacteristic, Guid>(),
            PatternMatchRowDocumentIds = new Dictionary<string, Guid>(),
            ValueRowDocumentIds = new Dictionary<string, Guid>()
        };

        // Add value distribution for categorical fields
        if (baseProfile.Distinct <= 100 || (baseProfile.Distinct <= 1000 && baseProfile.Distinct <= _totalCount * 0.1))
        {
            baseProfile.ValueDistribution = _valueDistribution
                .OrderByDescending(v => v.Value)
                .Take(100)
                .ToDictionary(k => k.Key?.ToString() ?? "null", v => v.Value);
        }

        // Create enhanced column profile
        var enhancedProfile = new AdvancedColumnProfile
        {
            // Copy all base properties
            FieldName = baseProfile.FieldName,
            Type = baseProfile.Type,
            Length = baseProfile.Length,
            Pattern = baseProfile.Pattern,
            Total = baseProfile.Total,
            Valid = baseProfile.Valid,
            Invalid = baseProfile.Invalid,
            Filled = baseProfile.Filled,
            Null = baseProfile.Null,
            Distinct = baseProfile.Distinct,
            Numbers = baseProfile.Numbers,
            NumbersOnly = baseProfile.NumbersOnly,
            Letters = baseProfile.Letters,
            LettersOnly = baseProfile.LettersOnly,
            NumbersAndLetters = baseProfile.NumbersAndLetters,
            Punctuation = baseProfile.Punctuation,
            LeadingSpaces = baseProfile.LeadingSpaces,
            NonPrintableCharacters = baseProfile.NonPrintableCharacters,
            Min = baseProfile.Min,
            Max = baseProfile.Max,
            Mean = baseProfile.Mean,
            Median = baseProfile.Median,
            Mode = baseProfile.Mode,
            Extreme = baseProfile.Extreme,
            Patterns = baseProfile.Patterns,
            ValueDistribution = baseProfile.ValueDistribution,

            // Add enhanced properties
            InferredDataType = _type,
            TypeDetectionConfidence = CalculateTypeDetectionConfidence(),
            Skewness = double.TryParse(skewness, out double skewnessValue) ? skewnessValue : 0,
            Kurtosis = double.TryParse(kurtosis, out double kurtosisValue) ? kurtosisValue : 0,
            InterquartileRange = interquartileRange
        };

        // Enhanced analytics based on enabled features
        if (options.EnableAdvancedTypeDetection && typeDetectionService != null && _valueSamples.Count > 0)
        {
            try
            {
                // Detect data types
                enhancedProfile.TypeDetectionResults = await typeDetectionService.DetectTypeAsync(_valueSamples);

                // Detect semantic types for string columns
                if (_type == "String")
                {
                    var stringValues = _valueSamples
                        .Where(v => v != null)
                        .Select(v => v.ToString())
                        .Where(s => !string.IsNullOrEmpty(s))
                        .ToList();

                    if (stringValues.Count > 0)
                    {
                        enhancedProfile.PossibleSemanticTypes = await typeDetectionService.DetectSemanticTypeAsync(stringValues);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log error but continue with other analytics
                enhancedProfile.Warnings = enhancedProfile.Warnings ?? new List<string>();
                enhancedProfile.Warnings.Add($"Error in type detection: {ex.Message}");
            }
        }

        // Generate histogram for numeric data
        if (_numericValues.Count > 0)
        {
            enhancedProfile.Histogram = CreateHistogram(_numericValues, 10);
        }

        // Detect format
        if (_formatPatterns.Count > 0)
        {
            var dominantFormat = _formatPatterns
                .OrderByDescending(f => f.Value)
                .First();

            double confidence = (double)dominantFormat.Value / (_totalCount - _nullCount - _emptyCount);

            enhancedProfile.DetectedFormat = new FormatInfo
            {
                Format = dominantFormat.Key,
                Confidence = confidence,
                Examples = GetFormatExamples(dominantFormat.Key)
            };
        }

        // Detect outliers
        if (options.EnableOutlierDetection && outlierDetectionService != null && _numericValues.Count > 0)
        {
            try
            {
                // Use values from valueDistribution for outlier detection
                var values = _valueDistribution.Keys
                    .Where(k => TryConvertToDouble(k, out _))
                    .ToList();

                if (values.Count > 0)
                {
                    enhancedProfile.Outliers = await outlierDetectionService.DetectOutliersAsync(
                        values,
                        options.OutlierDetectionThreshold,
                        _type);

                    // Attach row references to outliers
                    if (options.StoreOutlierRowReferences && enhancedProfile.Outliers.Count > 0)
                    {
                        // Match outliers with stored row references
                        foreach (var outlier in enhancedProfile.Outliers)
                        {
                            // Find matching outlier candidate with row reference
                            var matchingCandidate = _outlierCandidates
                                .Where(c => c.Value?.ToString() == outlier.Value)
                                .FirstOrDefault();

                            if (matchingCandidate.RowRef != null)
                            {
                                outlier.RowReference = matchingCandidate.RowRef;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Log error but continue with other analytics
                enhancedProfile.Warnings = enhancedProfile.Warnings ?? new List<string>();
                enhancedProfile.Warnings.Add($"Error in outlier detection: {ex.Message}");
            }
        }

        // Discover patterns
        if (options.EnablePatternDiscovery && patternDiscoveryService != null && _type == "String")
        {
            try
            {
                var stringValues = _valueSamples
                    .Where(v => v != null)
                    .Select(v => v.ToString())
                    .Where(s => !string.IsNullOrEmpty(s))
                    .ToList();

                if (stringValues.Count > 0)
                {
                    enhancedProfile.DiscoveredPatterns = await patternDiscoveryService.DiscoverPatternsAsync(
                        stringValues,
                        options.MaxPatternsToDiscover);

                    // Add row references to discovered patterns
                    if (options.StoreCompleteRows && enhancedProfile.DiscoveredPatterns.Count > 0)
                    {
                        foreach (var pattern in enhancedProfile.DiscoveredPatterns)
                        {
                            // Find examples that match this pattern
                            foreach (var example in pattern.Examples)
                            {
                                if (example.Value != null && _valueRows.TryGetValue(example.Value, out var rowRefs))
                                {
                                    example.RowData = rowRefs.FirstOrDefault()?.RowData;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Log error but continue with other analytics
                enhancedProfile.Warnings = enhancedProfile.Warnings ?? new List<string>();
                enhancedProfile.Warnings.Add($"Error in pattern discovery: {ex.Message}");
            }
        }

        // Apply validation rules
        if (options.EnableRuleBasedValidation && validationRuleService != null)
        {
            try
            {
                enhancedProfile.AppliedRules = await validationRuleService.GetApplicableRulesAsync(enhancedProfile);

                if (enhancedProfile.AppliedRules.Count > 0)
                {
                    // Use sample values for validation
                    var sampleValues = _valueSamples
                        .Take(Math.Min(_valueSamples.Count, 1000))
                        .ToList();

                    enhancedProfile.Violations = await validationRuleService.ValidateAsync(
                        sampleValues,
                        enhancedProfile.AppliedRules);
                }
            }
            catch (Exception ex)
            {
                // Log error but continue with other analytics
                enhancedProfile.Warnings = enhancedProfile.Warnings ?? new List<string>();
                enhancedProfile.Warnings.Add($"Error in validation rule processing: {ex.Message}");
            }
        }

        return enhancedProfile;
    }
}
