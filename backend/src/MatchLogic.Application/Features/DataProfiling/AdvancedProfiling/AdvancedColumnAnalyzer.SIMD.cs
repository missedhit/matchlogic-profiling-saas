using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;

public partial class AdvancedColumnAnalyzer
{
    /// <summary>
    /// SIMD-optimized analysis for string values
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AnalyzeWithSimd(string value, IDictionary<string, object> fullRow, long rowNumber)
    {
        Interlocked.Increment(ref _totalCount);

        if (string.IsNullOrEmpty(value))
        {
            Interlocked.Increment(ref _emptyCount);

            if (_characteristicRows[ProfileCharacteristic.Empty].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.Empty].Add(CreateRowReference(fullRow, value, rowNumber));
            }

            return;
        }

        // Track value distribution
        try
        {
            _valueDistributionLock.EnterUpgradeableReadLock();

            bool isNewValue = !_valueDistribution.ContainsKey(value);

            // Add to value distribution
            _valueDistribution.AddOrUpdate(value, 1, (_, count) => count + 1);

            // If this is a new distinct value, add it to characteristics
            if (isNewValue)
            {
                try
                {
                    _valueDistributionLock.EnterWriteLock();

                    if (_characteristicRows[ProfileCharacteristic.DistinctValue].Count < _options.MaxRowsPerCategory)
                    {
                        _characteristicRows[ProfileCharacteristic.DistinctValue].Add(
                            CreateRowReference(fullRow, value, rowNumber));
                    }
                }
                finally
                {
                    _valueDistributionLock.ExitWriteLock();
                }
            }

            // Sample values for ML analysis if enabled
            if (_options.EnableMlAnalysis && _valueSamples.Count < _options.MlAnalysisSampleSize)
            {
                _valueSamples.Add(value);
            }

            // Track distinct value rows (up to a limit)
            if (_valueRows.Count < _options.MaxDistinctValuesToTrack)
            {
                if (!_valueRows.ContainsKey(value))
                {
                    _valueRows[value] = new ConcurrentBag<RowReference>();
                }

                if (_valueRows[value].Count < _options.MaxRowsPerCategory)
                {
                    _valueRows[value].Add(CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // Store potential outlier candidate for later analysis
            //OV-R need to how we can optimize candidate outliers population
            if (_options.StoreOutlierRowReferences /*&& _outlierCandidates.Count < _options.MaxOutlierRowsToStore*/)
            {
                var rowRef = CreateRowReference(fullRow, value, rowNumber);
                _outlierCandidates.Add((value, rowRef));
            }
        }
        finally
        {
            _valueDistributionLock.ExitUpgradeableReadLock();
        }

        // Update the type statistics (strings)
        _typeCounters.AddOrUpdate("String", 1, (key, oldValue) => oldValue + 1);

        // Update the length information
        if (value.Length > _length)
        {
            Interlocked.Exchange(ref _length, value.Length);
        }

        // Pattern matching
        MatchPatterns(value, value, fullRow, rowNumber);

        // Character analysis with SIMD
        AnalyzeCharactersWithSimd(value, fullRow, rowNumber);

        // Detect format
        DetectFormat(value);

        // Min/Max string value tracking
        UpdateMinMaxStringValue(value, fullRow, rowNumber);

        // Try to convert to numeric for statistics
        if (TryConvertToDouble(value, out double numericValue))
        {
            UpdateNumericStatistics(numericValue);
        }
    }

    /// <summary>
    /// Update type statistics based on hierarchy
    /// </summary>
    private void UpdateTypeStatistics(object value)
    {
        // Type detection hierarchy (strict to lenient):
        // Integer -> Decimal -> Boolean -> DateTime -> String

        if (value is int or byte or short or long or BigInteger or sbyte or ushort or uint or ulong)
        {
            // Integer type
            _typeCounters.AddOrUpdate("Integer", 1, (key, oldValue) => oldValue + 1);

            if (_type != "Decimal" && _type != "Boolean" && _type != "DateTime")
            {
                _type = "Integer";
            }
        }
        else if (value is double or float or decimal)
        {
            if (value is double doubleVal && double.IsNaN(doubleVal))
            {
                _typeCounters.AddOrUpdate("String", 1, (key, oldValue) => oldValue + 1);
                _type = "String";
            }
            else
            {
                // Decimal type
                _typeCounters.AddOrUpdate("Decimal", 1, (key, oldValue) => oldValue + 1);

                if (_type == "Integer")
                {
                    _type = "Decimal";
                }
            }            
        }
        else if (value is bool)
        {
            // Boolean type
            _typeCounters.AddOrUpdate("Boolean", 1, (key, oldValue) => oldValue + 1);

            if (_type == "Integer" || _type == "Decimal")
            {
                _type = "Boolean";
            }
        }
        else if (value is DateTime or DateTimeOffset or DateOnly or TimeOnly)
        {
            // DateTime type
            _typeCounters.AddOrUpdate("DateTime", 1, (key, oldValue) => oldValue + 1);

            if (_type == "Integer" || _type == "Decimal" || _type == "Boolean")
            {
                _type = "DateTime";
            }
        }
        else if (value is Guid)
        {
            // Guid type
            _typeCounters.AddOrUpdate("Guid", 1, (key, oldValue) => oldValue + 1);
            _type = "String";
        }
        else if (value is string strValue)
        {
            // Try different string parsings
            if (int.TryParse(strValue, out _))
            {
                _typeCounters.AddOrUpdate("Integer", 1, (key, oldValue) => oldValue + 1);
                if (_type == "String" && _typeCounters["String"] < _typeCounters["Integer"])
                {
                    _type = "Integer";
                }
            }
            else if (double.TryParse(strValue, out _) || decimal.TryParse(strValue, out _))
            {
                _typeCounters.AddOrUpdate("Decimal", 1, (key, oldValue) => oldValue + 1);
                if (_type == "Integer" || (_type == "String" && _typeCounters["String"] < _typeCounters["Decimal"]))
                {
                    _type = "Decimal";
                }
            }
            else if (bool.TryParse(strValue, out _) ||
                     strValue.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("no", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("y", StringComparison.OrdinalIgnoreCase) ||
                     strValue.Equals("n", StringComparison.OrdinalIgnoreCase))
            {
                _typeCounters.AddOrUpdate("Boolean", 1, (key, oldValue) => oldValue + 1);
            }
            else if (DateTime.TryParse(strValue, out _))
            {
                _typeCounters.AddOrUpdate("DateTime", 1, (key, oldValue) => oldValue + 1);
            }
            else if (Guid.TryParse(strValue, out _))
            {
                _typeCounters.AddOrUpdate("Guid", 1, (key, oldValue) => oldValue + 1);
            }
            else if ((strValue.StartsWith("{") && strValue.EndsWith("}")) ||
                     (strValue.StartsWith("[") && strValue.EndsWith("]")))
            {
                _typeCounters.AddOrUpdate("JSON", 1, (key, oldValue) => oldValue + 1);
            }
            else if (strValue.StartsWith("<") && strValue.EndsWith(">"))
            {
                _typeCounters.AddOrUpdate("XML", 1, (key, oldValue) => oldValue + 1);
            }
            else
            {
                _typeCounters.AddOrUpdate("String", 1, (key, oldValue) => oldValue + 1);
            }

            // Fall back to string type
            if (_type != "Integer" && _type != "Decimal")
            {
                _type = "String";
            }
        }
        else
        {
            // Other type
            _typeCounters.AddOrUpdate("String", 1, (key, oldValue) => oldValue + 1);
            _type = "String";
        }

        // Update the length information based on value
        UpdateLengthInfo(value);
    }

    /// <summary>
    /// Update length information for the value
    /// </summary>
    private void UpdateLengthInfo(object value)
    {
        // Get string representation length
        int valueLength = value.ToString().Length;

        // Update max length if needed
        if (valueLength > _length)
        {
            Interlocked.Exchange(ref _length, valueLength);
        }
    }

    /// <summary>
    /// Match patterns using regex and dictionaries
    /// </summary>
    private void MatchPatterns(object value, string stringValue, IDictionary<string, object> fullRow, long rowNumber)
    {
        // Check if the value matches any pattern
        bool foundMatch = false;

        // Try to match regex patterns
        foreach (var (id, patternName, regex) in _regexPatterns)
        {
            var description = RegexPrefix + patternName;
            bool isValid = false;

            try
            {
                isValid = regex.IsMatch(stringValue);
            }
            catch (RegexMatchTimeoutException)
            {
                // Skip complex regex that times out
                continue;
            }

            // If the pattern matches, update its stats
            if (isValid)
            {
                foundMatch = true;

                var stats = _patternStats[patternName];
                _patternStats.AddOrUpdate(patternName,
                    _ => new PatternMatchStats { TotalCountField = 1, ValidCountField = 1, Description = description },
                    (_, stats) => {
                        Interlocked.Increment(ref stats.TotalCountField);
                        Interlocked.Increment(ref stats.ValidCountField);
                        stats.Description = description;
                        return stats;
                    });

                // Store row reference for this pattern if not at limit
                if (stats.ValidRows.Count < _options.MaxRowsPerCategory)
                {
                    stats.ValidRows.Add(CreateRowReference(fullRow, stringValue, rowNumber));
                }

                // Also store under pattern match characteristic
                if (_characteristicRows[ProfileCharacteristic.PatternMatch].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.PatternMatch].Add(
                        CreateRowReference(fullRow, stringValue, rowNumber));
                }
            }
            else
            {
                // The value doesn't match this pattern, track as invalid for this pattern
                var stats = _patternStats[patternName];
                _patternStats.AddOrUpdate(patternName,
                    _ => new PatternMatchStats { TotalCountField = 1, InvalidCountField = 1, Description = description },
                    (_, stats) => {
                        Interlocked.Increment(ref stats.TotalCountField);
                        Interlocked.Increment(ref stats.InvalidCountField);
                        stats.Description = description;
                        return stats;
                    });

                // Store invalid row reference
                if (stats.InvalidRows.Count < _options.MaxRowsPerCategory)
                {
                    stats.InvalidRows.Add(CreateRowReference(fullRow, stringValue, rowNumber));
                }
            }
        }

        // Try dictionary matches
        foreach (var (id, dictName, items) in _dictionaries)
        {
            var description = DictionaryPrefix + dictName;
            bool isValid = items.Contains(stringValue);

            if (isValid)
            {
                foundMatch = true;

                var stats = _patternStats[dictName];
                _patternStats.AddOrUpdate(dictName,
                    _ => new PatternMatchStats { TotalCountField = 1, ValidCountField = 1, Description = description },
                    (_, stats) => {
                        Interlocked.Increment(ref stats.TotalCountField);
                        Interlocked.Increment(ref stats.ValidCountField);
                        stats.Description = description;
                        return stats;
                    });

                // Store row reference
                if (stats.ValidRows.Count < _options.MaxRowsPerCategory)
                {
                    stats.ValidRows.Add(CreateRowReference(fullRow, stringValue, rowNumber));
                }

                // Also store under dictionary match characteristic
                if (_characteristicRows[ProfileCharacteristic.DictionaryMatch].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.DictionaryMatch].Add(
                        CreateRowReference(fullRow, stringValue, rowNumber));
                }
            }
            else
            {
                // The value doesn't match this dictionary
                var stats = _patternStats[dictName];
                _patternStats.AddOrUpdate(dictName,
                    _ => new PatternMatchStats { TotalCountField = 1, InvalidCountField = 1, Description = description },
                    (_, stats) => {
                        Interlocked.Increment(ref stats.TotalCountField);
                        Interlocked.Increment(ref stats.InvalidCountField);
                        stats.Description = description;
                        return stats;
                    });

                // Store invalid row reference
                if (stats.InvalidRows.Count < _options.MaxRowsPerCategory)
                {
                    stats.InvalidRows.Add(CreateRowReference(fullRow, stringValue, rowNumber));
                }
            }
        }

        // If no match found, mark as unclassified
        if (!foundMatch)
        {
            // The unclassified category doesn't have valid/invalid counts
            var stats = _patternStats["Unclassified"];
            _patternStats.AddOrUpdate("Unclassified",
                    _ => new PatternMatchStats { TotalCountField = 1, InvalidCountField = 0, ValidCountField = 0 },
                    (_, stats) => {
                        Interlocked.Increment(ref stats.TotalCountField);
                        return stats;
                    });

            // Store under unclassified characteristic
            if (_characteristicRows[ProfileCharacteristic.UnclassifiedPattern].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.UnclassifiedPattern].Add(
                    CreateRowReference(fullRow, stringValue, rowNumber));
            }
        }
    }
}
