using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling
{
    public class ColumnAnalyzer
    {
        private readonly string _fieldName;
        private readonly ProfilingOptions _options;
        private readonly List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> _regexPatterns;
        private readonly List<(Guid Id, string Name, HashSet<string> Items)> _dictionaries;

        // Inferred data type (following hierarchy)
        private string _type = "Integer"; // Default to Integer
        private int _length;

        // Basic counts
        private long _totalCount;
        private long _nullCount;
        private long _emptyCount;
        private readonly ConcurrentDictionary<object, long> _valueDistribution;

        // Pattern tracking
        internal readonly ConcurrentDictionary<string, PatternMatchStats> _patternStats = new();
        private string _dominantPattern = "Unclassified";

        // Fields for character analysis
        private long _lettersCount;
        private long _digitsCount;
        private long _whitespaceCount;
        private long _punctuationCount;
        private long _specialCharCount;
        private long _lettersOnlyCount;
        private long _digitsOnlyCount;
        private long _alphanumericCount;
        private long _leadingSpacesCount;
        private long _nonPrintableCount;

        // Min/Max tracking
        private object _minValue;
        private object _maxValue;
        private double _minNumericValue = double.MaxValue;
        private double _maxNumericValue = double.MinValue;

        // Statistical values
        private readonly List<double> _numericValues = new();
        private double _sum;
        private double _sumOfSquares;

        // Row references
        internal readonly ConcurrentDictionary<ProfileCharacteristic, ConcurrentBag<RowReference>> _characteristicRows = new();
        internal readonly ConcurrentDictionary<string, ConcurrentBag<RowReference>> _valueRows = new();

        // Type detection counters
        private int _intCount;
        private int _decimalCount;
        private int _datetimeCount;
        private int _booleanCount;
        private int _stringCount;

        // Class to track pattern-specific validation stats
        internal class PatternMatchStats
        {
            public int TotalCountField;
            public int ValidCountField;
            public int InvalidCountField;

            // Properties that expose the fields
            public int TotalCount => TotalCountField;
            public int ValidCount => ValidCountField;
            public int InvalidCount => InvalidCountField;

            public ConcurrentBag<RowReference> ValidRows { get; } = new();
            public ConcurrentBag<RowReference> InvalidRows { get; } = new();
        }

        public ColumnAnalyzer(
            string fieldName,
            ProfilingOptions options,
            List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> regexPatterns,
            List<(Guid Id, string Name, HashSet<string> Items)> dictionaries)
        {
            _fieldName = fieldName;
            _options = options;
            _regexPatterns = regexPatterns;
            _dictionaries = dictionaries;
            _valueDistribution = new ConcurrentDictionary<object, long>();

            // Initialize all characteristic row containers
            foreach (ProfileCharacteristic characteristic in Enum.GetValues(typeof(ProfileCharacteristic)))
            {
                _characteristicRows[characteristic] = new ConcurrentBag<RowReference>();
            }

            // Initialize pattern stats for all regex patterns
            foreach (var pattern in regexPatterns)
            {
                _patternStats[pattern.Name] = new PatternMatchStats();
            }

            // Initialize pattern stats for all dictionary categories
            foreach (var dict in dictionaries)
            {
                _patternStats[dict.Name] = new PatternMatchStats();
            }

            // Add unclassified pattern
            _patternStats["Unclassified"] = new PatternMatchStats();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Analyze(object value, IDictionary<string, object> fullRow, long rowNumber)
        {
            Interlocked.Increment(ref _totalCount);

            // Check for null
            if (value == null || value == DBNull.Value)
            {
                Interlocked.Increment(ref _nullCount);

                // Store row reference for null characteristic
                if (_characteristicRows[ProfileCharacteristic.Null].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.Null].Add(CreateRowReference(fullRow, null, rowNumber));
                }

                return;
            }

            // Empty check for strings
            if (value is string strVal && string.IsNullOrEmpty(strVal))
            {
                Interlocked.Increment(ref _emptyCount);

                if (_characteristicRows[ProfileCharacteristic.Empty].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.Empty].Add(CreateRowReference(fullRow, strVal, rowNumber));
                }
                // Also increment null count for empty strings as they are often treated as nulls in profiling

                // Store row reference for null characteristic
                if (_characteristicRows[ProfileCharacteristic.Null].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.Null].Add(CreateRowReference(fullRow, null, rowNumber));
                }

                return;
            }

            // Track value distribution and distinct values
            string stringValue = value?.ToString();
            // Now update the value distribution counter
            lock (this)
            {
                

                // First, check if this is a new value before adding to the distribution
                if (!_valueDistribution.ContainsKey(value))
                {
                    // This is a distinct value - add it to the DistinctValue characteristics
                    if (_characteristicRows[ProfileCharacteristic.DistinctValue].Count < _options.MaxRowsPerCategory)
                    {
                        _characteristicRows[ProfileCharacteristic.DistinctValue].Add(CreateRowReference(fullRow, stringValue, rowNumber));
                    }
                }


                _valueDistribution.AddOrUpdate(value, 1, (_, count) => count + 1);

                // Track distinct value rows (up to a limit)
                if (_valueRows.Count < _options.MaxDistinctValuesToTrack)
                {
                    if (!_valueRows.ContainsKey(stringValue))
                    {
                        _valueRows[stringValue] = new ConcurrentBag<RowReference>();
                    }

                    if (_valueRows[stringValue].Count < _options.MaxRowsPerCategory)
                    {
                        _valueRows[stringValue].Add(CreateRowReference(fullRow, stringValue, rowNumber));
                    }
                }
            }

            // Update data type based on hierarchy
            UpdateTypeStatistics(value);

            // Check if the value matches any pattern
            bool foundMatch = false;

            // Try to match regex patterns
            foreach (var (id, patternName, regex) in _regexPatterns)
            {
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
                        _ => new PatternMatchStats { TotalCountField = 1, ValidCountField = 1 },
                        (_, stats) => {
                            Interlocked.Increment(ref stats.TotalCountField);
                            Interlocked.Increment(ref stats.ValidCountField);
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
                        _ => new PatternMatchStats { TotalCountField = 1, InvalidCountField = 1 },
                        (_, stats) => {
                            Interlocked.Increment(ref stats.TotalCountField);
                            Interlocked.Increment(ref stats.InvalidCountField);
                            return stats;
                        });

                    // Store invalid row reference
                    if (stats.InvalidRows.Count < _options.MaxRowsPerCategory)
                    {
                        stats.InvalidRows.Add(CreateRowReference(fullRow, stringValue, rowNumber));
                    }
                }
            }

            // try dictionary matches
            foreach (var (id, dictName, items) in _dictionaries)
            {
                bool isValid = items.Contains(stringValue);

                if (isValid)
                {
                    foundMatch = true;

                    var stats = _patternStats[dictName];
                    _patternStats.AddOrUpdate(dictName,
                        _ => new PatternMatchStats { TotalCountField = 1, ValidCountField = 1 },
                        (_, stats) => {
                            Interlocked.Increment(ref stats.TotalCountField);
                            Interlocked.Increment(ref stats.ValidCountField);
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
                        _ => new PatternMatchStats { TotalCountField = 1, InvalidCountField = 1 },
                        (_, stats) => {
                            Interlocked.Increment(ref stats.TotalCountField);
                            Interlocked.Increment(ref stats.InvalidCountField);
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

                // Store row reference - no need to store row for unclassified patterns
                /*if (stats.InvalidRows.Count < _options.MaxRowsPerCategory)
                {
                    stats.InvalidRows.Add(CreateRowReference(fullRow, stringValue, rowNumber));
                }

                // Also store under unclassified characteristic
                if (_characteristicRows[ProfileCharacteristic.UnclassifiedPattern].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.UnclassifiedPattern].Add(
                        CreateRowReference(fullRow, stringValue, rowNumber));
                }*/
            }

            // Character analysis
            if (value is string strValue && !string.IsNullOrEmpty(strValue))
            {
                AnalyzeCharacters(strValue, fullRow, rowNumber);
            }
            else if (value is int or long or short or byte or sbyte or ushort or uint or ulong or float or double or decimal)
            {
                string numStr = value.ToString();
                // For numeric objects, directly increment the NumbersOnly count
                Interlocked.Increment(ref _digitsOnlyCount);

                // Store row reference for NumbersOnly characteristic
                if (_characteristicRows[ProfileCharacteristic.NumbersOnly].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.NumbersOnly].Add(
                        CreateRowReference(fullRow, numStr, rowNumber));
                }
                // Also increment the Numbers count for individual digits in numeric values
                Interlocked.Increment(ref _digitsCount);
                if (_characteristicRows[ProfileCharacteristic.Numbers].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.Numbers].Add(
                        CreateRowReference(fullRow, numStr, rowNumber));
                }
            }

            // Min/Max value tracking
            UpdateMinMaxValues(value, fullRow, rowNumber);

            // Track numeric values for statistics if appropriate
            if (TryConvertToDouble(value, out double numericValue))
            {
                UpdateNumericStatistics(numericValue);
            }
        }
        
        /// <summary>
        /// Updates type statistics and adjusts the column type based on hierarchy
        /// </summary>
        private void UpdateTypeStatistics(object value)
        {
            // Type detection hierarchy (strict to lenient):
            // Integer -> Decimal -> Boolean -> DateTime -> String

            if (value is int or byte or short or long)
            {
                // Integer type
                _intCount++;
                if (_type == "String" && _stringCount < _intCount)
                {
                    _type = "Integer";
                }
            }
            else if (value is double or float or decimal)
            {
                // Decimal type
                _decimalCount++;

                // Upgrade type if needed
                if (_type == "Integer" || (_type == "String" && _stringCount < _decimalCount))
                {
                    _type = "Decimal";
                }
            }
            else if (value is string strVal)
            {
                if (int.TryParse(strVal, out _))
                {
                    // String that can be parsed as int
                    _intCount++;
                    if (_type == "String" && _stringCount < _intCount)
                    {
                        _type = "Integer";
                    }
                }
                else if (double.TryParse(strVal, out _))
                {
                    // String that can be parsed as double
                    _decimalCount++;
                    if (_type == "Integer" || (_type == "String" && _stringCount < _decimalCount))
                    {
                        _type = "Decimal";
                    }
                }
                else if (bool.TryParse(strVal, out _))
                {
                    // Boolean type
                    _booleanCount++;

                    // Upgrade type if numeric
                    if (_type == "Integer" || _type == "Decimal")
                    {
                        _type = "String";
                    }
                }
                else if (DateTime.TryParse(strVal, out _))
                {
                    // DateTime type
                    _datetimeCount++;

                    // Upgrade type if numeric or boolean
                    if (_type == "Integer" || _type == "Decimal" || _type == "Boolean")
                    {
                        _type = "String";
                    }
                }
                else
                {
                    // Regular string
                    _stringCount++;
                    _type = "String";
                }
            }
            else if (value is bool)
            {
                // Boolean type
                _booleanCount++;

                // Upgrade type if numeric
                if (_type == "Integer" || _type == "Decimal")
                {
                    _type = "String";
                }
            }
            else if (value is DateTime or DateTimeOffset or DateOnly or TimeOnly)
            {
                // DateTime type
                _datetimeCount++;

                // Upgrade type if numeric or boolean
                if (_type == "Integer" || _type == "Decimal" || _type == "Boolean")
                {
                    _type = "DateTime";
                }
            }
            else
            {
                // String or other type
                _stringCount++;

                // Always upgrade to string for non-standard types
                _type = "String";
            }

            // Update the length information based on value
            UpdateLengthInfo(value);
        }

        private void UpdateLengthInfo(object value)
        {
            // Get string representation length
            int valueLength = value.ToString().Length;

            // Update max length if needed
            if (valueLength > _length)
            {
                _length = valueLength;
            }
        }

        private void AnalyzeCharacters(string value, IDictionary<string, object> fullRow, long rowNumber)
        {
            bool hasLetters = false;
            bool hasDigits = false;
            bool hasPunctuation = false;
            bool hasSpecialChars = false;
            bool hasNonPrintable = false;
            bool onlyLetters = true;
            bool onlyDigits = true;
            bool hasLeadingSpace = false;

            // Check for numeric string first (special case)
            bool isNumericString = double.TryParse(value, out _);
            if (isNumericString && !string.IsNullOrEmpty(value) && value.All(c => char.IsDigit(c) || c == '.' || c == '-' || c == '+'))
            {
                onlyDigits = true;
                Interlocked.Increment(ref _digitsOnlyCount);
                if (_characteristicRows[ProfileCharacteristic.NumbersOnly].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.NumbersOnly].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // Check for leading spaces
            if (value.Length > 0 && char.IsWhiteSpace(value[0]))
            {
                Interlocked.Increment(ref _leadingSpacesCount);
                hasLeadingSpace = true;

                // Store leading space row reference
                if (_characteristicRows[ProfileCharacteristic.WithLeadingSpaces].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.WithLeadingSpaces].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // Process each character
            foreach (char c in value)
            {
                if (char.IsLetter(c))
                {
                    //Interlocked.Increment(ref _lettersCount);
                    hasLetters = true;
                    onlyDigits = false;
                }
                else if (char.IsDigit(c))
                {
                    //Interlocked.Increment(ref _digitsCount);
                    hasDigits = true;
                    onlyLetters = false;
                }
                else if (char.IsWhiteSpace(c))
                {
                    //Interlocked.Increment(ref _whitespaceCount);
                    onlyLetters = false;
                    onlyDigits = false;
                }
                else if (char.IsPunctuation(c))
                {
                    //Interlocked.Increment(ref _punctuationCount);
                    hasPunctuation = true;
                    onlyLetters = false;
                    onlyDigits = false;
                }
                else if (char.IsControl(c) || c == '\t' || c == '\n' || c == '\r' || c == '\0' || c == '\b' || c == '\a')
                {
                    // Explicitly check for common control characters
                    // Interlocked.Increment(ref _nonPrintableCount);
                    hasNonPrintable = true;
                    onlyLetters = false;
                    onlyDigits = false;
                }
                else
                {
                    //Interlocked.Increment(ref _specialCharCount);
                    hasSpecialChars = true;
                    onlyLetters = false;
                    onlyDigits = false;
                }
            }

            // If value has any letter
            if (hasLetters)
            {
                Interlocked.Increment(ref _lettersCount);
                if (_characteristicRows[ProfileCharacteristic.Letters].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.Letters].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // If value has any digit
            if (hasDigits)
            {
                Interlocked.Increment(ref _digitsCount);
                if (_characteristicRows[ProfileCharacteristic.Numbers].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.Numbers].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // If value has any punctuation
            if (hasPunctuation)
            {
                Interlocked.Increment(ref _punctuationCount);
                if (_characteristicRows[ProfileCharacteristic.WithPunctuation].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.WithPunctuation].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }
            // if value has any non printable characters
            if(hasNonPrintable)
            {
                Interlocked.Increment(ref _nonPrintableCount);
                if (_characteristicRows[ProfileCharacteristic.WithNonPrintable].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.WithNonPrintable].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // if value has any special characters
            if (hasSpecialChars)
            {
                Interlocked.Increment(ref _specialCharCount);
                if (_characteristicRows[ProfileCharacteristic.WithSpecialChars].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.WithSpecialChars].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // Update counts and store row references for character characteristics
            // Only update letters-only if we haven't already identified as numeric-only
            if (onlyLetters && hasLetters)
            {
                Interlocked.Increment(ref _lettersOnlyCount);
                if (_characteristicRows[ProfileCharacteristic.LettersOnly].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.LettersOnly].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            // Only update digits-only if we haven't already done so in the numeric check
            if (onlyDigits && hasDigits && !isNumericString)
            {
                Interlocked.Increment(ref _digitsOnlyCount);
                if (_characteristicRows[ProfileCharacteristic.NumbersOnly].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.NumbersOnly].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            if (hasLetters && hasDigits && !hasPunctuation && !hasSpecialChars)
            {
                Interlocked.Increment(ref _alphanumericCount);
                if (_characteristicRows[ProfileCharacteristic.Alphanumeric].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.Alphanumeric].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }


            if (hasSpecialChars)
            {
                if (_characteristicRows[ProfileCharacteristic.WithSpecialChars].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.WithSpecialChars].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }

            if (hasNonPrintable)
            {
                if (_characteristicRows[ProfileCharacteristic.WithNonPrintable].Count < _options.MaxRowsPerCategory)
                {
                    _characteristicRows[ProfileCharacteristic.WithNonPrintable].Add(
                        CreateRowReference(fullRow, value, rowNumber));
                }
            }
        }

        private void UpdateMinMaxValues(object value, IDictionary<string, object> fullRow, long rowNumber)
        {
            if (TryConvertToDouble(value, out double numericValue))
            {
                // For numeric values, track min/max numerically
                bool isNewMin = false;
                bool isNewMax = false;

                lock (this) // Need to lock for these comparisons
                {
                    if (numericValue < _minNumericValue)
                    {
                        _minNumericValue = numericValue;
                        _minValue = value;
                        isNewMin = true;
                    }

                    if (numericValue > _maxNumericValue)
                    {
                        _maxNumericValue = numericValue;
                        _maxValue = value;
                        isNewMax = true;
                    }


                    // Store row references for min/max
                    if (isNewMin)
                    {
                        // Clear previous min references and add new one
                        var minRefs = _characteristicRows[ProfileCharacteristic.Minimum];
                        minRefs = new ConcurrentBag<RowReference>();
                        minRefs.Add(CreateRowReference(fullRow, value.ToString(), rowNumber));
                        _characteristicRows[ProfileCharacteristic.Minimum] = minRefs;
                    }

                    if (isNewMax)
                    {
                        // Clear previous max references and add new one
                        var maxRefs = _characteristicRows[ProfileCharacteristic.Maximum];
                        maxRefs = new ConcurrentBag<RowReference>();
                        maxRefs.Add(CreateRowReference(fullRow, value.ToString(), rowNumber));
                        _characteristicRows[ProfileCharacteristic.Maximum] = maxRefs;
                    }
                }
            }
          
        }

        private void UpdateNumericStatistics(double value)
        {
            // Track for statistical calculations
            lock (_numericValues)
            {
                _numericValues.Add(value);
            }

            // Update running sum and sum of squares for variance calculation
            double oldSum, newSum;
            do
            {
                oldSum = _sum;
                newSum = oldSum + value;
            } while (Interlocked.CompareExchange(ref _sum, newSum, oldSum) != oldSum);

            double oldSumSq, newSumSq;
            do
            {
                oldSumSq = _sumOfSquares;
                newSumSq = oldSumSq + (value * value);
            } while (Interlocked.CompareExchange(ref _sumOfSquares, newSumSq, oldSumSq) != oldSumSq);
        }

        private bool TryConvertToDouble(object value, out double result)
        {
            result = 0;

            if (value == null)
                return false;

            if (value is double d)
            {
                result = d;
                return true;
            }

            if (value is int i)
            {
                result = i;
                return true;
            }

            if (value is long l)
            {
                result = l;
                return true;
            }

            if (value is decimal m)
            {
                result = (double)m;
                return true;
            }

            if (value is float f)
            {
                result = f;
                return true;
            }

            // Try parsing string value
            return double.TryParse(value.ToString(), out result);
        }

        /// <summary>
        /// Creates a row reference object that includes complete row data
        /// </summary>
        private RowReference CreateRowReference(IDictionary<string, object> row, string value, long rowNumber)
        {
            var reference = new RowReference
            {
                Value = value,
                RowNumber = rowNumber
            };

            // Store complete row if configured
            if (_options.StoreCompleteRows)
            {
                reference.RowData = new Dictionary<string, object>(row);
            }

            return reference;
        }

        public ColumnProfile BuildColumnProfile()
        {
            // Determine the dominant pattern - pattern with the most valid matches
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
            long totalInvalid =  0;//_patternStats.Sum(p => p.Value.InvalidCount) -
            //                    (_patternStats.Count - 1) * (_totalCount - _nullCount - _emptyCount); // Adjust for double-counting

            // Build the pattern information
            var patternInfoList = new List<Domain.DataProfiling.PatternInfo>();

            foreach (var patternEntry in _patternStats)
            {
                if (patternEntry.Value.TotalCount > 0)
                {
                    patternInfoList.Add(new Domain.DataProfiling.PatternInfo
                    {
                        Pattern = patternEntry.Key,
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
                var validRows = patternEntry.Value.ValidRows.Take(_options.MaxRowsPerCategory).ToList();
                var invalidRows = patternEntry.Value.InvalidRows.Take(_options.MaxRowsPerCategory).ToList();

                patternRows[$"{patternEntry.Key}_Valid"] = validRows;
                patternRows[$"{patternEntry.Key}_Invalid"] = invalidRows;
            }

            // Calculate statistical values for numeric data
            string mean = string.Empty;
            string median = string.Empty;
            string mode = string.Empty;
            string stdDev = string.Empty;

            if (_numericValues.Count > 0)
            {
                // Calculate mean
                double meanValue = _sum / _numericValues.Count;
                mean = meanValue.ToString(CultureInfo.InvariantCulture);

                // Calculate standard deviation
                double variance = (_sumOfSquares / _numericValues.Count) - (meanValue * meanValue);
                double stdDevValue = Math.Sqrt(Math.Max(0, variance));
                stdDev = stdDevValue.ToString(CultureInfo.InvariantCulture);

                // Calculate median (requires sorting)
                double medianValue;
                lock (_numericValues)
                {
                    var sorted = _numericValues.OrderBy(v => v).ToList();
                    int middle = sorted.Count / 2;

                    if (sorted.Count % 2 == 0)
                        medianValue = (sorted[middle - 1] + sorted[middle]) / 2;
                    else
                        medianValue = sorted[middle];
                }

                median = medianValue.ToString(CultureInfo.InvariantCulture);

                // Calculate mode
                var mostCommon = _valueDistribution
                    .OrderByDescending(v => v.Value)
                    .FirstOrDefault();

                mode = mostCommon.Key?.ToString() ?? string.Empty;
            }

            // Build the column profile
            var profile = new ColumnProfile
            {
                FieldName = _fieldName,
                Type = _type,        // Using the hierarchical type
                Length = _length,
                Pattern = _dominantPattern,

                // Counts
                Total = _totalCount,
                Valid = totalValid,
                Invalid = totalInvalid,
                Filled = _totalCount - _nullCount - _emptyCount,
                Null = _nullCount + _emptyCount, // Treat empty as null for this count
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

                // Document IDs will be populated when saved to database
                CharacteristicRowDocumentIds = new Dictionary<ProfileCharacteristic, Guid>(),
                PatternMatchRowDocumentIds = new Dictionary<string, Guid>(),
                ValueRowDocumentIds = new Dictionary<string, Guid>()
            };

            // Add value distribution for categorical fields
            if (profile.Distinct <= 100 || (profile.Distinct <= 1000 && profile.Distinct <= _totalCount * 0.1))
            {
                profile.ValueDistribution = _valueDistribution
                    .OrderByDescending(v => v.Value)
                    .Take(100)
                    .ToDictionary(k => k.Key?.ToString() ?? "null", v => v.Value);
            }

            return profile;
        }
    }
}
