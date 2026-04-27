using MatchLogic.Domain.DataProfiling;
using MatchLogic.Application.Features.DataProfiling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public partial class AdvancedColumnAnalyzer
    {
        private readonly string _fieldName;
        private readonly AdvancedProfilingOptions _options;
        private readonly List<(Guid Id, string Name, System.Text.RegularExpressions.Regex Pattern)> _regexPatterns;
        private readonly List<(Guid Id, string Name, HashSet<string> Items)> _dictionaries;

        // Inferred data type (following hierarchy)
        private string _type = "String"; // Default to string
        private double _typeDetectionConfidence = 1.0;
        private readonly ConcurrentDictionary<string, int> _typeCounters = new();
        private int _length;

        // Basic counts
        private long _totalCount;
        private long _nullCount;
        private long _emptyCount;
        private readonly ConcurrentDictionary<object, long> _valueDistribution;
        private long _nonNullCount => _totalCount - _nullCount;
        private double _uniqueValuePercentage => _valueDistribution.Count > 0 ? (double)_valueDistribution.Count / _nonNullCount : 0;

        // Pattern tracking
        internal readonly ConcurrentDictionary<string, PatternMatchStats> _patternStats = new();
        private string _dominantPattern = "Unclassified";
        private const string DictionaryPrefix = "Library: ";
        private const string RegexPrefix = "Regular expression: ";


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
        private readonly ConcurrentBag<double> _numericValues = new();
        private double _sum;
        private double _sumOfSquares;
        private double _sumOfCubes;
        private double _sumOfQuads;

        // Value samples for machine learning
        private readonly ConcurrentBag<object> _valueSamples = new();

        // Row references
        internal readonly ConcurrentDictionary<ProfileCharacteristic, ConcurrentBag<RowReference>> _characteristicRows = new();
        internal readonly ConcurrentDictionary<string, ConcurrentBag<RowReference>> _valueRows = new();
        internal readonly ConcurrentBag<(object Value, RowReference RowRef)> _outlierCandidates = new();

        // Format detection
        private readonly ConcurrentDictionary<string, int> _formatPatterns = new();

        // Thread synchronization
        private readonly ReaderWriterLockSlim _valueDistributionLock = new(LockRecursionPolicy.SupportsRecursion);
        private readonly object _numericStatsLock = new();
        private readonly object _minMaxLock = new();

        // Public properties
        public long TotalCount => _totalCount;
        public long NonNullCount => _nonNullCount;
        public double UniqueValuePercentage => _uniqueValuePercentage;
        public bool HasNumericValues => _numericValues.Count > 0;
        public IEnumerable<double> NumericValues => _numericValues;

        /// <summary>
        /// Class to track pattern-specific validation stats
        /// </summary>
        internal class PatternMatchStats
        {
            public int TotalCountField;
            public int ValidCountField;
            public int InvalidCountField;

            // Properties that expose the fields
            public int TotalCount => TotalCountField;
            public int ValidCount => ValidCountField;
            public int InvalidCount => InvalidCountField;

            public string Description { get; set; } = string.Empty;

            public ConcurrentBag<RowReference> ValidRows { get; } = new();
            public ConcurrentBag<RowReference> InvalidRows { get; } = new();
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public AdvancedColumnAnalyzer(
            string fieldName,
            AdvancedProfilingOptions options,
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

            // Initialize type counters
            _typeCounters["Integer"] = 0;
            _typeCounters["Decimal"] = 0;
            _typeCounters["Boolean"] = 0;
            _typeCounters["DateTime"] = 0;
            _typeCounters["String"] = 0;
            _typeCounters["Guid"] = 0;
            _typeCounters["JSON"] = 0;
            _typeCounters["XML"] = 0;
        }

        /// <summary>
        /// Standard analyze method for processing a value
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Analyze(object value, IDictionary<string, object> fullRow, long rowNumber)
        {
            Interlocked.Increment(ref _totalCount);
            // Store row reference for total count
            if (_characteristicRows[ProfileCharacteristic.Total].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.Total].Add(
                    CreateRowReference(fullRow, value == null ? null : value.ToString(), rowNumber));
            }
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

            // Track value distribution
            string stringValue = value?.ToString();
            // Filled check for strings
            if (_characteristicRows[ProfileCharacteristic.Filled].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.Filled].Add(CreateRowReference(fullRow, stringValue, rowNumber));
            }

            try
            {
                _valueDistributionLock.EnterUpgradeableReadLock();

                bool isNewValue = !_valueDistribution.ContainsKey(value);

                // Track the value distribution
                _valueDistribution.AddOrUpdate(value, 1, (_, count) => count + 1);

                // If this is a new distinct value, add it to the distinctive characteristics
                if (isNewValue)
                {
                    try
                    {
                        _valueDistributionLock.EnterWriteLock();

                        if (_characteristicRows[ProfileCharacteristic.DistinctValue].Count < _options.MaxRowsPerCategory)
                        {
                            _characteristicRows[ProfileCharacteristic.DistinctValue].Add(
                                CreateRowReference(fullRow, stringValue, rowNumber));
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
                    if (!_valueRows.ContainsKey(stringValue))
                    {
                        _valueRows[stringValue] = new ConcurrentBag<RowReference>();
                    }

                    if (_valueRows[stringValue].Count < _options.MaxRowsPerCategory)
                    {
                        _valueRows[stringValue].Add(CreateRowReference(fullRow, stringValue, rowNumber));
                    }
                }

                // Store potential outlier candidate for later analysis
                //OV-R need to how we can optimize candidate outliers population
                if (_options.StoreOutlierRowReferences /*&& _outlierCandidates.Count < _options.MaxOutlierRowsToStore*/)
                {
                    var rowRef = CreateRowReference(fullRow, stringValue, rowNumber);
                    _outlierCandidates.Add((value, rowRef));
                }
            }
            finally
            {
                _valueDistributionLock.ExitUpgradeableReadLock();
            }

            // Update type statistics
            UpdateTypeStatistics(value);

            // Pattern matching
            MatchPatterns(value, stringValue, fullRow, rowNumber);

            // Character analysis
            if (value is string str)
            {
                // Use SIMD optimization for strings when available
                if (Vector.IsHardwareAccelerated && str.Length >= 16)
                {
                    AnalyzeCharactersWithSimd(str, fullRow, rowNumber);
                }
                else
                {
                    AnalyzeCharacters(str, fullRow, rowNumber);
                }

                // Detect format
                DetectFormat(str);
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

            // Try to convert to numeric for statistics
            if (TryConvertToDouble(value, out double numericValue))
            {
                UpdateNumericStatistics(numericValue);
            }
        }
    }
}
