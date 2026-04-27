using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public partial class AdvancedColumnAnalyzer
    {
        /// <summary>
        /// Update min/max values for string values
        /// </summary>
        private void UpdateMinMaxStringValue(string value, IDictionary<string, object> fullRow, long rowNumber)
        {
            bool isNewMin = false;
            bool isNewMax = false;

            lock (_minMaxLock)
            {
                if (_minValue == null || string.Compare(value, _minValue.ToString(), StringComparison.Ordinal) < 0)
                {
                    _minValue = value;
                    isNewMin = true;
                }

                if (_maxValue == null || string.Compare(value, _maxValue.ToString(), StringComparison.Ordinal) > 0)
                {
                    _maxValue = value;
                    isNewMax = true;
                }
            }

            // Store row references for min/max
            if (isNewMin)
            {
                var minRefs = new ConcurrentBag<RowReference>();
                minRefs.Add(CreateRowReference(fullRow, value, rowNumber));
                _characteristicRows[ProfileCharacteristic.Minimum] = minRefs;
            }

            if (isNewMax)
            {
                var maxRefs = new ConcurrentBag<RowReference>();
                maxRefs.Add(CreateRowReference(fullRow, value, rowNumber));
                _characteristicRows[ProfileCharacteristic.Maximum] = maxRefs;
            }
        }

        /// <summary>
        /// Update min/max values for any value type
        /// </summary>
        private void UpdateMinMaxValues(object value, IDictionary<string, object> fullRow, long rowNumber)
        {
            if (TryConvertToDouble(value, out double numericValue))
            {
                // For numeric values, track min/max numerically
                bool isNewMin = false;
                bool isNewMax = false;

                lock (_minMaxLock)
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
                }

                // Store row references for min/max
                if (isNewMin)
                {
                    var minRefs = new ConcurrentBag<RowReference>();
                    minRefs.Add(CreateRowReference(fullRow, value.ToString(), rowNumber));
                    _characteristicRows[ProfileCharacteristic.Minimum] = minRefs;
                }

                if (isNewMax)
                {
                    var maxRefs = new ConcurrentBag<RowReference>();
                    maxRefs.Add(CreateRowReference(fullRow, value.ToString(), rowNumber));
                    _characteristicRows[ProfileCharacteristic.Maximum] = maxRefs;
                }
            }
            else
            {
                // For non-numeric values, compare string representation
                UpdateMinMaxStringValue(value.ToString(), fullRow, rowNumber);
            }
        }

        /// <summary>
        /// Update statistics for numeric values
        /// </summary>
        private void UpdateNumericStatistics(double value)
        {
            // Skip if Value is NaN
            if (double.IsNaN(value))
                return;
            // Add to numeric values collection
            _numericValues.Add(value);

            // Update running sum and sum of powers for statistical moments
            // Use atomic operations to prevent race conditions
            lock (_numericStatsLock)
            {
                _sum += value;
                _sumOfSquares += (value * value);
                _sumOfCubes += (value * value * value);
                _sumOfQuads += (value * value * value * value);
            }
        }

        /// <summary>
        /// Try to convert a value to double
        /// </summary>
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

            if (value is byte b)
            {
                result = b;
                return true;
            }

            if (value is sbyte sb)
            {
                result = sb;
                return true;
            }

            if (value is short s)
            {
                result = s;
                return true;
            }

            if (value is ushort us)
            {
                result = us;
                return true;
            }

            if (value is uint ui)
            {
                result = ui;
                return true;
            }

            if (value is ulong ul && ul <= long.MaxValue)
            {
                result = ul;
                return true;
            }

            if (value is bool bl)
            {
                result = bl ? 1 : 0;
                return true;
            }

            if (value is DateTime dt)
            {
                result = dt.Ticks;
                return true;
            }

            // Try parsing string value
            return double.TryParse(value.ToString(), out result);
        }

        /// <summary>
        /// Create a row reference object
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
    }
}
