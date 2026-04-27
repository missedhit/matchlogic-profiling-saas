using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public class OutlierDetectionService : IOutlierDetectionService
    {
        private readonly ILogger<OutlierDetectionService> _logger;
        private readonly AdvancedProfilingOptions _options;
        private readonly int _minSamplesForDetection = 10;

        public OutlierDetectionService(
            ILogger<OutlierDetectionService> logger,
            IOptions<AdvancedProfilingOptions> options = null)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options?.Value ?? new AdvancedProfilingOptions();
        }

        /// <summary>
        /// Detects outliers in a column using statistical methods
        /// </summary>
        public async Task<List<Outlier>> DetectOutliersAsync(
            IEnumerable<object> values,
            double threshold = 3.0,
            string columnType = "String")
        {
            var outliers = new List<Outlier>();

            // Extract non-null values
            var nonNullValues = values?.Where(v => v != null)?.ToList() ?? new List<object>();
            if (nonNullValues.Count == 0)
            {
                return outliers;
            }

            try
            {
                // Choose detection strategy based on column type
                if (columnType == "Integer" || columnType == "Decimal" || columnType == "Number")
                {
                    outliers = await DetectNumericOutliersAsync(nonNullValues, threshold);
                }
                else if (columnType == "String")
                {
                    outliers = await DetectStringOutliersAsync(nonNullValues, threshold);
                }
                else if (columnType == "DateTime")
                {
                    outliers = await DetectDateTimeOutliersAsync(nonNullValues, threshold);
                }
                else if (columnType == "Boolean")
                {
                    outliers = await DetectBooleanOutliersAsync(nonNullValues, threshold);
                }
                else
                {
                    // For other types, combine several methods
                    outliers = await DetectGenericOutliersAsync(nonNullValues, threshold);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error detecting outliers for {ColumnType} column", columnType);
            }

            return outliers.OrderByDescending(o => Math.Abs(o.ZScore)).ToList();
        }

        /// <summary>
        /// Detects outliers in entire dataset, identifying anomalous rows
        /// </summary>
        public async Task<List<RowOutlier>> DetectRowOutliersAsync(
            IEnumerable<IDictionary<string, object>> rows,
            IEnumerable<string> columnsToConsider = null,
            double threshold = 3.0)
        {
            var result = new List<RowOutlier>();
            var rowsList = rows?.ToList() ?? new List<IDictionary<string, object>>();

            if (rowsList.Count < _minSamplesForDetection)
            {
                return result;
            }

            try
            {
                // Determine which columns to analyze
                HashSet<string> columnsToAnalyze;

                if (columnsToConsider != null && columnsToConsider.Any())
                {
                    columnsToAnalyze = new HashSet<string>(columnsToConsider);
                }
                else
                {
                    // Auto-detect columns by taking keys from first non-empty row
                    var firstNonEmptyRow = rowsList.FirstOrDefault(r => r != null && r.Count > 0);
                    if (firstNonEmptyRow == null)
                    {
                        return result;
                    }

                    columnsToAnalyze = new HashSet<string>(firstNonEmptyRow.Keys);
                }

                // Filter out metadata columns
                columnsToAnalyze.Remove("_metadata");
                columnsToAnalyze.Remove("_id");

                // Step 1: Analyze each column for outliers separately
                var columnOutliers = new ConcurrentDictionary<string, List<Outlier>>();
                var columnTypes = new ConcurrentDictionary<string, string>();

                // Determine column types
                await Task.Run(() => {
                    Parallel.ForEach(columnsToAnalyze, column => {
                        // Extract column values
                        var columnValues = rowsList
                            .Where(r => r != null && r.ContainsKey(column))
                            .Select(r => r[column])
                            .ToList();

                        // Skip empty columns
                        if (columnValues.Count == 0)
                            return;

                        // Determine column type
                        string columnType = DetermineColumnType(columnValues);
                        columnTypes[column] = columnType;
                    });
                });

                // Detect outliers for each column
                await Task.Run(() => {
                    Parallel.ForEach(columnsToAnalyze, async column => {
                        // Extract column values
                        var columnValues = rowsList
                            .Where(r => r != null && r.ContainsKey(column))
                            .Select(r => r[column])
                            .ToList();

                        // Skip empty columns
                        if (columnValues.Count == 0)
                            return;

                        string columnType = columnTypes.GetValueOrDefault(column, "String");

                        // Detect outliers for this column
                        var outliers = await DetectOutliersAsync(columnValues, threshold, columnType);

                        // Store results if outliers found
                        if (outliers.Count > 0)
                        {
                            columnOutliers[column] = outliers;
                        }
                    });
                });

                // Step 2: Calculate outlier scores for each row
                var rowScores = new Dictionary<int, Dictionary<string, double>>();

                for (int i = 0; i < rowsList.Count; i++)
                {
                    var row = rowsList[i];
                    if (row == null)
                        continue;

                    var columnScores = new Dictionary<string, double>();
                    bool hasOutliers = false;

                    foreach (var column in columnsToAnalyze)
                    {
                        if (!row.ContainsKey(column) || row[column] == null)
                            continue;

                        // Skip if no outliers detected for this column
                        if (!columnOutliers.TryGetValue(column, out var outliers))
                            continue;

                        // Check if this value is an outlier
                        string valueStr = row[column].ToString();
                        var matchingOutlier = outliers.FirstOrDefault(o => o.Value == valueStr);

                        if (matchingOutlier != null)
                        {
                            columnScores[column] = matchingOutlier.ZScore;
                            hasOutliers = true;
                        }
                    }

                    // Add to results if outliers found
                    if (hasOutliers)
                    {
                        rowScores[i] = columnScores;
                    }
                }

                // Step 3: Build result objects
                foreach (var entry in rowScores)
                {
                    int rowIndex = entry.Key;
                    var scores = entry.Value;

                    // Calculate aggregated outlier score (average of absolute Z-scores)
                    double aggregateScore = scores.Values.Average(Math.Abs);

                    // Build result with correct properties
                    result.Add(new RowOutlier
                    {
                        RowData = rowsList[rowIndex],
                        AnomalyScore = aggregateScore,
                        ContributingFields = scores.Count,
                        FieldScores = scores.ToDictionary(kv => kv.Key, kv => Math.Abs(kv.Value))
                    });
                }

                // Step 4: Add multivariate outlier detection
                await DetectMultivariateOutliersAsync(rowsList, columnsToAnalyze, columnTypes, threshold, result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error detecting row outliers");
            }

            return result.OrderByDescending(o => o.AnomalyScore).ToList();
        }

        /// <summary>
        /// Detects multivariate outliers using Mahalanobis distance
        /// </summary>
        private async Task DetectMultivariateOutliersAsync(
            List<IDictionary<string, object>> rows,
            HashSet<string> columns,
            ConcurrentDictionary<string, string> columnTypes,
            double threshold,
            List<RowOutlier> result)
        {
            try
            {
                // Filter to numeric columns only for multivariate analysis
                var numericColumns = columnTypes
                    .Where(kv => kv.Value == "Integer" || kv.Value == "Decimal" || kv.Value == "Number")
                    .Select(kv => kv.Key)
                    .ToList();

                if (numericColumns.Count < 2)
                {
                    // Need at least 2 numeric columns for multivariate analysis
                    return;
                }

                // Extract numeric features for each row
                var rowFeatures = new List<(int RowIndex, Dictionary<string, double> Features)>();

                for (int i = 0; i < rows.Count; i++)
                {
                    var row = rows[i];
                    if (row == null)
                        continue;

                    var features = new Dictionary<string, double>();
                    bool hasAllValues = true;

                    foreach (var column in numericColumns)
                    {
                        if (!row.ContainsKey(column) || row[column] == null)
                        {
                            hasAllValues = false;
                            break;
                        }

                        if (TryConvertToDouble(row[column], out double value))
                        {
                            features[column] = value;
                        }
                        else
                        {
                            hasAllValues = false;
                            break;
                        }
                    }

                    if (hasAllValues && features.Count == numericColumns.Count)
                    {
                        rowFeatures.Add((i, features));
                    }
                }

                // Need enough samples for meaningful analysis
                if (rowFeatures.Count < _minSamplesForDetection)
                {
                    return;
                }

                // Calculate means for each column
                var means = new Dictionary<string, double>();
                foreach (var column in numericColumns)
                {
                    means[column] = rowFeatures.Average(r => r.Features[column]);
                }

                // Calculate covariance matrix
                var covariance = new Dictionary<(string, string), double>();
                foreach (var col1 in numericColumns)
                {
                    foreach (var col2 in numericColumns)
                    {
                        double sum = 0;
                        foreach (var (_, features) in rowFeatures)
                        {
                            sum += (features[col1] - means[col1]) * (features[col2] - means[col2]);
                        }
                        covariance[(col1, col2)] = sum / (rowFeatures.Count - 1);
                    }
                }

                // Calculate Mahalanobis distances (simplified approach)
                var distances = new Dictionary<int, double>();

                foreach (var (rowIndex, features) in rowFeatures)
                {
                    // Simplified distance calculation
                    double sumSquaredDiff = 0;
                    foreach (var col in numericColumns)
                    {
                        double diff = features[col] - means[col];
                        double variance = covariance[(col, col)];
                        if (variance > 0.0001) // Avoid division by very small numbers
                        {
                            sumSquaredDiff += (diff * diff) / variance;
                        }
                    }

                    double distance = Math.Sqrt(sumSquaredDiff);
                    distances[rowIndex] = distance;
                }

                // Detect multivariate outliers
                double meanDistance = distances.Values.Average();
                double stdDevDistance = Math.Sqrt(distances.Values.Sum(d => Math.Pow(d - meanDistance, 2)) / distances.Count);

                foreach (var (rowIndex, distance) in distances)
                {
                    double zScore = (distance - meanDistance) / (stdDevDistance > 0.0001 ? stdDevDistance : 0.0001);

                    if (zScore > threshold)
                    {
                        // Check if this row is already in results
                        var existingOutlier = result.FirstOrDefault(o => o.RowData == rows[rowIndex]);

                        if (existingOutlier != null)
                        {
                            // Update existing outlier with multivariate score
                            existingOutlier.AnomalyScore = Math.Max(existingOutlier.AnomalyScore, zScore);

                            // Add a special field score to indicate it's multivariate
                            existingOutlier.FieldScores["_multivariate"] = zScore;
                            existingOutlier.ContributingFields++;
                        }
                        else
                        {
                            // Create new multivariate outlier
                            var fieldScores = new Dictionary<string, double>();

                            // Add a special field score to indicate it's multivariate
                            fieldScores["_multivariate"] = zScore;

                            result.Add(new RowOutlier
                            {
                                RowData = rows[rowIndex],
                                AnomalyScore = zScore,
                                ContributingFields = 1, // Just the multivariate analysis
                                FieldScores = fieldScores
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error detecting multivariate outliers");
            }
        }

        /// <summary>
        /// Determines the data type of a column based on sample values
        /// </summary>
        private string DetermineColumnType(List<object> values)
        {
            // Skip null values for type detection
            var nonNullValues = values.Where(v => v != null).ToList();
            if (nonNullValues.Count == 0)
                return "String"; // Default to string for empty columns

            // Check for consistent types
            int intCount = 0;
            int decimalCount = 0;
            int dateCount = 0;
            int boolCount = 0;
            int stringCount = 0;

            foreach (var value in nonNullValues)
            {
                if (value is int || value is long || value is short || value is byte ||
                    value is uint || value is ulong || value is ushort || value is sbyte)
                {
                    intCount++;
                }
                else if (value is double || value is float || value is decimal)
                {
                    decimalCount++;
                }
                else if (value is DateTime)
                {
                    dateCount++;
                }
                else if (value is bool)
                {
                    boolCount++;
                }
                else
                {
                    string strValue = value.ToString();

                    // Try to parse as various types
                    if (int.TryParse(strValue, out _))
                    {
                        intCount++;
                    }
                    else if (double.TryParse(strValue, out _))
                    {
                        decimalCount++;
                    }
                    else if (DateTime.TryParse(strValue, out _))
                    {
                        dateCount++;
                    }
                    else if (bool.TryParse(strValue, out _) ||
                             strValue.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
                             strValue.Equals("no", StringComparison.OrdinalIgnoreCase) ||
                             strValue.Equals("y", StringComparison.OrdinalIgnoreCase) ||
                             strValue.Equals("n", StringComparison.OrdinalIgnoreCase))
                    {
                        boolCount++;
                    }
                    else
                    {
                        stringCount++;
                    }
                }
            }

            // Determine dominant type (using a threshold percentage)
            double threshold = 0.7; // 70% of values should be of consistent type
            int total = nonNullValues.Count;

            if ((double)intCount / total >= threshold)
                return "Integer";
            if ((double)(intCount + decimalCount) / total >= threshold)
                return "Decimal";
            if ((double)dateCount / total >= threshold)
                return "DateTime";
            if ((double)boolCount / total >= threshold)
                return "Boolean";

            // Default to string if no clear type dominates
            return "String";
        }

        /// <summary>
        /// Detects outliers in numeric data using Z-score method
        /// </summary>
        private async Task<List<Outlier>> DetectNumericOutliersAsync(List<object> values, double threshold)
        {
            var outliers = new List<Outlier>();

            // Try to convert values to doubles
            var numericValues = new List<(object Original, double Value)>();
            foreach (var value in values)
            {
                if (TryConvertToDouble(value, out double numericValue))
                {
                    numericValues.Add((value, numericValue));
                }
            }

            if (numericValues.Count < _minSamplesForDetection)
            {
                return outliers;
            }

            // Calculate mean and standard deviation
            double[] doubleValues = numericValues.Select(x => x.Value).ToArray();
            double mean = doubleValues.Average();
            double variance = doubleValues.Sum(x => Math.Pow(x - mean, 2)) / doubleValues.Length;
            double stdDev = Math.Sqrt(variance);

            // For very small standard deviations, use a minimum to avoid division issues
            if (stdDev < 0.0001)
            {
                stdDev = 0.0001;
            }

            // Detect outliers using Z-score
            foreach (var (original, value) in numericValues)
            {
                double zScore = (value - mean) / stdDev;

                if (Math.Abs(zScore) > threshold)
                {
                    outliers.Add(new Outlier
                    {
                        Value = original.ToString(),
                        ZScore = zScore,
                        RowReference = null // Will be populated later if row data is available
                    });
                }
            }

            return outliers;
        }

        /// <summary>
        /// Detects outliers in string data using length and frequency
        /// </summary>
        private async Task<List<Outlier>> DetectStringOutliersAsync(List<object> values, double threshold)
        {
            var outliers = new List<Outlier>();

            // Convert to strings
            var stringValues = values.Select(v => v.ToString()).ToList();

            // 1. Detect length-based outliers
            var lengths = stringValues.Select(s => (double)s.Length).ToArray();
            double meanLength = lengths.Average();
            double varianceLength = lengths.Sum(x => Math.Pow(x - meanLength, 2)) / lengths.Length;
            double stdDevLength = Math.Sqrt(varianceLength);

            // For very small standard deviations, use a minimum to avoid division issues
            if (stdDevLength < 0.0001)
            {
                stdDevLength = 0.0001;
            }

            for (int i = 0; i < stringValues.Count; i++)
            {
                string value = stringValues[i];
                double zScore = (value.Length - meanLength) / stdDevLength;

                if (Math.Abs(zScore) > threshold)
                {
                    outliers.Add(new Outlier
                    {
                        Value = value,
                        ZScore = zScore,
                        RowReference = null
                    });
                }
            }

            // 2. Detect frequency-based outliers (very rare values)
            var valueCounts = new Dictionary<string, int>();
            foreach (var value in stringValues)
            {
                if (valueCounts.ContainsKey(value))
                {
                    valueCounts[value]++;
                }
                else
                {
                    valueCounts[value] = 1;
                }
            }

            // Calculate frequency statistics
            var frequencies = valueCounts.Values.Select(v => (double)v).ToArray();
            double meanFreq = frequencies.Average();
            double varianceFreq = frequencies.Sum(x => Math.Pow(x - meanFreq, 2)) / frequencies.Length;
            double stdDevFreq = Math.Sqrt(varianceFreq);

            // Only add frequency-based outliers if we have enough different values
            if (valueCounts.Count > 5 && stdDevFreq > 0.0001)
            {
                foreach (var kvp in valueCounts)
                {
                    double zScore = (kvp.Value - meanFreq) / stdDevFreq;
                    // Only count very rare values as outliers (negative z-score)
                    if (zScore < -threshold)
                    {
                        // Don't add duplicates
                        if (!outliers.Any(o => o.Value == kvp.Key))
                        {
                            outliers.Add(new Outlier
                            {
                                Value = kvp.Key,
                                ZScore = zScore,
                                RowReference = null
                            });
                        }
                    }
                }
            }

            return outliers;
        }

        /// <summary>
        /// Detects outliers in DateTime data
        /// </summary>
        private async Task<List<Outlier>> DetectDateTimeOutliersAsync(List<object> values, double threshold)
        {
            var outliers = new List<Outlier>();

            // Try to parse dates
            var dateValues = new List<(object Original, DateTime Value)>();
            foreach (var value in values)
            {
                if (value is DateTime dt)
                {
                    dateValues.Add((value, dt));
                }
                else if (DateTime.TryParse(value.ToString(), out DateTime parsedDate))
                {
                    dateValues.Add((value, parsedDate));
                }
            }

            if (dateValues.Count < _minSamplesForDetection)
            {
                return outliers;
            }

            // Convert to ticks for statistical calculations
            var ticks = dateValues.Select(d => (double)d.Value.Ticks).ToArray();
            double meanTicks = ticks.Average();
            double varianceTicks = ticks.Sum(x => Math.Pow(x - meanTicks, 2)) / ticks.Length;
            double stdDevTicks = Math.Sqrt(varianceTicks);

            // Detect outliers based on ticks
            foreach (var (original, date) in dateValues)
            {
                double zScore = ((double)date.Ticks - meanTicks) / stdDevTicks;

                if (Math.Abs(zScore) > threshold)
                {
                    outliers.Add(new Outlier
                    {
                        Value = original.ToString(),
                        ZScore = zScore,
                        RowReference = null
                    });
                }
            }

            return outliers;
        }

        /// <summary>
        /// Detects outliers in boolean data based on frequency
        /// </summary>
        private async Task<List<Outlier>> DetectBooleanOutliersAsync(List<object> values, double threshold)
        {
            var outliers = new List<Outlier>();

            // Try to parse booleans
            var boolValues = new List<(object Original, bool Value)>();
            foreach (var value in values)
            {
                if (value is bool b)
                {
                    boolValues.Add((value, b));
                }
                else if (bool.TryParse(value.ToString(), out bool parsedBool))
                {
                    boolValues.Add((value, parsedBool));
                }
                else
                {
                    // Try to parse common boolean representations
                    string strValue = value.ToString().ToLowerInvariant();
                    if (strValue == "yes" || strValue == "y" || strValue == "1" || strValue == "t" || strValue == "true")
                    {
                        boolValues.Add((value, true));
                    }
                    else if (strValue == "no" || strValue == "n" || strValue == "0" || strValue == "f" || strValue == "false")
                    {
                        boolValues.Add((value, false));
                    }
                }
            }

            if (boolValues.Count < _minSamplesForDetection)
            {
                return outliers;
            }

            // Count true/false values
            int trueCount = boolValues.Count(b => b.Value);
            int falseCount = boolValues.Count - trueCount;

            // If one value is very rare (below 5% or based on threshold), mark it as outlier
            if (trueCount > 0 && (double)trueCount / boolValues.Count < 0.05)
            {
                // Add a sample of the rare true values
                var rareValues = boolValues.Where(b => b.Value).Take(5);
                foreach (var (original, _) in rareValues)
                {
                    outliers.Add(new Outlier
                    {
                        Value = original.ToString(),
                        ZScore = -((double)boolValues.Count / trueCount), // Negative Z-score approximation
                        RowReference = null
                    });
                }
            }
            else if (falseCount > 0 && (double)falseCount / boolValues.Count < 0.05)
            {
                // Add a sample of the rare false values
                var rareValues = boolValues.Where(b => !b.Value).Take(5);
                foreach (var (original, _) in rareValues)
                {
                    outliers.Add(new Outlier
                    {
                        Value = original.ToString(),
                        ZScore = -((double)boolValues.Count / falseCount), // Negative Z-score approximation
                        RowReference = null
                    });
                }
            }

            return outliers;
        }

        /// <summary>
        /// Detects outliers for generic data types
        /// </summary>
        private async Task<List<Outlier>> DetectGenericOutliersAsync(List<object> values, double threshold)
        {
            var outliers = new List<Outlier>();

            // Try numeric detection first
            var numericOutliers = await DetectNumericOutliersAsync(values, threshold);
            outliers.AddRange(numericOutliers);

            // Then try string-based detection
            var stringOutliers = await DetectStringOutliersAsync(values, threshold);

            // Add non-duplicate string outliers
            foreach (var outlier in stringOutliers)
            {
                if (!outliers.Any(o => o.Value == outlier.Value))
                {
                    outliers.Add(outlier);
                }
            }

            return outliers;
        }

        #region Utility Methods

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

        #endregion
    }
}
