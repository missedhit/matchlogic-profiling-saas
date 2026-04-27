using MatchLogic.Domain.DataProfiling;
using MathNet.Numerics.Statistics;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public partial class AdvancedColumnAnalyzer
    {
        /// <summary>
        /// Calculate type detection confidence based on type counters
        /// </summary>
        private double CalculateTypeDetectionConfidence()
        {
            if (_totalCount == 0 || _totalCount == _nullCount)
                return 1.0; // Default confidence for all null

            long nonNullCount = _totalCount - _nullCount;

            // Get counts for detected type
            int countForDetectedType = 0;

            switch (_type)
            {
                case "Integer":
                    countForDetectedType = _typeCounters["Integer"];
                    break;
                case "Decimal":
                    countForDetectedType = _typeCounters["Decimal"] + _typeCounters["Integer"];
                    break;
                case "Boolean":
                    countForDetectedType = _typeCounters["Boolean"];
                    break;
                case "DateTime":
                    countForDetectedType = _typeCounters["DateTime"];
                    break;
                case "String":
                    countForDetectedType = _typeCounters["String"];
                    break;
            }

            return (double)countForDetectedType / nonNullCount;
        }

        /// <summary>
        /// Create a histogram from numeric values
        /// </summary>
        private HistogramData CreateHistogram(IEnumerable<double> values, int binCount)
        {
            if (!values.Any())
                return new HistogramData { Bins = new List<double>(), Frequencies = new List<int>() };

            // Use MathNet.Numerics for histogram calculation
            var histogram = new Histogram(values, binCount);

            // Convert the bucket information to our HistogramData format
            var bins = new List<double>();

            // Add all bin edges
            for (int i = 0; i < histogram.BucketCount; i++)
            {
                bins.Add(histogram[i].LowerBound);
            }
            // Add the upper bound of the last bucket
            bins.Add(histogram.UpperBound);

            var result = new HistogramData
            {
                Bins = bins,
                Frequencies = Enumerable.Range(0, histogram.BucketCount)
                             .Select(i => (int)histogram[i].Count)
                             .ToList(),
                BinWidth = histogram[0].Width
            };

            return result;
        }

        /// <summary>
        /// Get examples for a detected format
        /// </summary>
        /* private List<string> GetFormatExamples(string format)
        {
            // Get examples from value rows with this format
            var examples = new List<string>();

            try
            {
                // Look for values matching the format
                foreach (var entry in _valueRows.Take(50))
                {
                    string value = entry.Key;

                    bool isMatch = false;

                    switch (format)
                    {
                        case "yyyy-MM-dd":
                            isMatch = value.Length == 10 && value[4] == '-' && value[7] == '-' &&
                                     DateTime.TryParse(value, out _);
                            break;
                        case "MM/dd/yyyy":
                            isMatch = value.Length == 10 && value[2] == '/' && value[5] == '/' &&
                                     DateTime.TryParse(value, out _);
                            break;
                        case "yyyy/MM/dd":
                            isMatch = value.Length == 10 && value[4] == '/' && value[7] == '/' &&
                                     DateTime.TryParse(value, out _);
                            break;
                        case "Decimal":
                            isMatch = value.Contains(".") && double.TryParse(value, out _);
                            break;
                        case "Integer":
                            isMatch = !value.Contains(".") && long.TryParse(value, out _);
                            break;
                        case "Currency":
                            isMatch = (value.StartsWith("$") || value.StartsWith("€") || value.StartsWith("£")) &&
                                     value.Contains(".") && double.TryParse(value.Substring(1), out _);
                            break;
                        case "PhoneNumber":
                            isMatch = value.Length >= 10 && value.Count(c => char.IsDigit(c)) >= 10 &&
                                     (value.Contains("-") || value.Contains("(") || value.Contains(")") || value.Contains(" "));
                            break;
                        case "Email":
                            isMatch = value.Contains("@") && value.Contains(".");
                            break;
                        case "URL":
                            isMatch = value.StartsWith("http://") || value.StartsWith("https://") || value.StartsWith("www.");
                            break;
                        case "IPAddress":
                            isMatch = value.Count(c => c == '.') == 3 &&
                                     value.Split('.').All(part => int.TryParse(part, out var num) && num >= 0 && num <= 255);
                            break;
                        case "GUID":
                            isMatch = Guid.TryParse(value, out _);
                            break;
                    }

                    if (isMatch && !examples.Contains(value))
                    {
                        examples.Add(value);

                        if (examples.Count >= 5)
                            break;
                    }
                }
            }
            catch
            {
                // Fallback if any error occurs
                return new List<string>();
            }

            return examples;
        }*/
    }
}
