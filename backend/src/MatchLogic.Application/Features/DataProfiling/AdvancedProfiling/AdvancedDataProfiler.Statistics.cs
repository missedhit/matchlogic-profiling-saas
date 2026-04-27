using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public partial class AdvancedDataProfiler
    {
        /// <summary>
        /// Calculate the median of a list of values
        /// </summary>
        private double CalculateMedian(List<double> values)
        {
            if (values == null || values.Count == 0)
                return 0;

            var sortedValues = values.OrderBy(v => v).ToList();
            int count = sortedValues.Count;

            if (count % 2 == 0)
            {
                // Even count, average the two middle values
                return (sortedValues[count / 2 - 1] + sortedValues[count / 2]) / 2;
            }
            else
            {
                // Odd count, return the middle value
                return sortedValues[count / 2];
            }
        }

        /// <summary>
        /// Calculate the standard deviation of a list of values
        /// </summary>
        private double CalculateStandardDeviation(List<double> values)
        {
            if (values == null || values.Count <= 1)
                return 0;

            double avg = values.Average();
            double sumOfSquaredDifferences = values.Sum(v => Math.Pow(v - avg, 2));
            return Math.Sqrt(sumOfSquaredDifferences / (values.Count - 1)); // Using n-1 for sample standard deviation
        }

        /// <summary>
        /// Evaluate data quality based on an existing profile
        /// </summary>
        public async Task<DatasetQualityScore> EvaluateDataQualityAsync(AdvancedProfileResult profile)
        {
            if (_dataQualityService == null)
            {
                var issues = new List<string>();

                // Generate quality issues manually if no service is available
                foreach (var column in profile.AdvancedColumnProfiles)
                {
                    IdentifyColumnQualityIssues(column.Key, column.Value, issues);
                }

                IdentifyDatasetQualityIssues(profile, issues);

                // Calculate a simple quality score based on issues found
                int overallScore = Math.Max(0, 100 - issues.Count * 5);

                return new DatasetQualityScore
                {
                    OverallScore = overallScore,
                    QualityIssues = issues
                };
            }

            return await _dataQualityService.EvaluateDatasetQualityAsync(profile);
        }

        /// <summary>
        /// Identify quality issues for a specific column
        /// </summary>
        private void IdentifyColumnQualityIssues(
            string columnName,
            AdvancedColumnProfile profile,
            List<string> issues)
        {
            // Check for null/empty values
            if (profile.Null > 0 && (double)profile.Null / profile.Total > 0.1)
            {
                double nullPercentage = (double)profile.Null / profile.Total * 100;
                issues.Add($"Column '{columnName}' has {nullPercentage:F1}% null values.");
            }

            // Check for pattern matching
            if (profile.Pattern == "Unclassified" && !string.IsNullOrEmpty(profile.Type) && profile.Type == "String")
            {
                issues.Add($"Column '{columnName}' has no clear pattern recognition.");
            }

            // Check discovered patterns for low coverage
            if (profile.DiscoveredPatterns?.Count > 0)
            {
                var inconsistentPatterns = profile.DiscoveredPatterns
                    .Where(p => p.Coverage < 70 && p.Count > 10)
                    .ToList();

                if (inconsistentPatterns.Count > 0)
                {
                    issues.Add($"Column '{columnName}' has multiple inconsistent data patterns, suggesting format inconsistency.");
                }
            }

            // Check for outliers
            if (profile.Outliers?.Count > 0)
            {
                double outlierPercentage = (double)profile.Outliers.Count / profile.Total * 100;
                if (outlierPercentage > 1)
                {
                    issues.Add($"Column '{columnName}' has {outlierPercentage:F1}% outlier values. Check for data entry errors.");
                }

                // Check for extreme outliers with very high z-scores
                var extremeOutliers = profile.Outliers.Where(o => Math.Abs(o.ZScore) > 5).Count();
                if (extremeOutliers > 0)
                {
                    issues.Add($"Column '{columnName}' has {extremeOutliers} extreme outliers that should be reviewed.");
                }
            }

            // Check for validation rule violations
            if (profile.Violations?.Count > 0)
            {
                int violationCount = profile.Violations.Count;
                issues.Add($"Column '{columnName}' has {violationCount} validation rule violations.");

                // Group violations by rule to identify common problems
                var commonViolations = profile.Violations
                    .GroupBy(v => v.RuleName)
                    .OrderByDescending(g => g.Count())
                    .Take(3)
                    .Select(g => $"{g.Key} ({g.Count()} occurrences)")
                    .ToList();

                if (commonViolations.Any())
                {
                    issues.Add($"Most common violations in '{columnName}': {string.Join(", ", commonViolations)}");
                }
            }

            // Check for mixed data types
            if (profile.TypeDetectionResults?.Count > 1 && profile.TypeDetectionResults[0].Confidence < 0.9)
            {
                var types = profile.TypeDetectionResults
                    .Take(3)
                    .Select(t => $"{t.DataType} ({t.Confidence:P0})")
                    .ToList();

                issues.Add($"Column '{columnName}' contains mixed data types: {string.Join(", ", types)}");
            }

            // Check for inconsistent formatting
            if (profile.LeadingSpaces > 0 && (double)profile.LeadingSpaces / profile.Total > 0.1)
            {
                issues.Add($"Column '{columnName}' has inconsistent formatting with leading spaces.");
            }

            // Check for non-printable characters
            if (profile.NonPrintableCharacters > 0)
            {
                double nonPrintablePercentage = (double)profile.NonPrintableCharacters / profile.Total * 100;
                if (nonPrintablePercentage > 0.5)
                {
                    issues.Add($"Column '{columnName}' contains {nonPrintablePercentage:F1}% non-printable characters, suggesting data corruption or encoding issues.");
                }
            }

            // Check for special characters in inappropriate contexts
            if (profile.Type != "String" && profile.Punctuation > 0)
            {
                issues.Add($"Column '{columnName}' contains special characters that may affect data quality.");
            }

            // Check for data distribution issues using statistical measures
            if (profile.Skewness != 0 && Math.Abs(profile.Skewness) > 3)
            {
                issues.Add($"Column '{columnName}' has highly skewed distribution (skewness: {profile.Skewness:F2}), which may impact analysis.");
            }

            if (profile.Kurtosis > 10)
            {
                issues.Add($"Column '{columnName}' has extreme kurtosis ({profile.Kurtosis:F2}), indicating potential data quality issues.");
            }
        }

        /// <summary>
        /// Identifies quality issues for the entire dataset
        /// </summary>
        private void IdentifyDatasetQualityIssues(
            AdvancedProfileResult profileResult,
            List<string> issues)
        {
            // Check for columns with low quality scores
            var lowQualityColumns = profileResult.AdvancedColumnProfiles
                .Where(cp => cp.Value.QualityScore?.OverallScore < _defaultOptions.DataQualityScoreThreshold)
                .Select(cp => cp.Key)
                .ToList();

            if (lowQualityColumns.Count > 0)
            {
                issues.Add($"Low quality data detected in columns: {string.Join(", ", lowQualityColumns)}");
            }

            // Check for potential duplicate rows
            if (profileResult.CandidateKeys?.Count > 0)
            {
                var bestKey = profileResult.CandidateKeys.OrderByDescending(k => k.Uniqueness).First();
                if (bestKey.Uniqueness < 0.95)
                {
                    double duplicatePercentage = (1 - bestKey.Uniqueness) * 100;
                    var keyColumns = string.Join(", ", bestKey.Columns);
                    issues.Add($"Approximately {duplicatePercentage:F1}% of records may be duplicates based on key candidate {keyColumns}. Consider de-duplication strategies.");
                }
            }

            // Check for strongly correlated columns
            if (profileResult.ColumnRelationships?.Count > 0)
            {
                var strongCorrelations = profileResult.ColumnRelationships
                    .Where(r => r.Strength > 0.95)
                    .ToList();

                if (strongCorrelations.Count > 0)
                {
                    issues.Add($"Found {strongCorrelations.Count} strongly correlated column pairs. Consider normalization.");

                    // List top correlations
                    var topCorrelations = strongCorrelations
                        .OrderByDescending(c => c.Strength)
                        .Take(3)
                        .Select(c => $"{c.SourceColumn} ↔ {c.TargetColumn} ({c.Strength:F2})")
                        .ToList();

                    if (topCorrelations.Any())
                    {
                        issues.Add($"Top correlations: {string.Join(", ", topCorrelations)}");
                    }
                }
            }

            // Check for functional dependencies
            if (profileResult.FunctionalDependencies?.Count > 0)
            {
                var strongDependencies = profileResult.FunctionalDependencies
                    .Where(fd => fd.Confidence > 0.95)
                    .ToList();

                if (strongDependencies.Count > 0)
                {
                    issues.Add($"Found {strongDependencies.Count} functional dependencies, suggesting possible normalization opportunities.");
                }
            }

            // Check for overall data volume
            if (profileResult.TotalRecords < 10)
            {
                issues.Add("Dataset contains very few records, which may impact analysis quality.");
            }

            // Check for excessive column count
            if (profileResult.AdvancedColumnProfiles.Count > 50)
            {
                issues.Add($"Dataset contains {profileResult.ColumnProfiles.Count} columns. Consider database normalization.");
            }

            // Check for data clusters that might indicate data segmentation
            if (profileResult.AdvancedColumnProfiles.Values.Any(cp => cp.Clusters?.Count > 3))
            {
                var columnsWithClusters = profileResult.AdvancedColumnProfiles
                    .Where(cp => cp.Value.Clusters?.Count > 3)
                    .Select(cp => cp.Key)
                    .Take(3)
                    .ToList();

                if (columnsWithClusters.Any())
                {
                    issues.Add($"Significant data clustering detected in columns: {string.Join(", ", columnsWithClusters)}. Data may have natural segments to consider in analysis.");
                }
            }

            // Check for systemic quality issues across multiple columns
            var columnsWithNulls = profileResult.ColumnProfiles
                .Where(cp => (double)cp.Value.Null / cp.Value.Total > 0.3)
                .Count();

            if (columnsWithNulls > profileResult.ColumnProfiles.Count / 3)
            {
                issues.Add($"{columnsWithNulls} columns have more than 30% null values. Consider investigating data collection processes.");
            }

            // Check for apparent schema issues
            var stringColumnsWithNumbers = profileResult.AdvancedColumnProfiles
                .Where(cp => cp.Value.Type == "String" && cp.Value.Numbers > 0 && cp.Value.Numbers > cp.Value.Letters)
                .Select(cp => cp.Key)
                .ToList();

            if (stringColumnsWithNumbers.Count > 0 && stringColumnsWithNumbers.Count <= 3)
            {
                issues.Add($"Columns may have incorrect data types: {string.Join(", ", stringColumnsWithNumbers)} contain primarily numeric values but are stored as strings.");
            }
            else if (stringColumnsWithNumbers.Count > 3)
            {
                issues.Add($"{stringColumnsWithNumbers.Count} string columns contain primarily numeric values but are stored as strings. Consider reviewing column data types.");
            }

            // Check for warnings from the profiling process
            if (profileResult.Warnings?.Count > 0)
            {
                issues.Add($"{profileResult.Warnings.Count} warnings were generated during profiling. Review them for potential data issues.");
            }
        }
    }
}
