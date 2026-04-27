using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public class DataQualityService : IDataQualityService
    {
        private readonly ILogger<DataQualityService> _logger;
        private readonly AdvancedProfilingOptions _options;

        public DataQualityService(
            ILogger<DataQualityService> logger,
            IOptions<AdvancedProfilingOptions> options)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options?.Value ?? new AdvancedProfilingOptions();
        }

        /// <summary>
        /// Evaluates column quality
        /// </summary>
        public async Task<DataQualityScore> EvaluateColumnQualityAsync(AdvancedColumnProfile columnProfile)
        {
            var score = new DataQualityScore();

            try
            {
                // Completeness: Percentage of non-null values
                score.Completeness = CalculateCompleteness(columnProfile);

                // Validity: Percentage of values that match expected patterns/rules
                score.Validity = CalculateValidity(columnProfile);

                // Accuracy: Based on outliers and violations
                score.Accuracy = CalculateAccuracy(columnProfile);

                // Consistency: Based on type detection confidence and pattern matching
                score.Consistency = CalculateConsistency(columnProfile);

                // Uniqueness: Based on distinct value percentage
                score.Uniqueness = CalculateUniqueness(columnProfile);

                // Overall score: Weighted average of individual scores
                score.OverallScore = CalculateOverallScore(score);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error evaluating quality for column {ColumnName}", columnProfile.FieldName);
                // Set a default low score in case of error
                score.OverallScore = 30;
            }

            return score;
        }

        /// <summary>
        /// Evaluates dataset quality
        /// </summary>
        public async Task<DatasetQualityScore> EvaluateDatasetQualityAsync(AdvancedProfileResult profileResult)
        {
            var score = new DatasetQualityScore
            {
                ColumnScores = new Dictionary<string, int>(),
                QualityIssues = new List<string>()
            };

            try
            {
                // Process each column
                double totalScore = 0;
                var columnScores = new Dictionary<string, int>();

                foreach (var columnEntry in profileResult.AdvancedColumnProfiles)
                {
                    var columnName = columnEntry.Key;
                    var columnProfile = columnEntry.Value;

                    // Evaluate column quality if not already evaluated
                    if (columnProfile.QualityScore == null)
                    {
                        columnProfile.QualityScore = await EvaluateColumnQualityAsync(columnProfile);
                    }

                    int columnScore = columnProfile.QualityScore.OverallScore;
                    totalScore += columnScore;
                    columnScores[columnName] = columnScore;

                    // Identify quality issues
                    IdentifyColumnQualityIssues(columnName, columnProfile, score.QualityIssues);
                }

                // Calculate overall score
                score.OverallScore = profileResult.AdvancedColumnProfiles.Count > 0
                    ? (int)Math.Round(totalScore / profileResult.AdvancedColumnProfiles.Count)
                    : 0;

                score.ColumnScores = columnScores;

                // Add dataset-level quality issues
                IdentifyDatasetQualityIssues(profileResult, score.QualityIssues);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error evaluating dataset quality");
                score.OverallScore = 30;
                score.QualityIssues.Add("Error evaluating dataset quality: " + ex.Message);
            }

            return score;
        }

        /// <summary>
        /// Gets quality issues for a dataset
        /// </summary>
        public async Task<List<string>> GetQualityIssuesAsync(AdvancedProfileResult profileResult)
        {
            var issues = new List<string>();

            try
            {
                // If dataset quality has already been evaluated, return existing issues
                if (profileResult.DatasetQuality?.QualityIssues != null &&
                    profileResult.DatasetQuality.QualityIssues.Count > 0)
                {
                    return profileResult.DatasetQuality.QualityIssues;
                }

                // Otherwise, evaluate dataset quality
                var qualityScore = await EvaluateDatasetQualityAsync(profileResult);
                issues = qualityScore.QualityIssues;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting quality issues");
                issues.Add("Error analyzing quality issues: " + ex.Message);
            }

            return issues;
        }

        #region Quality Score Calculation Methods

        /// <summary>
        /// Calculates completeness score (0-100)
        /// </summary>
        private int CalculateCompleteness(AdvancedColumnProfile profile)
        {
            // Completeness is the percentage of non-null values
            double nonNullPercentage = profile.Total > 0
                ? (double)(profile.Total - profile.Null) / profile.Total * 100
                : 0;

            return (int)Math.Round(nonNullPercentage);
        }

        /// <summary>
        /// Calculates validity score (0-100)
        /// </summary>
        private int CalculateValidity(AdvancedColumnProfile profile)
        {
            // Default validity score
            int validityScore = 75;

            // If pattern matching is available, use it
            if (profile.Pattern != "Unclassified" && profile.Patterns?.Count > 0)
            {
                var bestPattern = profile.Patterns.OrderByDescending(p => p.Count).First();
                validityScore = (int)Math.Round(bestPattern.MatchPercentage);
            }

            // If validation rules are available, use them
            if (profile.AppliedRules?.Count > 0)
            {
                int totalChecks = 0;
                int passedChecks = 0;

                foreach (var rule in profile.AppliedRules)
                {
                    totalChecks += rule.PassCount + rule.FailCount;
                    passedChecks += rule.PassCount;
                }

                if (totalChecks > 0)
                {
                    validityScore = (int)Math.Round((double)passedChecks / totalChecks * 100);
                }
            }

            return validityScore;
        }

        /// <summary>
        /// Calculates accuracy score (0-100)
        /// </summary>
        private int CalculateAccuracy(AdvancedColumnProfile profile)
        {
            // Start with a default score
            int accuracyScore = 85;

            // Reduce score based on outliers
            if (profile.Outliers?.Count > 0)
            {
                double outlierPercentage = (double)profile.Outliers.Count / profile.Filled;
                int outlierDeduction = (int)Math.Round(outlierPercentage * 100);
                accuracyScore = Math.Max(0, accuracyScore - outlierDeduction);
            }

            // Reduce score based on validation violations
            if (profile.Violations?.Count > 0)
            {
                int violationDeduction = Math.Min(25, profile.Violations.Count * 5);
                accuracyScore = Math.Max(0, accuracyScore - violationDeduction);
            }

            return accuracyScore;
        }

        /// <summary>
        /// Calculates consistency score (0-100)
        /// </summary>
        private int CalculateConsistency(AdvancedColumnProfile profile)
        {
            // Start with a score based on type detection confidence
            int consistencyScore = (int)Math.Round(profile.TypeDetectionConfidence * 100);

            // Consider pattern discovery results
            if (profile.DiscoveredPatterns?.Count > 0)
            {
                var dominantPattern = profile.DiscoveredPatterns.OrderByDescending(p => p.Coverage).First();

                // Adjust score based on pattern coverage
                double patternCoverage = dominantPattern.Coverage / 100;
                consistencyScore = (int)Math.Round(consistencyScore * 0.6 + patternCoverage * 100 * 0.4);
            }

            // Consider leading spaces and special characters as inconsistencies
            if (profile.LeadingSpaces > 0)
            {
                double leadingSpacePercentage = (double)profile.LeadingSpaces / profile.Filled;
                int deduction = (int)Math.Round(leadingSpacePercentage * 20);
                consistencyScore = Math.Max(0, consistencyScore - deduction);
            }

            return consistencyScore;
        }

        /// <summary>
        /// Calculates uniqueness score (0-100)
        /// </summary>
        private int CalculateUniqueness(AdvancedColumnProfile profile)
        {
            // Uniqueness is based on the percentage of distinct values
            double uniquenessRatio = profile.Filled > 0
                ? (double)profile.Distinct / profile.Filled
                : 0;

            // Scale uniqueness based on the column's purpose
            int uniquenessScore;

            // For potential keys, high uniqueness is good
            if (uniquenessRatio > 0.9 && profile.Distinct > 10)
            {
                uniquenessScore = 100;
            }
            // For categorical columns, appropriate cardinality is good
            else if (profile.Distinct <= 100 && profile.Distinct >= 2)
            {
                uniquenessScore = 90;
            }
            // For low-cardinality columns that should be categorical
            else if (profile.Distinct < 20)
            {
                uniquenessScore = 70;
            }
            // For other columns, moderate uniqueness is reasonable
            else
            {
                uniquenessScore = (int)Math.Round(50 + uniquenessRatio * 50);
            }

            return uniquenessScore;
        }

        /// <summary>
        /// Calculates overall quality score (0-100)
        /// </summary>
        private int CalculateOverallScore(DataQualityScore score)
        {
            // Define weights for each component
            const double CompletenessWeight = 0.25;
            const double ValidityWeight = 0.25;
            const double AccuracyWeight = 0.2;
            const double ConsistencyWeight = 0.15;
            const double UniquenessWeight = 0.15;

            // Calculate weighted average
            double overallScore =
                score.Completeness * CompletenessWeight +
                score.Validity * ValidityWeight +
                score.Accuracy * AccuracyWeight +
                score.Consistency * ConsistencyWeight +
                score.Uniqueness * UniquenessWeight;

            return (int)Math.Round(overallScore);
        }

        #endregion

        #region Quality Issue Identification Methods

        /// <summary>
        /// Identifies quality issues for a column
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
                double outlierPercentage = (double)profile.Outliers.Count / profile.Filled * 100;
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
            if (profile.LeadingSpaces > 0 && (double)profile.LeadingSpaces / profile.Filled > 0.1)
            {
                issues.Add($"Column '{columnName}' has inconsistent formatting with leading spaces.");
            }

            // Check for non-printable characters
            if (profile.NonPrintableCharacters > 0)
            {
                double nonPrintablePercentage = (double)profile.NonPrintableCharacters / profile.Filled * 100;
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

            // Check for low cardinality in categorical columns
            if (profile.ValueDistribution != null &&
                profile.ValueDistribution.Count == 1 &&
                profile.Total > 10)
            {
                issues.Add($"Column '{columnName}' has only one distinct value, suggesting it may be a constant column.");
            }

            // Check for semantic type confidence
            if (profile.PossibleSemanticTypes?.Count > 0 &&
                profile.PossibleSemanticTypes[0].Confidence < 0.7 &&
                profile.PossibleSemanticTypes[0].Type != "Unknown")
            {
                issues.Add($"Column '{columnName}' has low confidence ({profile.PossibleSemanticTypes[0].Confidence:P0}) in semantic type detection as '{profile.PossibleSemanticTypes[0].Type}'.");
            }

            // Check format confidence for structured data (dates, phone numbers, etc.)
            if (profile.DetectedFormat != null && profile.DetectedFormat.Confidence < 0.8)
            {
                issues.Add($"Column '{columnName}' has low format detection confidence ({profile.DetectedFormat.Confidence:P0}) for format '{profile.DetectedFormat.Format}'.");
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
                .Where(cp => cp.Value.QualityScore?.OverallScore < _options.DataQualityScoreThreshold)
                .Select(cp => cp.Key)
                .ToList();

            if (lowQualityColumns.Count > 0)
            {
                issues.Add($"Low quality data detected in columns: {string.Join(", ", lowQualityColumns)}");
            }

            // Check for overall data quality score
            if (profileResult.DatasetQuality != null && profileResult.DatasetQuality.OverallScore < 70)
            {
                issues.Add($"Overall dataset quality score is low ({profileResult.DatasetQuality.OverallScore}/100). Review dataset-level issues.");

                // Add specific dataset quality issues
                if (profileResult.DatasetQuality.QualityIssues?.Count > 0)
                {
                    issues.AddRange(profileResult.DatasetQuality.QualityIssues.Take(3));
                    if (profileResult.DatasetQuality.QualityIssues.Count > 3)
                    {
                        issues.Add($"(Plus {profileResult.DatasetQuality.QualityIssues.Count - 3} more data quality issues)");
                    }
                }
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
                issues.Add($"Dataset contains {profileResult.AdvancedColumnProfiles.Count} columns. Consider database normalization.");
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
            var columnsWithNulls = profileResult.AdvancedColumnProfiles
                .Where(cp => (double)cp.Value.Null / cp.Value.Total > 0.3)
                .Count();

            if (columnsWithNulls > profileResult.AdvancedColumnProfiles.Count / 3)
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

        #endregion
    }
}
