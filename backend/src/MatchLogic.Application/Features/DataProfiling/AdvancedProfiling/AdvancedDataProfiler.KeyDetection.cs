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
        /// Generates recommendations based on profile results
        /// </summary>
        public async Task<List<string>> GenerateRecommendationsAsync(AdvancedProfileResult profile)
        {
            var recommendations = new List<string>();

            // Check for column-level issues
            foreach (var columnEntry in profile.AdvancedColumnProfiles)
            {
                var columnName = columnEntry.Key;
                var columnProfile = columnEntry.Value;

                // Check for high null/empty ratio
                double nullEmptyRatio = (double)(columnProfile.Null) / columnProfile.Total;
                if (nullEmptyRatio > 0.2)
                {
                    recommendations.Add($"Column '{columnName}' has {nullEmptyRatio:P2} null or empty values. Consider validating data collection or making this field required.");
                }

                // Check for low quality score
                if (columnProfile.QualityScore?.OverallScore < 70)
                {
                    recommendations.Add($"Column '{columnName}' has a low quality score ({columnProfile.QualityScore?.OverallScore}/100). Review quality issues for this column.");
                }

                // Check for inconsistent data type
                if (columnProfile.TypeDetectionResults?.Count > 1 &&
                    columnProfile.TypeDetectionResults[0].Confidence < 0.9)
                {
                    recommendations.Add($"Column '{columnName}' has mixed data types. Consider standardizing values or splitting into multiple columns.");
                }

                // Check for high number of outliers
                if (columnProfile.Outliers?.Count > 0 &&
                    columnProfile.Outliers.Count > columnProfile.Total * 0.01)
                {
                    recommendations.Add($"Column '{columnName}' has {columnProfile.Outliers.Count} outliers. Review these values for potential data errors.");
                }

                // Check for regex/dictionary matches
                if (columnProfile.Patterns?.Any() == true)
                {
                    var bestPattern = columnProfile.Patterns.OrderByDescending(p => p.Count).First();
                    if (bestPattern.MatchPercentage < 80)
                    {
                        recommendations.Add($"Column '{columnName}' has low pattern matching ({bestPattern.MatchPercentage:F2}%). Consider standardizing values to match '{bestPattern.Pattern}'.");
                    }
                }

                // Check for abnormal distribution
                if (Math.Abs(columnProfile.Skewness) > 3 || columnProfile.Kurtosis > 10)
                {
                    recommendations.Add($"Column '{columnName}' has unusual statistical distribution (skewness: {columnProfile.Skewness:F2}, kurtosis: {columnProfile.Kurtosis:F2}). Verify if this is expected for your data domain.");
                }

                // Check for validation rule violations
                if (columnProfile.Violations?.Count > 0)
                {
                    recommendations.Add($"Column '{columnName}' has {columnProfile.Violations.Count} validation rule violations. Review and enforce data entry rules.");
                }

                // Check for inconsistent formatting
                if (columnProfile.DetectedFormat != null && columnProfile.DetectedFormat.Confidence < 0.8)
                {
                    recommendations.Add($"Column '{columnName}' has inconsistent formatting. Consider standardizing the format to '{columnProfile.DetectedFormat.Format}'.");
                }
            }

            // Check for dataset-level issues
            if (profile.CorrelationMatrix != null)
            {
                var highCorrelations = profile.ColumnRelationships?
                    .Where(r => r.Strength > 0.9)
                    .ToList();

                if (highCorrelations?.Count > 0)
                {
                    foreach (var correlation in highCorrelations.Take(3))
                    {
                        recommendations.Add($"High correlation ({correlation.Strength:F2}) detected between '{correlation.SourceColumn}' and '{correlation.TargetColumn}'. Consider if one column could be derived from the other.");
                    }

                    if (highCorrelations.Count > 3)
                    {
                        recommendations.Add($"{highCorrelations.Count - 3} more highly correlated column pairs detected. Consider database normalization.");
                    }
                }
            }

            // Check for candidate keys
            if (profile.CandidateKeys?.Count > 0)
            {
                var bestKey = profile.CandidateKeys.OrderByDescending(k => k.Uniqueness).First();
                if (bestKey.Uniqueness >= 0.99)
                {
                    var keyColumns = string.Join(", ", bestKey.Columns);
                    recommendations.Add($"Possible primary key detected: {keyColumns} with {bestKey.Uniqueness:P2} uniqueness. Consider defining this as a key in your data model.");
                }
                else if (bestKey.Uniqueness < 0.95 && bestKey.Uniqueness > 0.8)
                {
                    var keyColumns = string.Join(", ", bestKey.Columns);
                    recommendations.Add($"Potential duplicate data detected. Best key candidate ({keyColumns}) has {bestKey.Uniqueness:P2} uniqueness. Consider data deduplication.");
                }
            }

            // Check for functional dependencies
            if (profile.FunctionalDependencies?.Count > 0)
            {
                var strongDependencies = profile.FunctionalDependencies
                    .Where(fd => fd.Confidence > 0.95)
                    .Take(3)
                    .ToList();

                if (strongDependencies.Count > 0)
                {
                    recommendations.Add($"Strong functional dependencies detected. Consider normalizing your database schema to reduce redundancy.");
                }
            }

            // Check overall data quality score
            if (profile.DatasetQuality != null && profile.DatasetQuality.OverallScore < 70)
            {
                recommendations.Add($"Overall data quality score is low ({profile.DatasetQuality.OverallScore}/100). Review dataset quality issues and implement data governance practices.");
            }

            // Add generic recommendations if we have few specific ones
            if (recommendations.Count < 3)
            {
                if (profile.ColumnProfiles.Count > 10)
                {
                    recommendations.Add("Consider normalizing your data model as you have a large number of columns.");
                }

                if (profile.TotalRecords > 100000)
                {
                    recommendations.Add("Large dataset detected. Consider implementing data archiving or partitioning strategies for better performance.");
                }

                if (profile.AdvancedColumnProfiles.Values.Any(cp => cp.Clusters?.Count > 0))
                {
                    recommendations.Add("Data clustering detected. Consider segmenting your analysis by these natural data clusters.");
                }
            }

            return recommendations;
        }
    }
}
