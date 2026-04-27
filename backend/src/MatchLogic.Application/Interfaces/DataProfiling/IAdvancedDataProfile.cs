using MatchLogic.Application.Features.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataProfiling
{
    public interface IAdvancedDataProfiler : IAsyncDisposable
    {
        /// <summary>
        /// Profiles the provided data stream with enhanced analytics capabilities
        /// </summary>
        /// <param name="dataStream">The data stream to profile</param>
        /// <param name="dataSourceName">Optional name of the data source</param>
        /// <param name="columnsToProfile">Optional list of specific columns to profile</param>
        /// <param name="options">Optional configuration options to override defaults</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Enhanced profile result with advanced analytics</returns>
        Task<AdvancedProfileResult> ProfileDataAsync(
            IAsyncEnumerable<IDictionary<string, object>> dataStream,
            DataSource dataSource = null,
            IEnumerable<string> columnsToProfile = null,
            AdvancedProfilingOptions options = null,
            string collectionName = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Evaluates data quality based on an existing profile
        /// </summary>
        /// <param name="profile">The profile to evaluate</param>
        /// <returns>Data quality assessment</returns>
        Task<DatasetQualityScore> EvaluateDataQualityAsync(AdvancedProfileResult profile);

        /// <summary>
        /// Generates recommendations based on profile results
        /// </summary>
        /// <param name="profile">The profile to analyze</param>
        /// <returns>List of recommendations for data improvement</returns>
        Task<List<string>> GenerateRecommendationsAsync(AdvancedProfileResult profile);
    }

    /// <summary>
    /// Interface for data quality service
    /// </summary>
    public interface IDataQualityService
    {
        /// <summary>
        /// Evaluates column quality
        /// </summary>
        /// <param name="columnProfile">The column profile to evaluate</param>
        /// <returns>Quality score for the column</returns>
        Task<DataQualityScore> EvaluateColumnQualityAsync(AdvancedColumnProfile columnProfile);

        /// <summary>
        /// Evaluates dataset quality
        /// </summary>
        /// <param name="profileResult">The profile result to evaluate</param>
        /// <returns>Quality score for the dataset</returns>
        Task<DatasetQualityScore> EvaluateDatasetQualityAsync(AdvancedProfileResult profileResult);

        /// <summary>
        /// Gets quality issues for a dataset
        /// </summary>
        /// <param name="profileResult">The profile result to analyze</param>
        /// <returns>List of quality issues</returns>
        Task<List<string>> GetQualityIssuesAsync(AdvancedProfileResult profileResult);
    }

    /// <summary>
    /// Interface for outlier detection service
    /// </summary>
    public interface IOutlierDetectionService
    {
        /// <summary>
        /// Detects outliers in a column
        /// </summary>
        /// <param name="values">The values to analyze</param>
        /// <param name="threshold">Z-score threshold</param>
        /// <param name="columnType">Data type of the column</param>
        /// <returns>List of outliers</returns>
        Task<List<Outlier>> DetectOutliersAsync(
            IEnumerable<object> values,
            double threshold = 3.0,
            string columnType = "String");

        /// <summary>
        /// Detects outliers in entire dataset
        /// </summary>
        /// <param name="rows">The rows to analyze</param>
        /// <param name="columnsToConsider">Optional list of columns to consider</param>
        /// <param name="threshold">Threshold for outlier detection</param>
        /// <returns>List of row-level outliers</returns>
        Task<List<RowOutlier>> DetectRowOutliersAsync(
            IEnumerable<IDictionary<string, object>> rows,
            IEnumerable<string> columnsToConsider = null,
            double threshold = 3.0);
    }

    /// <summary>
    /// Interface for advanced type detection service
    /// </summary>
    public interface ITypeDetectionService
    {
        /// <summary>
        /// Detects the most likely data type for a column
        /// </summary>
        /// <param name="values">Sample values from the column</param>
        /// <returns>Type detection results with confidence scores</returns>
        Task<List<TypeDetectionResult>> DetectTypeAsync(IEnumerable<object> values);

        /// <summary>
        /// Detects semantic types (e.g., email, phone number) for a column
        /// </summary>
        /// <param name="values">Sample values from the column</param>
        /// <returns>Semantic type detection results</returns>
        Task<List<SemanticType>> DetectSemanticTypeAsync(IEnumerable<string> values);
    }

    /// <summary>
    /// Interface for pattern discovery service
    /// </summary>
    public interface IPatternDiscoveryService
    {
        /// <summary>
        /// Discovers patterns in string data
        /// </summary>
        /// <param name="values">The values to analyze</param>
        /// <param name="maxPatterns">Maximum number of patterns to discover</param>
        /// <returns>List of discovered patterns</returns>
        Task<List<DiscoveredPattern>> DiscoverPatternsAsync(
            IEnumerable<string> values,
            int maxPatterns = 10);
    }

    /// <summary>
    /// Interface for validation rule service
    /// </summary>
    public interface IValidationRuleService
    {
        /// <summary>
        /// Gets applicable validation rules for a column
        /// </summary>
        /// <param name="columnProfile">The column profile</param>
        /// <returns>List of applicable validation rules</returns>
        Task<List<ValidationRule>> GetApplicableRulesAsync(AdvancedColumnProfile columnProfile);

        /// <summary>
        /// Validates values against rules
        /// </summary>
        /// <param name="values">The values to validate</param>
        /// <param name="rules">The rules to apply</param>
        /// <returns>List of validation violations</returns>
        Task<List<ValidationViolation>> ValidateAsync(
            IEnumerable<object> values,
            IEnumerable<ValidationRule> rules);
    }

    /// <summary>
    /// Interface for clustering service
    /// </summary>
    public interface IClusteringService
    {
        /// <summary>
        /// Detects clusters in a dataset
        /// </summary>
        /// <param name="rows">The rows to analyze</param>
        /// <param name="columnsToConsider">Optional list of columns to consider</param>
        /// <param name="maxClusters">Maximum number of clusters to detect</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of detected clusters</returns>
        Task<List<RowCluster>> DetectRowClustersAsync(
            IEnumerable<IDictionary<string, object>> rows,
            IEnumerable<string> columnsToConsider = null,
            int maxClusters = 5,
            CancellationToken cancellationToken = default);
    }
}
