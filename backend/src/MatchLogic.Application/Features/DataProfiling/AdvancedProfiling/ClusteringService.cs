using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public class ClusteringService : IClusteringService
    {
        private readonly ILogger<ClusteringService> _logger;
        private readonly AdvancedProfilingOptions _options;
        private readonly int _minRowsForClustering = 10;

        public ClusteringService(
            ILogger<ClusteringService> logger,
            IOptions<AdvancedProfilingOptions> options = null)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options?.Value ?? new AdvancedProfilingOptions();
        }

        /// <summary>
        /// Detects clusters in a dataset
        /// </summary>
        /// <param name="rows">The rows to analyze</param>
        /// <param name="columnsToConsider">Optional list of columns to consider</param>
        /// <param name="maxClusters">Maximum number of clusters to detect</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of detected clusters</returns>
        public async Task<List<RowCluster>> DetectRowClustersAsync(
            IEnumerable<IDictionary<string, object>> rows,
            IEnumerable<string> columnsToConsider = null,
            int maxClusters = 5,
            CancellationToken cancellationToken = default)
        {
            return await ClusterDataAsync(rows, columnsToConsider, maxClusters, cancellationToken);
        }

        /// <summary>
        /// Performs clustering on a collection of data rows
        /// </summary>
        private async Task<List<RowCluster>> ClusterDataAsync(
            IEnumerable<IDictionary<string, object>> rows,
            IEnumerable<string> columnsToConsider = null,
            int maxClusters = 5,
            CancellationToken cancellationToken = default)
        {
            var result = new List<RowCluster>();
            var rowList = rows?.ToList() ?? new List<IDictionary<string, object>>();

            if (rowList.Count < _minRowsForClustering)
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
                    var firstNonEmptyRow = rowList.FirstOrDefault(r => r != null && r.Count > 0);
                    if (firstNonEmptyRow == null)
                    {
                        return result;
                    }

                    columnsToAnalyze = new HashSet<string>(firstNonEmptyRow.Keys);
                }

                // Filter out metadata columns
                columnsToAnalyze.Remove("_metadata");
                columnsToAnalyze.Remove("_id");

                // Execute clustering algorithm
                result = await PerformClusteringAsync(rowList, columnsToAnalyze, maxClusters, cancellationToken);

                // Calculate silhouette scores to evaluate cluster quality
                if (result.Count > 1)
                {
                    await CalculateSilhouetteScoresAsync(result, rowList, columnsToAnalyze);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error performing clustering");
            }

            return result;
        }

        /// <summary>
        /// Performs simple K-means style clustering on the data
        /// </summary>
        private async Task<List<RowCluster>> PerformClusteringAsync(
            List<IDictionary<string, object>> rows,
            HashSet<string> columnsToAnalyze,
            int maxClusters,
            CancellationToken cancellationToken)
        {
            var result = new List<RowCluster>();

            // Create feature vectors for each row
            var rowFeatures = new List<Dictionary<string, double>>();
            foreach (var row in rows)
            {
                var features = ExtractFeatures(row, columnsToAnalyze);
                rowFeatures.Add(features);
            }

            // Determine optimal number of clusters (capped at maxClusters)
            int k = Math.Min(
                maxClusters,
                Math.Max(2, (int)Math.Sqrt(rows.Count / 2))
            );

            // Initialize cluster centers randomly from the data
            var random = new Random(42); // Fixed seed for reproducibility
            var clusterCenters = new List<Dictionary<string, double>>();
            var selectedIndices = new HashSet<int>();

            for (int i = 0; i < k; i++)
            {
                int randomIndex;

                // Ensure we don't select the same center twice
                do
                {
                    randomIndex = random.Next(rowFeatures.Count);
                } while (selectedIndices.Contains(randomIndex));

                selectedIndices.Add(randomIndex);
                clusterCenters.Add(new Dictionary<string, double>(rowFeatures[randomIndex]));
            }

            // Run K-means iterations
            var assignments = new int[rows.Count];
            bool changed = true;
            int iteration = 0;
            int maxIterations = 100;

            // Check for cancellation before starting iterations
            cancellationToken.ThrowIfCancellationRequested();

            await Task.Run(() => {
                while (changed && iteration < maxIterations)
                {
                    changed = false;
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    // Assign each row to nearest cluster
                    Parallel.For(0, rows.Count, i =>
                    {
                        double minDistance = double.MaxValue;
                        int bestCluster = 0;

                        for (int c = 0; c < k; c++)
                        {
                            double distance = CalculateDistance(rowFeatures[i], clusterCenters[c]);

                            if (distance < minDistance)
                            {
                                minDistance = distance;
                                bestCluster = c;
                            }
                        }

                        // Check if assignment changed
                        if (assignments[i] != bestCluster)
                        {
                            assignments[i] = bestCluster;
                            changed = true;
                        }
                    });

                    // Recalculate cluster centers
                    var newCenters = new Dictionary<string, double>[k];
                    var counts = new int[k];

                    for (int c = 0; c < k; c++)
                    {
                        newCenters[c] = new Dictionary<string, double>();
                        counts[c] = 0;
                    }

                    // Sum features for each cluster
                    for (int i = 0; i < rows.Count; i++)
                    {
                        int cluster = assignments[i];
                        counts[cluster]++;

                        foreach (var feature in rowFeatures[i])
                        {
                            if (!newCenters[cluster].ContainsKey(feature.Key))
                            {
                                newCenters[cluster][feature.Key] = 0;
                            }

                            newCenters[cluster][feature.Key] += feature.Value;
                        }
                    }

                    // Calculate averages for new centers
                    for (int c = 0; c < k; c++)
                    {
                        if (counts[c] > 0)
                        {
                            foreach (var feature in newCenters[c].Keys.ToList())
                            {
                                newCenters[c][feature] /= counts[c];
                            }

                            clusterCenters[c] = newCenters[c];
                        }
                    }

                    iteration++;
                }
            }, cancellationToken);

            // Create results
            var clusterCounts = new int[k];
            var clusterSamples = new List<IDictionary<string, object>>[k];
            var clusterFeatures = new Dictionary<int, Dictionary<string, double>>();

            for (int c = 0; c < k; c++)
            {
                clusterCounts[c] = 0;
                clusterSamples[c] = new List<IDictionary<string, object>>();
                clusterFeatures[c] = clusterCenters[c];
            }

            // Count cluster members and collect samples
            for (int i = 0; i < rows.Count; i++)
            {
                int cluster = assignments[i];
                clusterCounts[cluster]++;

                // Collect sample rows (up to 10 per cluster)
                if (clusterSamples[cluster].Count < 10)
                {
                    clusterSamples[cluster].Add(rows[i]);
                }
            }

            // Build result objects
            for (int c = 0; c < k; c++)
            {
                if (clusterCounts[c] > 0)
                {
                    result.Add(new RowCluster
                    {
                        ClusterId = c,
                        Count = clusterCounts[c],
                        SampleRows = clusterSamples[c],
                        DistinctiveFeatures = IdentifyDistinctiveFeatures(c, clusterFeatures)
                    });
                }
            }

            return result;
        }

        /// <summary>
        /// Extracts numeric features from a row
        /// </summary>
        private Dictionary<string, double> ExtractFeatures(
            IDictionary<string, object> row,
            HashSet<string> columnsToAnalyze)
        {
            var features = new Dictionary<string, double>();

            foreach (var column in columnsToAnalyze)
            {
                if (row.TryGetValue(column, out var value) && value != null)
                {
                    if (TryConvertToDouble(value, out double numericValue))
                    {
                        features[column] = numericValue;
                    }
                    else
                    {
                        // For non-numeric values, encode using hash
                        string strValue = value.ToString();

                        if (!string.IsNullOrEmpty(strValue))
                        {
                            // Use hash code as a simple encoding
                            // Normalize to a reasonable range (-1 to 1)
                            double hash = strValue.GetHashCode() % 1000 / 1000.0;
                            features[column] = hash;
                        }
                        else
                        {
                            features[column] = 0;
                        }
                    }
                }
                else
                {
                    // Use a special value for missing data
                    features[column] = -999.0;
                }
            }

            return features;
        }

        /// <summary>
        /// Calculates Euclidean distance between feature vectors
        /// </summary>
        private double CalculateDistance(
            Dictionary<string, double> features1,
            Dictionary<string, double> features2)
        {
            double sumOfSquares = 0;

            // Create a set of all feature keys
            var allKeys = new HashSet<string>(features1.Keys);
            allKeys.UnionWith(features2.Keys);

            foreach (var key in allKeys)
            {
                double value1 = features1.TryGetValue(key, out var v1) ? v1 : 0;
                double value2 = features2.TryGetValue(key, out var v2) ? v2 : 0;

                double diff = value1 - value2;
                sumOfSquares += diff * diff;
            }

            return Math.Sqrt(sumOfSquares);
        }

        /// <summary>
        /// Identifies distinctive features for each cluster
        /// </summary>
        private Dictionary<string, double> IdentifyDistinctiveFeatures(
            int clusterId,
            Dictionary<int, Dictionary<string, double>> clusterFeatures)
        {
            var distinctiveFeatures = new Dictionary<string, double>();

            if (clusterFeatures.Count <= 1)
            {
                return distinctiveFeatures;
            }

            // Calculate global means across all clusters
            var globalMeans = new Dictionary<string, double>();
            var featureCounts = new Dictionary<string, int>();

            foreach (var cluster in clusterFeatures.Values)
            {
                foreach (var feature in cluster)
                {
                    if (!globalMeans.ContainsKey(feature.Key))
                    {
                        globalMeans[feature.Key] = 0;
                        featureCounts[feature.Key] = 0;
                    }

                    globalMeans[feature.Key] += feature.Value;
                    featureCounts[feature.Key]++;
                }
            }

            foreach (var key in globalMeans.Keys.ToList())
            {
                if (featureCounts[key] > 0)
                {
                    globalMeans[key] /= featureCounts[key];
                }
            }

            // Calculate standard deviations
            var globalStdDevs = new Dictionary<string, double>();

            foreach (var key in globalMeans.Keys)
            {
                double sumOfSquaredDiff = 0;
                int count = 0;

                foreach (var cluster in clusterFeatures.Values)
                {
                    if (cluster.TryGetValue(key, out double value))
                    {
                        double diff = value - globalMeans[key];
                        sumOfSquaredDiff += diff * diff;
                        count++;
                    }
                }

                if (count > 1)
                {
                    globalStdDevs[key] = Math.Sqrt(sumOfSquaredDiff / count);
                }
                else
                {
                    globalStdDevs[key] = 0.0001; // Small default to avoid division by zero
                }
            }

            // Find distinctive features for this cluster
            var clusterCenter = clusterFeatures[clusterId];

            foreach (var feature in clusterCenter)
            {
                if (globalMeans.TryGetValue(feature.Key, out double globalMean) &&
                    globalStdDevs.TryGetValue(feature.Key, out double stdDev) &&
                    stdDev > 0.0001) // Avoid division by very small numbers
                {
                    // Calculate Z-score for this feature
                    double zScore = (feature.Value - globalMean) / stdDev;

                    // Consider it distinctive if Z-score magnitude is above threshold
                    if (Math.Abs(zScore) > 1.0)
                    {
                        distinctiveFeatures[feature.Key] = zScore;
                    }
                }
            }

            return distinctiveFeatures;
        }

        /// <summary>
        /// Calculates silhouette scores to evaluate cluster quality
        /// </summary>
        private async Task CalculateSilhouetteScoresAsync(
            List<RowCluster> clusters,
            List<IDictionary<string, object>> rows,
            HashSet<string> columnsToAnalyze)
        {
            await Task.Run(() => {
                try
                {
                    // Extract features for each row
                    var rowFeatures = new List<Dictionary<string, double>>();
                    foreach (var row in rows)
                    {
                        rowFeatures.Add(ExtractFeatures(row, columnsToAnalyze));
                    }

                    // Group rows by cluster
                    var rowsByCluster = new Dictionary<int, List<int>>();

                    // Create a map from row to cluster
                    var rowToCluster = new Dictionary<IDictionary<string, object>, int>();

                    // Initialize clusters
                    foreach (var cluster in clusters)
                    {
                        rowsByCluster[cluster.ClusterId] = new List<int>();
                    }

                    // Assign each row to its cluster
                    for (int i = 0; i < rows.Count; i++)
                    {
                        var row = rows[i];

                        // Find which cluster this row belongs to
                        foreach (var cluster in clusters)
                        {
                            if (cluster.SampleRows.Contains(row))
                            {
                                rowToCluster[row] = cluster.ClusterId;
                                rowsByCluster[cluster.ClusterId].Add(i);
                                break;
                            }
                        }
                    }

                    // Calculate silhouette scores for each cluster
                    foreach (var cluster in clusters)
                    {
                        int clusterId = cluster.ClusterId;

                        if (!rowsByCluster.ContainsKey(clusterId) || rowsByCluster[clusterId].Count == 0)
                        {
                            continue;
                        }

                        double totalSilhouette = 0;
                        int pointsWithScore = 0;

                        // Calculate silhouette for each point in the cluster
                        foreach (var rowIndex in rowsByCluster[clusterId])
                        {
                            // Calculate a (average distance to points in same cluster)
                            double a = 0;
                            int intraCount = 0;

                            foreach (var otherRowIndex in rowsByCluster[clusterId])
                            {
                                if (rowIndex != otherRowIndex)
                                {
                                    a += CalculateDistance(rowFeatures[rowIndex], rowFeatures[otherRowIndex]);
                                    intraCount++;
                                }
                            }

                            a = intraCount > 0 ? a / intraCount : 0;

                            // Calculate b (average distance to points in nearest other cluster)
                            double b = double.MaxValue;

                            foreach (var otherClusterId in rowsByCluster.Keys)
                            {
                                if (otherClusterId != clusterId)
                                {
                                    double avgDistance = 0;
                                    int interCount = 0;

                                    foreach (var otherRowIndex in rowsByCluster[otherClusterId])
                                    {
                                        avgDistance += CalculateDistance(rowFeatures[rowIndex], rowFeatures[otherRowIndex]);
                                        interCount++;
                                    }

                                    avgDistance = interCount > 0 ? avgDistance / interCount : double.MaxValue;
                                    b = Math.Min(b, avgDistance);
                                }
                            }

                            // Calculate silhouette
                            if (a < b && a > 0)
                            {
                                // Well clustered point
                                double silhouette = 1 - (a / b);
                                totalSilhouette += silhouette;
                                pointsWithScore++;
                            }
                            else if (a > b && b > 0)
                            {
                                // Misclassified point
                                double silhouette = (b / a) - 1;
                                totalSilhouette += silhouette;
                                pointsWithScore++;
                            }
                            else
                            {
                                // Either a=0 (single point in cluster) or b=0 (no other clusters)
                                // Silhouette is 0 in these edge cases
                            }
                        }

                        // Set average silhouette score for the cluster
                        cluster.SilhouetteScore = pointsWithScore > 0 ? totalSilhouette / pointsWithScore : 0;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error calculating silhouette scores");
                }
            });
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
    }
}
