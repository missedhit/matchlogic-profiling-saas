using MatchLogic.Application.Common;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.MatchDefinition.Adapters;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Analytics;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace MatchLogic.Application.Features.DataMatching.Analytics;

/// <summary>
/// Match quality analytics service for MatchGraphDME.
/// Computes graph-based metrics and stores GroupIds by confidence threshold.
/// Designed for single-threaded execution after graph building.
/// </summary>
public class MatchQualityAnalysisDME
{
    private readonly ILogger<MatchQualityAnalysisDME> _logger;    
    private readonly MatchDefinitionAdapter _adapter;
    public MatchQualityAnalysisDME(ILogger<MatchQualityAnalysisDME> logger = null)
    {
        _logger = logger;
        _adapter = new MatchDefinitionAdapter();
    }

    /// <summary>
    /// Generate comprehensive match quality report from MatchGraphDME.
    /// Call this after graph is built. GroupIds are populated separately after grouping.
    /// </summary>
    public MatchQualityReportDME GenerateReport(
        MatchGraphDME graph,
        MatchDefinitionCollection definitions,
        ScoreBandCollection scoreBandCollection,
        Dictionary<Guid, string> dataSourceNames = null)
    {
        if (graph == null)
            throw new ArgumentNullException(nameof(graph));

        var stopwatch = Stopwatch.StartNew();
        _logger?.LogInformation(
            "Starting match quality analysis for graph with {Nodes} nodes, {Edges} edges",
            graph.TotalNodes, graph.TotalEdges);

        var report = new MatchQualityReportDME
        {
            ProjectId = graph.ProjectId,
            GraphId = graph.GraphId,
            AnalyzedAt = DateTime.UtcNow
        };

        // 1. Summary statistics
        report.Summary = CalculateSummary(graph);

        var scoreBand = scoreBandCollection == null ? Constants.bands : scoreBandCollection.ScoreBands;

        // 2. Score distribution with empty GroupId lists (populated after grouping)
        report.ScoreDistribution = CalculateScoreDistribution(graph, scoreBand);

        // 3. Definition performance
        report.DefinitionPerformance = CalculateDefinitionPerformance(graph, definitions);

        // 4. Data source health
        report.DataSourceHealth = CalculateDataSourceHealth(graph, dataSourceNames);

        // 5. Cross-source matches
        report.CrossSourceMatches = CalculateCrossSourceMatches(graph, dataSourceNames);

        // 6. Anomaly detection
        report.Anomalies = DetectAnomalies(graph, report);

        // 7. Generate recommendations
        report.Recommendations = GenerateRecommendations(report);

        stopwatch.Stop();
        report.AnalysisDurationMs = stopwatch.ElapsedMilliseconds;

        _logger?.LogInformation(
            "Match quality analysis completed in {Duration}ms", 
            stopwatch.ElapsedMilliseconds);

        return report;
    }

    /// <summary>
    /// Populate GroupIds by threshold after grouping completes.
    /// Call this with accumulated group data from the grouping step.
    /// </summary>
    public void PopulateGroupIdsByThreshold(
        MatchQualityReportDME report,
        IEnumerable<(int GroupId, double AvgScore)> groupScores)
    {
        if (report?.ScoreDistribution?.Bands == null)
            return;

        // Initialize lists if needed
        foreach (var band in report.ScoreDistribution.Bands)
        {
            band.GroupIds ??= new List<int>();
        }

        foreach (var (groupId, avgScore) in groupScores)
        {
            var band = report.ScoreDistribution.Bands
                .FirstOrDefault(b => avgScore >= b.MinThreshold && avgScore < b.MaxThreshold);

            // Handle edge case for score = 1.0 (goes into Excellent band)
            if (band == null && avgScore >= 0.90)
            {
                band = report.ScoreDistribution.Bands.FirstOrDefault(b => b.Label == "Excellent");
            }

            band?.GroupIds?.Add(groupId);
        }

        // Update group counts
        foreach (var band in report.ScoreDistribution.Bands)
        {
            band.GroupCount = band.GroupIds?.Count ?? 0;
        }

        _logger?.LogInformation(
            "Populated GroupIds: {TotalGroups} groups across {BandCount} threshold bands",
            groupScores.Count(),
            report.ScoreDistribution.Bands.Count);
    }

    #region Summary Calculation

    private MatchQualitySummaryDME CalculateSummary(MatchGraphDME graph)
    {
        var totalRecords = graph.TotalNodes;
        var totalPairs = graph.TotalEdges;

        // Count records with at least one match
        var recordsWithMatches = graph.AdjacencyList
            .Count(kvp => kvp.Value != null && kvp.Value.Count > 0);

        var recordsWithoutMatches = totalRecords - recordsWithMatches;
        var matchRate = totalRecords > 0 ? (double)recordsWithMatches / totalRecords : 0;

        // Score statistics
        double avgScore = 0, medianScore = 0, minScore = 0, maxScore = 0;
        
        if (graph.EdgeDetails.Any())
        {
            var scores = graph.EdgeDetails.Values.Select(e => e.MaxScore).ToList();
            avgScore = scores.Average();
            medianScore = CalculateMedian(scores);
            minScore = scores.Min();
            maxScore = scores.Max();
        }

        return new MatchQualitySummaryDME
        {
            TotalRecords = totalRecords,
            TotalMatchPairs = totalPairs,
            RecordsWithMatches = recordsWithMatches,
            RecordsWithoutMatches = recordsWithoutMatches,
            MatchRate = matchRate,
            AverageScore = avgScore,
            MedianScore = medianScore,
            MinScore = minScore,
            MaxScore = maxScore
        };
    }

    #endregion

    #region Score Distribution

    private ScoreDistributionReportDME CalculateScoreDistribution(MatchGraphDME graph, List<ScoreBandDME> bands)
    {
        // Initialize threshold bands        
        var totalPairs = graph.TotalEdges;

        // Count pairs per band
        foreach (var edge in graph.EdgeDetails.Values)
        {
            var band = bands.FirstOrDefault(b =>
                edge.MaxScore >= b.MinThreshold && edge.MaxScore < b.MaxThreshold);

            // Handle score = 1.0
            if (band == null && edge.MaxScore >= 0.90)
            {
                band = bands.First(b => b.Label == "Excellent");
            }

            if (band != null)
            {
                band.PairCount++;
            }
        }

        // Calculate percentages and initialize GroupIds list
        foreach (var band in bands)
        {
            band.Percentage = totalPairs > 0 
                ? Math.Round((double)band.PairCount / totalPairs * 100, 2) 
                : 0;
            band.GroupIds = new List<int>(); // Populated after grouping
            band.GroupCount = 0;
        }

        return new ScoreDistributionReportDME
        {
            Bands = bands,
            TotalPairs = totalPairs
        };
    }

    #endregion

    #region Definition Performance

    private List<DefinitionPerformanceReportDME> CalculateDefinitionPerformance(
        MatchGraphDME graph,
        MatchDefinitionCollection definitions)
    {
        var definitionsUI = _adapter.ToMappedRowDto(definitions);
        var performanceByDef = new Dictionary<int, DefinitionPerformanceReportDME>();

        // Initialize performance records for each definition
        if (definitions?.Definitions != null)
        {
            for (int i = 0; i < definitionsUI.Definitions.Count; i++)
            {
                var def = definitionsUI.Definitions[i];
                performanceByDef[i] = new DefinitionPerformanceReportDME
                {
                    DefinitionIndex = i,
                    DefinitionName = BuildDefinitionName(def),
                    PairCount = 0,
                    TotalScore = 0,
                    HighConfidenceCount = 0,
                    MinScore = double.MaxValue,
                    MaxScore = double.MinValue
                };
            }
        }

        // Aggregate scores by definition
        foreach (var edge in graph.EdgeDetails.Values)
        {
            if (edge.ScoresByDefinition == null)
                continue;

            foreach (var kvp in edge.ScoresByDefinition)
            {
                var defIndex = kvp.Key;
                var scoreDetail = kvp.Value;

                if (!performanceByDef.TryGetValue(defIndex, out var perf))
                {
                    // Definition not in collection - create entry
                    perf = new DefinitionPerformanceReportDME
                    {
                        DefinitionIndex = defIndex,
                        DefinitionName = $"Definition {defIndex}",
                        MinScore = double.MaxValue,
                        MaxScore = double.MinValue
                    };
                    performanceByDef[defIndex] = perf;
                }

                // Use WeightedScore from MatchScoreDetail
                var score = scoreDetail.WeightedScore;
                perf.PairCount++;
                perf.TotalScore += score;

                if (score >= 0.85)
                    perf.HighConfidenceCount++;

                if (score < perf.MinScore)
                    perf.MinScore = score;

                if (score > perf.MaxScore)
                    perf.MaxScore = score;
            }
        }

        // Finalize calculations
        var results = new List<DefinitionPerformanceReportDME>();
        foreach (var perf in performanceByDef.Values.OrderByDescending(p => p.PairCount))
        {
            if (perf.PairCount > 0)
            {
                perf.AverageScore = perf.TotalScore / perf.PairCount;
                perf.HighConfidencePercentage = (double)perf.HighConfidenceCount / perf.PairCount;
            }
            else
            {
                perf.MinScore = 0;
                perf.MaxScore = 0;
            }

            results.Add(perf);
        }

        return results;
    }

    /// <summary>
    /// Build definition name from first field mapping of each criteria.
    /// Example: "Email + FullName + Phone"
    /// </summary>
    private string BuildDefinitionName(MatchDefinitionMappedRowDto definition)
    {
        if (definition?.Criteria == null || !definition.Criteria.Any())
            return "Unnamed Definition";

        var fieldNames = new List<string>();

        foreach (var criteria in definition.Criteria)
        {
            // Get the first field mapping's field name
            var firstMapping = criteria.MappedRow.FieldsByDataSource.Values?.FirstOrDefault();
            if (firstMapping != null && !string.IsNullOrEmpty(firstMapping.Name))
            {
                fieldNames.Add(firstMapping.Name);
            }
        }

        if (!fieldNames.Any())
            return "Unnamed Definition";

        return string.Join(" + ", fieldNames);
    }

    #endregion

    #region Data Source Health

    private List<DataSourceHealthReportDME> CalculateDataSourceHealth(
        MatchGraphDME graph,
        Dictionary<Guid, string> dataSourceNames)
    {
        var healthBySource = new Dictionary<Guid, DataSourceHealthReportDME>();

        // Group nodes by data source
        foreach (var kvp in graph.NodeMetadata)
        {
            var recordKey = kvp.Key;
            var sourceId = recordKey.DataSourceId;

            if (!healthBySource.TryGetValue(sourceId, out var health))
            {
                health = new DataSourceHealthReportDME
                {
                    DataSourceId = sourceId,
                    DataSourceName = dataSourceNames?.GetValueOrDefault(sourceId) 
                        ?? sourceId.ToString()
                };
                healthBySource[sourceId] = health;
            }

            health.TotalRecords++;

            // Check if record has matches
            if (graph.AdjacencyList.TryGetValue(recordKey, out var neighbors) 
                && neighbors != null && neighbors.Count > 0)
            {
                health.MatchedRecords++;
                health.TotalDegree += neighbors.Count;
            }
        }

        // Finalize calculations
        var results = new List<DataSourceHealthReportDME>();
        foreach (var health in healthBySource.Values.OrderByDescending(h => h.TotalRecords))
        {
            health.MatchRate = health.TotalRecords > 0
                ? (double)health.MatchedRecords / health.TotalRecords
                : 0;

            health.AverageDegree = health.MatchedRecords > 0
                ? (double)health.TotalDegree / health.MatchedRecords
                : 0;

            results.Add(health);
        }

        return results;
    }

    #endregion

    #region Cross-Source Matches

    private List<CrossSourceMatchReportDME> CalculateCrossSourceMatches(
        MatchGraphDME graph,
        Dictionary<Guid, string> dataSourceNames)
    {
        var crossSourceStats = new Dictionary<(Guid, Guid), CrossSourceMatchReportDME>();

        foreach (var kvp in graph.EdgeDetails)
        {
            var edgeKey = kvp.Key;
            var edgeDetails = kvp.Value;

            var source1 = edgeKey.Item1.DataSourceId;
            var source2 = edgeKey.Item2.DataSourceId;

            // Skip same-source pairs
            if (source1 == source2)
                continue;

            // Normalize key ordering for consistent grouping
            var key = source1.CompareTo(source2) <= 0 
                ? (source1, source2) 
                : (source2, source1);

            if (!crossSourceStats.TryGetValue(key, out var stat))
            {
                stat = new CrossSourceMatchReportDME
                {
                    Source1Id = key.Item1,
                    Source2Id = key.Item2,
                    Source1Name = dataSourceNames?.GetValueOrDefault(key.Item1) 
                        ?? key.Item1.ToString(),
                    Source2Name = dataSourceNames?.GetValueOrDefault(key.Item2) 
                        ?? key.Item2.ToString()
                };
                crossSourceStats[key] = stat;
            }

            stat.PairCount++;
            stat.TotalScore += edgeDetails.MaxScore;
        }

        // Finalize
        var results = new List<CrossSourceMatchReportDME>();
        foreach (var stat in crossSourceStats.Values.OrderByDescending(s => s.PairCount))
        {
            stat.AverageScore = stat.PairCount > 0 
                ? stat.TotalScore / stat.PairCount 
                : 0;
            results.Add(stat);
        }

        return results;
    }

    #endregion

    #region Anomaly Detection

    private List<AnomalyReportDME> DetectAnomalies(
        MatchGraphDME graph, 
        MatchQualityReportDME report)
    {
        var anomalies = new List<AnomalyReportDME>();

        // 1. Detect hub nodes (records matching too many others)
        var hubAnomaly = DetectHubNodes(graph);
        if (hubAnomaly != null)
            anomalies.Add(hubAnomaly);

        // 2. Detect low confidence pairs
        var lowConfAnomaly = DetectLowConfidencePairs(report);
        if (lowConfAnomaly != null)
            anomalies.Add(lowConfAnomaly);

        // 3. Detect data source with significantly lower match rate
        var sourceAnomaly = DetectUnbalancedSource(report);
        if (sourceAnomaly != null)
            anomalies.Add(sourceAnomaly);

        // 4. Detect definitions with poor performance
        var defAnomalies = DetectPoorDefinitions(report);
        anomalies.AddRange(defAnomalies);

        return anomalies;
    }

    private AnomalyReportDME DetectHubNodes(MatchGraphDME graph)
    {
        if (!graph.AdjacencyList.Any())
            return null;

        var degrees = graph.AdjacencyList.Values
            .Where(v => v != null)
            .Select(n => n.Count)
            .ToList();

        if (!degrees.Any())
            return null;

        var avgDegree = degrees.Average();
        var stdDev = CalculateStdDev(degrees);

        // Hub threshold: 3 standard deviations above mean, minimum 20 connections
        var hubThreshold = Math.Max(avgDegree + (3 * stdDev), 20);

        var hubNodes = graph.AdjacencyList
            .Where(kvp => kvp.Value != null && kvp.Value.Count > hubThreshold)
            .OrderByDescending(kvp => kvp.Value.Count)
            .Take(10)
            .Select(kvp => new HubNodeDetailDME
            {
                RecordKey = kvp.Key.ToString(),
                Degree = kvp.Value.Count
            })
            .ToList();

        if (!hubNodes.Any())
            return null;

        return new AnomalyReportDME
        {
            Type = AnomalyTypeDME.HubNodes,
            Severity = AnomalySeverityDME.Warning,
            Title = "Records Matching Too Many Others",
            Description = $"{hubNodes.Count} records have more than {hubThreshold:F0} connections each (avg is {avgDegree:F1})",
            Count = hubNodes.Count,
            Threshold = hubThreshold,
            Action = "Review for generic data (e.g., 'John Smith') or data quality issues",
            HubNodeDetails = hubNodes
        };
    }

    private AnomalyReportDME DetectLowConfidencePairs(MatchQualityReportDME report)
    {
        const double lowConfidenceThreshold = 0.60;

        var lowConfBands = report.ScoreDistribution.Bands
            .Where(b => b.MaxThreshold <= lowConfidenceThreshold)
            .ToList();

        var lowConfCount = lowConfBands.Sum(b => b.PairCount);
        var totalPairs = report.Summary.TotalMatchPairs;

        if (lowConfCount == 0 || totalPairs == 0)
            return null;

        var percentage = (double)lowConfCount / totalPairs * 100;

        // Only report if > 5% of pairs are low confidence
        if (percentage < 5)
            return null;

        // Find affected definitions
        var affectedDefs = report.DefinitionPerformance
            .Where(d => d.AverageScore < lowConfidenceThreshold)
            .Select(d => d.DefinitionName)
            .ToList();

        return new AnomalyReportDME
        {
            Type = AnomalyTypeDME.LowConfidencePairs,
            Severity = AnomalySeverityDME.Info,
            Title = "Low Confidence Match Pairs",
            Description = $"{lowConfCount:N0} pairs ({percentage:F1}%) have confidence scores below {lowConfidenceThreshold * 100}%",
            Count = lowConfCount,
            Threshold = lowConfidenceThreshold,
            Action = "Consider raising match threshold or adding criteria to affected definitions",
            AffectedDefinitions = affectedDefs
        };
    }

    private AnomalyReportDME DetectUnbalancedSource(MatchQualityReportDME report)
    {
        if (report.DataSourceHealth == null || report.DataSourceHealth.Count < 2)
            return null;

        var avgMatchRate = report.DataSourceHealth.Average(h => h.MatchRate);
        
        // Find source with significantly lower match rate (20% below average)
        var problemSource = report.DataSourceHealth
            .FirstOrDefault(h => h.MatchRate < avgMatchRate * 0.80);

        if (problemSource == null)
            return null;

        var unmatchedCount = problemSource.TotalRecords - problemSource.MatchedRecords;

        return new AnomalyReportDME
        {
            Type = AnomalyTypeDME.UnbalancedSource,
            Severity = AnomalySeverityDME.Info,
            Title = "Data Source with Lower Match Rate",
            Description = $"{problemSource.DataSourceName} has {problemSource.MatchRate * 100:F1}% match rate vs {avgMatchRate * 100:F1}% average",
            Count = unmatchedCount,
            Action = "Check data quality or field mapping for this source",
            AffectedDataSource = problemSource.DataSourceName
        };
    }

    private List<AnomalyReportDME> DetectPoorDefinitions(MatchQualityReportDME report)
    {
        var anomalies = new List<AnomalyReportDME>();

        if (report.DefinitionPerformance == null)
            return anomalies;

        // Flag definitions with < 60% avg score and > 100 pairs
        foreach (var def in report.DefinitionPerformance
            .Where(d => d.AverageScore < 0.60 && d.PairCount > 100))
        {
            anomalies.Add(new AnomalyReportDME
            {
                Type = AnomalyTypeDME.PoorDefinition,
                Severity = AnomalySeverityDME.Warning,
                Title = $"Definition '{def.DefinitionName}' Has Low Average Score",
                Description = $"Average score is {def.AverageScore * 100:F1}% across {def.PairCount:N0} pairs",
                Count = def.PairCount,
                Threshold = 0.60,
                Action = "Consider adding more criteria or disabling this definition"
            });
        }

        return anomalies;
    }

    #endregion

    #region Recommendations

    private List<RecommendationDME> GenerateRecommendations(MatchQualityReportDME report)
    {
        var recommendations = new List<RecommendationDME>();

        // Success indicators
        if (report.Summary.MatchRate >= 0.70)
        {
            recommendations.Add(new RecommendationDME
            {
                Type = RecommendationTypeDME.Success,
                Message = $"{report.Summary.MatchRate * 100:F1}% of records found matches - good coverage for this data volume"
            });
        }

        if (report.Summary.AverageScore >= 0.80)
        {
            recommendations.Add(new RecommendationDME
            {
                Type = RecommendationTypeDME.Success,
                Message = $"{report.Summary.AverageScore * 100:F1}% average match score indicates strong match quality"
            });
        }

        // Action items from anomalies
        var hubAnomaly = report.Anomalies?.FirstOrDefault(a => a.Type == AnomalyTypeDME.HubNodes);
        if (hubAnomaly != null)
        {
            recommendations.Add(new RecommendationDME
            {
                Type = RecommendationTypeDME.Action,
                Message = $"Review {hubAnomaly.Count} hub records before finalizing - may indicate over-matching"
            });
        }

        // Tips for poor definitions
        var poorDefs = report.DefinitionPerformance?
            .Where(d => d.AverageScore < 0.60)
            .ToList();

        if (poorDefs?.Any() == true)
        {
            var worstDef = poorDefs.OrderBy(d => d.AverageScore).First();
            recommendations.Add(new RecommendationDME
            {
                Type = RecommendationTypeDME.Tip,
                Message = $"'{worstDef.DefinitionName}' definition has {worstDef.AverageScore * 100:F0}% avg score - consider adding criteria"
            });
        }

        return recommendations;
    }

    #endregion

    #region Helper Methods

    private double CalculateMedian(List<double> values)
    {
        if (values == null || !values.Any())
            return 0;

        var sorted = values.OrderBy(v => v).ToList();
        var count = sorted.Count;

        if (count % 2 == 0)
            return (sorted[count / 2 - 1] + sorted[count / 2]) / 2.0;

        return sorted[count / 2];
    }

    private double CalculateStdDev(List<int> values)
    {
        if (values == null || values.Count < 2)
            return 0;

        var avg = values.Average();
        var sumSquares = values.Sum(v => Math.Pow(v - avg, 2));
        return Math.Sqrt(sumSquares / (values.Count - 1));
    }

    #endregion
}

#region Group Statistics Accumulator

/// <summary>
/// Lightweight accumulator for tracking group statistics during streaming.
/// Uses simple counters - no locks needed since grouping writes are single-threaded.
/// </summary>
public class GroupStatisticsAccumulator
{
    private readonly List<(int GroupId, double AvgScore)> _groupScores = new();
    private int _groupCount;
    private int _totalRecordsInGroups;

    /// <summary>
    /// Accumulate statistics for a single group.
    /// Call this for each group during the WriteGroups step.
    /// </summary>
    public void AccumulateGroup(int groupId, double avgScore, int recordCount)
    {
        _groupScores.Add((groupId, avgScore));
        _groupCount++;
        _totalRecordsInGroups += recordCount;
    }

    public int GroupCount => _groupCount;
    public int TotalRecordsInGroups => _totalRecordsInGroups;
    public double AverageGroupSize => _groupCount > 0 
        ? (double)_totalRecordsInGroups / _groupCount 
        : 0;

    public IEnumerable<(int GroupId, double AvgScore)> GetGroupScores() => _groupScores;
}

#endregion
