using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Analytics;
#region Report Models

/// <summary>
/// Complete match quality report for DME
/// </summary>
public class MatchQualityReportDME
{
    public Guid ProjectId { get; set; }
    public Guid GraphId { get; set; }
    public DateTime AnalyzedAt { get; set; }
    public long AnalysisDurationMs { get; set; }

    public MatchQualitySummaryDME Summary { get; set; }
    public ScoreDistributionReportDME ScoreDistribution { get; set; }
    public List<DefinitionPerformanceReportDME> DefinitionPerformance { get; set; }
    public List<DataSourceHealthReportDME> DataSourceHealth { get; set; }
    public List<CrossSourceMatchReportDME> CrossSourceMatches { get; set; }
    public List<AnomalyReportDME> Anomalies { get; set; }
    public List<RecommendationDME> Recommendations { get; set; }

    /// <summary>
    /// Convert to dictionary for MongoDB storage
    /// </summary>
    public IDictionary<string, object> ToDictionary()
    {
        return new Dictionary<string, object>
        {
            ["_id"] = $"analytics_{ProjectId}_{AnalyzedAt:yyyyMMddHHmmss}",
            ["ProjectId"] = ProjectId,
            ["GraphId"] = GraphId,
            ["AnalyzedAt"] = AnalyzedAt,
            ["AnalysisDurationMs"] = AnalysisDurationMs,
            ["Summary"] = Summary,
            ["ScoreDistribution"] = ScoreDistribution,
            ["DefinitionPerformance"] = DefinitionPerformance,
            ["DataSourceHealth"] = DataSourceHealth,
            ["CrossSourceMatches"] = CrossSourceMatches,
            ["Anomalies"] = Anomalies,
            ["Recommendations"] = Recommendations
        };
    }
}

public class MatchQualitySummaryDME
{
    public int TotalRecords { get; set; }
    public int TotalMatchPairs { get; set; }
    public int RecordsWithMatches { get; set; }
    public int RecordsWithoutMatches { get; set; }
    public double MatchRate { get; set; }
    public double AverageScore { get; set; }
    public double MedianScore { get; set; }
    public double MinScore { get; set; }
    public double MaxScore { get; set; }
}

public class ScoreDistributionReportDME
{
    public List<ScoreBandDME> Bands { get; set; }
    public int TotalPairs { get; set; }
}

public class ScoreBandDME
{
    public string Label { get; set; }
    public double MinThreshold { get; set; }
    public double MaxThreshold { get; set; }
    public int PairCount { get; set; }
    public double Percentage { get; set; }

    /// <summary>
    /// GroupIds that fall within this threshold band.
    /// Populated after grouping completes.
    /// </summary>
    public List<int> GroupIds { get; set; }

    /// <summary>
    /// Count of groups in this band.
    /// </summary>
    public int GroupCount { get; set; }
}

public class ScoreBandCollection: IEntity
{
    public List<ScoreBandDME> ScoreBands { get; set; }
    public Guid ProjectId { get; set; }
}
public class DefinitionPerformanceReportDME
{
    public int DefinitionIndex { get; set; }
    public string DefinitionName { get; set; }
    public int PairCount { get; set; }
    public double TotalScore { get; set; }
    public double AverageScore { get; set; }
    public int HighConfidenceCount { get; set; }
    public double HighConfidencePercentage { get; set; }
    public double MinScore { get; set; }
    public double MaxScore { get; set; }
}

public class DataSourceHealthReportDME
{
    public Guid DataSourceId { get; set; }
    public string DataSourceName { get; set; }
    public int TotalRecords { get; set; }
    public int MatchedRecords { get; set; }
    public double MatchRate { get; set; }
    public int TotalDegree { get; set; }
    public double AverageDegree { get; set; }
}

public class CrossSourceMatchReportDME
{
    public Guid Source1Id { get; set; }
    public Guid Source2Id { get; set; }
    public string Source1Name { get; set; }
    public string Source2Name { get; set; }
    public int PairCount { get; set; }
    public double TotalScore { get; set; }
    public double AverageScore { get; set; }
}

public class AnomalyReportDME
{
    public AnomalyTypeDME Type { get; set; }
    public AnomalySeverityDME Severity { get; set; }
    public string Title { get; set; }
    public string Description { get; set; }
    public int Count { get; set; }
    public double Threshold { get; set; }
    public string Action { get; set; }
    public List<HubNodeDetailDME> HubNodeDetails { get; set; }
    public List<string> AffectedDefinitions { get; set; }
    public string AffectedDataSource { get; set; }
}

public class HubNodeDetailDME
{
    public string RecordKey { get; set; }
    public int Degree { get; set; }
}

public class RecommendationDME
{
    public RecommendationTypeDME Type { get; set; }
    public string Message { get; set; }
}

public enum AnomalyTypeDME
{
    HubNodes,
    LowConfidencePairs,
    UnbalancedSource,
    PoorDefinition
}

public enum AnomalySeverityDME
{
    Info,
    Warning,
    Critical
}

public enum RecommendationTypeDME
{
    Success,
    Action,
    Tip
}

#endregion