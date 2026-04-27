using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling;

public class AdvancedProfilingOptions
{
    // Base profiling options
    public int BatchSize { get; set; } = 1000;
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    public int BufferSize { get; set; } = 10000;
    public int SampleSize { get; set; } = 100;
    public int MaxRowsPerCategory { get; set; } = 50;
    public int MaxDistinctValuesToTrack { get; set; } = 50;
    public bool StoreCompleteRows { get; set; } = true;

    // Feature flags
    public bool EnableAdvancedTypeDetection { get; set; } = true;
    public bool EnableDataQualityAnalysis { get; set; } = true;
    public bool EnableOutlierDetection { get; set; } = false;
    public bool EnableClustering { get; set; } = false;
    public bool EnableCorrelationAnalysis { get; set; } = false;
    public bool EnableSimdOptimizations { get; set; } = true;
    public bool EnableRuleBasedValidation { get; set; } = true;
    public bool EnablePatternDiscovery { get; set; } = true;
    public bool EnableReactivePipeline { get; set; } = true;

    // Advanced configuration
    public double OutlierDetectionThreshold { get; set; } = 3.0; // Number of standard deviations
    public int MaxClusterCount { get; set; } = 5;
    public double MinCorrelationStrength { get; set; } = 0.5;
    public int MaxPatternsToDiscover { get; set; } = 10;
    public int DataQualityScoreThreshold { get; set; } = 80; // 0-100 scale

    // ML.NET configuration
    public bool EnableMlAnalysis { get; set; } = true;
    public int MlAnalysisSampleSize { get; set; } = 10000;
    public double MlAnalysisTrainTestSplit { get; set; } = 0.1;

    // Type detection thresholds
    public double TypeDetectionConfidenceThreshold { get; set; } = 0.8;
    public int MinSampleSizeForTypeInference { get; set; } = 100;

    // Performance tuning
    public int MemoryPoolSize { get; set; } = 50 * 1024 * 1024; // 100 MB
    public bool UseMemoryPool { get; set; } = true;
    public int TimeoutMilliseconds { get; set; } = 30000; // 30 seconds

    // Row reference settings
    public bool StoreOutlierRowReferences { get; set; } = false;
    public int MaxOutlierRowsToStore { get; set; } = 100;

    // Clustering settings
    public bool EvaluateClusterQuality { get; set; } = false;
    public int MaxSampleRowsPerCluster { get; set; } = 10;
    public bool DetectTransitiveRelationships { get; set; } = false;
}