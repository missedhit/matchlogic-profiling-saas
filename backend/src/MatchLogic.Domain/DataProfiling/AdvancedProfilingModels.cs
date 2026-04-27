using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.DataProfiling
{
    public class AdvancedProfileResult : ProfileResult
    {
        private ConcurrentDictionary<string, AdvancedColumnProfile> _advancedColumnProfiles = new();

        // Standard column profiles
        // Override base property for polymorphic access
        public override ConcurrentDictionary<string, ColumnProfile> ColumnProfiles
        {
            get => new ConcurrentDictionary<string, ColumnProfile>(
                _advancedColumnProfiles.Select(kvp =>
                    new KeyValuePair<string, ColumnProfile>(kvp.Key, kvp.Value)));
            set
            {
                _advancedColumnProfiles = new ConcurrentDictionary<string, AdvancedColumnProfile>(
                    value.Select(kvp =>
                        new KeyValuePair<string, AdvancedColumnProfile>(
                            kvp.Key,
                            kvp.Value as AdvancedColumnProfile ??
                            throw new InvalidCastException($"Expected AdvancedColumnProfile but got {kvp.Value?.GetType().Name}"))));
            }
        }

        public ConcurrentDictionary<string, AdvancedColumnProfile> AdvancedColumnProfiles
        {
            get => _advancedColumnProfiles;
            set => _advancedColumnProfiles = value;
        }

        // Cross-column analytics
        public CorrelationMatrix CorrelationMatrix { get; set; }
        public List<ColumnRelationship> ColumnRelationships { get; set; } = new();
        public DatasetQualityScore DatasetQuality { get; set; }
        public List<string> Warnings { get; set; } = new();
        public List<string> Recommendations { get; set; } = new();

        // Detected keys and relationships
        public List<CandidateKey> CandidateKeys { get; set; } = new();
        public List<FunctionalDependency> FunctionalDependencies { get; set; } = new();       
    }

    /// <summary>
    /// Enhanced column profile with advanced analytics
    /// </summary>
    public class AdvancedColumnProfile : ColumnProfile
    {
        // Advanced type detection
        public string InferredDataType { get; set; }
        public double TypeDetectionConfidence { get; set; }
        public List<TypeDetectionResult> TypeDetectionResults { get; set; } = new();

        // Statistical properties
        public HistogramData Histogram { get; set; }
        public List<Outlier> Outliers { get; set; } = new();
        public List<ClusterInfo> Clusters { get; set; } = new();
        public double Skewness { get; set; }
        public double Kurtosis { get; set; }
        public string InterquartileRange { get; set; }

        // Data quality metrics
        public DataQualityScore QualityScore { get; set; }
        public List<DiscoveredPattern> DiscoveredPatterns { get; set; } = new();
        public List<ValidationRule> AppliedRules { get; set; } = new();
        public List<ValidationViolation> Violations { get; set; } = new();

        // Format detection
        public FormatInfo DetectedFormat { get; set; }

        // Semantic type detection
        public List<SemanticType> PossibleSemanticTypes { get; set; } = new();

        public List<string> Warnings { get; set; } = new();
    }

    /// <summary>
    /// Type detection result with confidence
    /// </summary>
    public class TypeDetectionResult
    {
        public string DataType { get; set; }
        public double Confidence { get; set; }
    }

    /// <summary>
    /// Histogram data for visualization
    /// </summary>
    public class HistogramData
    {
        public List<double> Bins { get; set; } = new();
        public List<int> Frequencies { get; set; } = new();
        public double BinWidth { get; set; }
    }

    /// <summary>
    /// Information about detected outliers
    /// </summary>
    public class Outlier
    {
        public string Value { get; set; }
        public double ZScore { get; set; }
        public RowReference RowReference { get; set; }
    }

    /// <summary>
    /// Information about a cluster of similar values
    /// </summary>
    public class ClusterInfo
    {
        public int ClusterId { get; set; }
        public int Count { get; set; }
        public double Centroid { get; set; }
        public string Representative { get; set; }
        public List<string> SampleValues { get; set; } = new();
        public List<RowReference> SampleRows { get; set; } = new();
    }

    /// <summary>
    /// Data quality score metrics
    /// </summary>
    public class DataQualityScore
    {
        public int OverallScore { get; set; } // 0-100
        public int Completeness { get; set; } // 0-100
        public int Accuracy { get; set; } // 0-100
        public int Consistency { get; set; } // 0-100
        public int Uniqueness { get; set; } // 0-100
        public int Validity { get; set; } // 0-100
    }

    /// <summary>
    /// Dataset-level quality score
    /// </summary>
    public class DatasetQualityScore
    {
        public int OverallScore { get; set; } // 0-100
        public Dictionary<string, int> ColumnScores { get; set; } = new();
        public List<string> QualityIssues { get; set; } = new();
    }

    /// <summary>
    /// Discovered data pattern
    /// </summary>
    public class DiscoveredPattern
    {
        public string Pattern { get; set; }
        public int Count { get; set; }
        public double Coverage { get; set; } // percentage
        public List<RowReference> Examples { get; set; } = new();
    }

    /// <summary>
    /// Rule applied during validation
    /// </summary>
    public class ValidationRule
    {
        public string RuleName { get; set; }
        public string Description { get; set; }
        public int PassCount { get; set; }
        public int FailCount { get; set; }
        public Dictionary<string, string> Parameters { get; set; }
    }

    /// <summary>
    /// Validation rule violation
    /// </summary>
    public class ValidationViolation
    {
        public string RuleName { get; set; }
        public string Value { get; set; }
        public string Message { get; set; }
        public List<RowReference> Examples { get; set; } = new();
    }

    /// <summary>
    /// Format information for the column
    /// </summary>
    public class FormatInfo
    {
        public string Format { get; set; }
        public double Confidence { get; set; }
        public List<string> Examples { get; set; } = new();
    }

    /// <summary>
    /// Semantic type information
    /// </summary>
    public class SemanticType
    {
        public string Type { get; set; } // e.g., "PhoneNumber", "EmailAddress", "PersonName"
        public double Confidence { get; set; }
    }

    /// <summary>
    /// Correlation matrix between columns
    /// </summary>
    public class CorrelationMatrix
    {
        public List<string> Columns { get; set; } = new();
        public double[,] Values { get; set; }
    }

    /// <summary>
    /// Relationship between columns
    /// </summary>
    public class ColumnRelationship
    {
        public string SourceColumn { get; set; }
        public string TargetColumn { get; set; }
        public string RelationshipType { get; set; } // e.g., "OneToOne", "OneToMany"
        public double Strength { get; set; }
    }

    /// <summary>
    /// Candidate key information
    /// </summary>
    public class CandidateKey
    {
        public List<string> Columns { get; set; } = new();
        public double Uniqueness { get; set; } // 0-1
        public int NonNullCount { get; set; }
    }

    /// <summary>
    /// Functional dependency between columns
    /// </summary>
    public class FunctionalDependency
    {
        public List<string> DeterminantColumns { get; set; } = new();
        public List<string> DependentColumns { get; set; } = new();
        public double Confidence { get; set; }
        public string DependencyType { get; set; }
    }

    /// <summary>
    /// Row-level cluster information
    /// </summary>
    public class RowCluster
    {
        public int ClusterId { get; set; }
        public int Count { get; set; }
        public double SilhouetteScore { get; set; }
        public List<IDictionary<string, object>> SampleRows { get; set; } = new();
        public Dictionary<string, double> DistinctiveFeatures { get; set; } = new();
    }

    /// <summary>
    /// Row-level outlier information
    /// </summary>
    public class RowOutlier
    {
        public IDictionary<string, object> RowData { get; set; }
        public double AnomalyScore { get; set; }
        public int ContributingFields { get; set; }
        public Dictionary<string, double> FieldScores { get; set; } = new();
    }
}
