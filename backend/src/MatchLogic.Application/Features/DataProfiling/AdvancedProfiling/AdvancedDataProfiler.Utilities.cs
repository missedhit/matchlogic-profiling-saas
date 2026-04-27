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
        /// Get a list of enabled features based on options
        /// </summary>
        private string GetEnabledFeatures(AdvancedProfilingOptions options)
        {
            var features = new List<string>();

            if (options.EnableAdvancedTypeDetection) features.Add("Advanced Type Detection");
            if (options.EnableDataQualityAnalysis) features.Add("Data Quality Analysis");
            if (options.EnableOutlierDetection) features.Add("Outlier Detection");
            if (options.EnableClustering) features.Add("Clustering");
            if (options.EnableCorrelationAnalysis) features.Add("Correlation Analysis");
            if (options.EnableSimdOptimizations) features.Add("SIMD Optimizations");
            if (options.EnableRuleBasedValidation) features.Add("Rule-Based Validation");
            if (options.EnablePatternDiscovery) features.Add("Pattern Discovery");
            if (options.EnableReactivePipeline) features.Add("Reactive Pipeline");
            if (options.EnableMlAnalysis) features.Add("ML Analysis");

            return string.Join(", ", features);
        }

        /// <summary>
        /// Clean up resources
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;
            _semaphore.Dispose();

            await Task.CompletedTask;
        }
    }
}
