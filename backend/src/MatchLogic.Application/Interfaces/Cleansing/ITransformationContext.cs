using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing
{
    /// <summary>
    /// Interface for transformation context that orchestrates rule application
    /// </summary>
    public interface ITransformationContext
    {
        /// <summary>
        /// Initializes the context with rules from a configuration
        /// </summary>
        Task InitializeAsync(EnhancedCleaningRules configuration);

        /// <summary>
        /// Transforms a single record
        /// </summary>
        Record TransformRecord(Record record);

        /// <summary>
        /// Transforms a batch of records
        /// </summary>
        RecordBatch TransformBatch(RecordBatch batch);

        /// <summary>
        /// Gets statistics about the transformation process
        /// </summary>
        TransformationStatistics GetStatistics();

        /// <summary>
        /// Resets the context
        /// </summary>
        void Reset();
    }

    /// <summary>
    /// Statistics about the transformation process
    /// </summary>
    public class TransformationStatistics
    {
        /// <summary>
        /// Total number of records processed
        /// </summary>
        public int RecordsProcessed { get; set; }

        /// <summary>
        /// Total number of rules applied
        /// </summary>
        public int RulesApplied { get; set; }

        /// <summary>
        /// Total time spent processing records
        /// </summary>
        public TimeSpan ProcessingTime { get; set; }

        /// <summary>
        /// Records processed per second
        /// </summary>
        public double RecordsPerSecond => ProcessingTime.TotalSeconds > 0 ?
            RecordsProcessed / ProcessingTime.TotalSeconds : 0;

        /// <summary>
        /// Rules applied per second
        /// </summary>
        public double RulesPerSecond => ProcessingTime.TotalSeconds > 0 ?
            RulesApplied / ProcessingTime.TotalSeconds : 0;

        /// <summary>
        /// Creates a new statistics object
        /// </summary>
        public TransformationStatistics()
        {
        }

        /// <summary>
        /// Returns a string representation of these statistics
        /// </summary>
        public override string ToString()
        {
            return $"Processed {RecordsProcessed:N0} records with {RulesApplied:N0} rule applications " +
                   $"in {ProcessingTime.TotalSeconds:F2}s ({RecordsPerSecond:F1} records/sec)";
        }
    }

}
