using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization
{
    /// <summary>
    /// Context for carrying information through a data flow pipeline
    /// </summary>
    public class DataFlowContext
    {
        /// <summary>
        /// Unique identifier for this flow
        /// </summary>
        public Guid Id { get; } = Guid.NewGuid();

        /// <summary>
        /// Name of the input collection
        /// </summary>
        public string InputCollection { get; }

        /// <summary>
        /// Name of the output collection
        /// </summary>
        public string OutputCollection { get; }

        /// <summary>
        /// Additional properties for the flow
        /// </summary>
        public Dictionary<string, object> Properties { get; } = new Dictionary<string, object>();

        /// <summary>
        /// Statistics about the flow
        /// </summary>
        public FlowStatistics Statistics { get; } = new FlowStatistics();

        /// <summary>
        /// Cancellation token for this flow
        /// </summary>
        public CancellationToken CancellationToken { get; set; } = CancellationToken.None;

        /// <summary>
        /// Logger for this flow
        /// </summary>
        public ILogger Logger { get; set; }

        /// <summary>
        /// Creates a new data flow context
        /// </summary>
        public DataFlowContext(string inputCollection, string outputCollection, ILogger logger = null)
        {
            InputCollection = inputCollection;//?? throw new ArgumentNullException(nameof(inputCollection));
            OutputCollection = outputCollection;// ?? throw new ArgumentNullException(nameof(outputCollection));
            Logger = logger;
        }

        /// <summary>
        /// Gets a property value
        /// </summary>
        public T GetProperty<T>(string key, T defaultValue = default)
        {
            if (Properties.TryGetValue(key, out var value) && value is T typedValue)
            {
                return typedValue;
            }
            return defaultValue;
        }

        /// <summary>
        /// Sets a property value
        /// </summary>
        public void SetProperty<T>(string key, T value)
        {
            Properties[key] = value;
        }

        // Add method to update step info with current statistics
        public void UpdateStepStatistics(JobStatus jobStatus)
        {
            if (jobStatus != null)
            {
                jobStatus.Statistics = this.Statistics;
            }
        }
    }

    /// <summary>
    /// Statistics about a data flow
    /// </summary>
    public class FlowStatistics
    {
        /// <summary>
        /// Total number of records processed
        /// </summary>
        public int RecordsProcessed { get; set; }

        /// <summary>
        /// Total number of records with errors
        /// </summary>
        public int ErrorRecords { get; set; }

        /// <summary>
        /// Total number of batches processed
        /// </summary>
        public int BatchesProcessed { get; set; }

        /// <summary>
        /// Total number of transformations applied
        /// </summary>
        public int TransformationsApplied { get; set; }

        /// <summary>
        /// Time when the flow started
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Time when the flow completed
        /// </summary>
        public DateTime? EndTime { get; private set; }

        /// <summary>
        /// Total duration of the flow
        /// </summary>
        public TimeSpan Duration => (EndTime ?? DateTime.UtcNow) - StartTime;

        // Add new properties for detailed tracking
        public Dictionary<string, int> ErrorCategories { get; set; } = new Dictionary<string, int>();
        public string OperationType { get; set; } // Import,Cleanse

        /// <summary>
        /// Records processed per second
        /// </summary>
        public double RecordsPerSecond => Duration.TotalSeconds > 0
            ? RecordsProcessed / Duration.TotalSeconds
            : 0;

        // Method to categorize an error
        public void RecordError(string category = "General")
        {
            ErrorRecords++;
            if (!ErrorCategories.ContainsKey(category))
            {
                ErrorCategories[category] = 0;
            }
            ErrorCategories[category]++;
        }
        /// <summary>
        /// Marks the flow as complete
        /// </summary>
        public void MarkComplete()
        {
            EndTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Returns a string representation of these statistics
        /// </summary>
        public override string ToString()
        {
            return $"Processed {RecordsProcessed:N0} records in {BatchesProcessed:N0} batches " +
                   $"with {TransformationsApplied:N0} transformations. " +
                   $"Duration: {Duration.TotalSeconds:N2}s ({RecordsPerSecond:N2} records/sec). " +
                   $"Errors: {ErrorRecords:N0}";
        }
    }
}
