using System;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Configuration for the master record determination pipeline
/// </summary>
public class MasterRecordDeterminationConfig
{
    /// <summary>
    /// Enable data source filtering for rules.
    /// When true, rules will only consider records from selected data sources.
    /// When false, all records are considered regardless of data source selection.
    /// </summary>
    public bool UseDataSourceFiltering { get; set; } = true;
    /// <summary>
    /// Number of groups to process in each batch
    /// </summary>
    public int BatchSize { get; set; } = 500;

    /// <summary>
    /// Maximum number of batches to process concurrently
    /// </summary>
    public int MaxConcurrentBatches { get; set; } = 4;

    /// <summary>
    /// Capacity of the bounded channel for group batches
    /// </summary>
    public int ChannelCapacity { get; set; } = 1000;

    /// <summary>
    /// Maximum concurrent database operations (semaphore limit)
    /// </summary>
    public int MaxDatabaseConcurrency { get; set; } = 2;

    /// <summary>
    /// Interval for progress reporting
    /// </summary>
    public TimeSpan ProgressReportingInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Enable detailed logging of rule applications
    /// </summary>
    public bool EnableDetailedLogging { get; set; } = false;

    /// <summary>
    /// Enable audit trail for master record changes
    /// </summary>
    public bool EnableAuditTrail { get; set; } = true;

    /// <summary>
    /// Timeout for processing a single group (to prevent hangs)
    /// </summary>
    public TimeSpan GroupProcessingTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Returns the default configuration
    /// </summary>
    public static MasterRecordDeterminationConfig Default()
    {
        return new MasterRecordDeterminationConfig
        {
            UseDataSourceFiltering = true  // Enabled by default
        };
    }

    /// <summary>
    /// Returns a configuration optimized for high throughput
    /// </summary>
    public static MasterRecordDeterminationConfig HighThroughput()
    {
        return new MasterRecordDeterminationConfig
        {
            BatchSize = 1000,
            MaxConcurrentBatches = 8,
            ChannelCapacity = 2000,
            MaxDatabaseConcurrency = 4,
            EnableDetailedLogging = false,
            UseDataSourceFiltering = true
        };
    }

    /// <summary>
    /// Returns a configuration optimized for debugging
    /// </summary>
    public static MasterRecordDeterminationConfig Debug()
    {
        return new MasterRecordDeterminationConfig
        {
            BatchSize = 100,
            MaxConcurrentBatches = 1,
            ChannelCapacity = 100,
            MaxDatabaseConcurrency = 1,
            EnableDetailedLogging = true,
            ProgressReportingInterval = TimeSpan.FromSeconds(1),
            UseDataSourceFiltering = true
        };
    }

    /// <summary>
    /// Validates the configuration
    /// </summary>
    public bool IsValid(out string error)
    {
        if (BatchSize <= 0)
        {
            error = "BatchSize must be positive";
            return false;
        }

        if (MaxConcurrentBatches <= 0)
        {
            error = "MaxConcurrentBatches must be positive";
            return false;
        }

        if (ChannelCapacity <= 0)
        {
            error = "ChannelCapacity must be positive";
            return false;
        }

        if (MaxDatabaseConcurrency <= 0)
        {
            error = "MaxDatabaseConcurrency must be positive";
            return false;
        }

        error = null;
        return true;
    }
}
