using System;
using System.Collections.Generic;

namespace MatchLogic.Infrastructure.Persistence.MongoDB;

/// <summary>
/// Configuration options for MongoDB data store
/// Designed for high-throughput scenarios (1M+ records, 6.5M+ pairs)
/// </summary>
public class MongoDbOptions
{
    public const string SectionName = "MongoDB";

    /// <summary>
    /// MongoDB connection string
    /// Example: "mongodb://localhost:27017" or "mongodb+srv://user:pass@cluster.mongodb.net"
    /// </summary>
    public string ConnectionString { get; set; } = "mongodb://localhost:27017";

    /// <summary>
    /// Database name. For SAAS, this could be tenant-specific
    /// </summary>
    public string DatabaseName { get; set; } = "MatchLogic";

    /// <summary>
    /// Optional prefix for all collection names (useful for multi-tenancy)
    /// Example: "tenant1_" would result in collections like "tenant1_JobStatus"
    /// </summary>
    public string CollectionPrefix { get; set; } = "";

    /// <summary>
    /// Maximum connection pool size. Recommended: 500 for high-throughput scenarios
    /// </summary>
    public int MaxConnectionPoolSize { get; set; } = 500;

    /// <summary>
    /// Minimum connection pool size. Keeps connections warm
    /// </summary>
    public int MinConnectionPoolSize { get; set; } = 25;

    /// <summary>
    /// Connection timeout in seconds
    /// </summary>
    public int ConnectTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Socket timeout in seconds for operations
    /// </summary>
    public int SocketTimeoutSeconds { get; set; } = 300;

    /// <summary>
    /// Server selection timeout in seconds
    /// </summary>
    public int ServerSelectionTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Maximum time to wait for a connection from the pool
    /// </summary>
    public int WaitQueueTimeoutSeconds { get; set; } = 120;

    /// <summary>
    /// Batch size for bulk insert operations
    /// 25,000 is optimal for most scenarios with 1KB average document size
    /// </summary>
    public int BulkInsertBatchSize { get; set; } = 25000;

    /// <summary>
    /// Write concern level
    /// </summary>
    public WriteConcernLevel WriteConcern { get; set; } = WriteConcernLevel.Acknowledged;

    /// <summary>
    /// Whether to use unordered bulk writes (faster but no guaranteed order)
    /// Recommended: true for pair generation scenarios
    /// </summary>
    public bool UseUnorderedBulkWrites { get; set; } = true;

    /// <summary>
    /// Enable command logging for debugging
    /// </summary>
    public bool EnableCommandLogging { get; set; } = false;

    /// <summary>
    /// Retry writes on transient failures
    /// </summary>
    public bool RetryWrites { get; set; } = true;

    /// <summary>
    /// Maximum retry attempts for failed operations
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    /// Delay between retry attempts in milliseconds
    /// </summary>
    public int RetryDelayMilliseconds { get; set; } = 100;

    /// <summary>
    /// Whether to create indexes automatically on first use
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Read preference for queries
    /// </summary>
    public ReadPreferenceLevel ReadPreference { get; set; } = ReadPreferenceLevel.Primary;
}

/// <summary>
/// MongoDB Write Concern levels
/// </summary>
public enum WriteConcernLevel
{
    /// <summary>
    /// No acknowledgment (fastest, least safe)
    /// </summary>
    Unacknowledged = 0,

    /// <summary>
    /// Acknowledged by primary (good balance)
    /// </summary>
    Acknowledged = 1,

    /// <summary>
    /// Acknowledged by majority of replica set (safest for production)
    /// </summary>
    Majority = 2,

    /// <summary>
    /// Written to journal on primary
    /// </summary>
    Journaled = 3
}

/// <summary>
/// MongoDB Read Preference levels
/// </summary>
public enum ReadPreferenceLevel
{
    /// <summary>
    /// Always read from primary
    /// </summary>
    Primary = 0,

    /// <summary>
    /// Prefer primary, fallback to secondary
    /// </summary>
    PrimaryPreferred = 1,

    /// <summary>
    /// Always read from secondary
    /// </summary>
    Secondary = 2,

    /// <summary>
    /// Prefer secondary, fallback to primary
    /// </summary>
    SecondaryPreferred = 3,

    /// <summary>
    /// Read from nearest member
    /// </summary>
    Nearest = 4
}

/// <summary>
/// Options for a separate progress/job status database
/// </summary>
public class MongoDbProgressOptions : MongoDbOptions
{
    public new const string SectionName = "MongoDB:Progress";

    public MongoDbProgressOptions()
    {
        DatabaseName = "MatchLogic_Progress";
        // Progress DB typically has lower throughput requirements
        MaxConnectionPoolSize = 100;
        MinConnectionPoolSize = 10;
        BulkInsertBatchSize = 1000;
    }
}

/// <summary>
/// Multi-tenancy configuration for SAAS deployment
/// </summary>
public class MongoDbTenantOptions
{
    /// <summary>
    /// Tenant isolation strategy
    /// </summary>
    public TenantIsolationStrategy IsolationStrategy { get; set; } = TenantIsolationStrategy.CollectionPrefix;

    /// <summary>
    /// Current tenant identifier (set per-request in SAAS scenario)
    /// </summary>
    public string TenantId { get; set; } = "";

    /// <summary>
    /// Whether to include tenant ID in all queries automatically
    /// </summary>
    public bool EnforceTenantIsolation { get; set; } = false;
}

/// <summary>
/// How to isolate tenant data
/// </summary>
public enum TenantIsolationStrategy
{
    /// <summary>
    /// Prefix collection names with tenant ID (e.g., "tenant1_profiles")
    /// </summary>
    CollectionPrefix = 0,

    /// <summary>
    /// Separate database per tenant (e.g., "MatchLogic_tenant1")
    /// </summary>
    DatabasePerTenant = 1,

    /// <summary>
    /// Shared collections with TenantId field in each document
    /// </summary>
    SharedWithDiscriminator = 2
}