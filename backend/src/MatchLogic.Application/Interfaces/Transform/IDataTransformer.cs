using System;
using System.Collections.Generic;
using System.Threading;

namespace MatchLogic.Application.Interfaces.Transform;

/// <summary>
/// Strategy for transforming/flattening data.
/// Stateless and thread-safe.
/// PERFORMANCE: Called per-row in tight loops - no allocation, no reflection.
/// </summary>
public interface IDataTransformer : IDisposable
{
    /// <summary>
    /// Apply transformation via streaming.
    /// Does NOT materialize entire result set.
    /// </summary>
    /// <remarks>
    /// Contract:
    /// - Yielded rows may have different schema from input (flatten/aggregate/rename)
    /// - Each row is independent (no cross-row buffering)
    /// - Cancellation propagated immediately
    /// - May yield 0..N rows per input row (filter, flatten, aggregate)
    /// </remarks>
    IAsyncEnumerable<IDictionary<string, object>> TransformAsync(
        IAsyncEnumerable<IDictionary<string, object>> sourceRows,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Declarative name for logging/telemetry.
    /// </summary>
    string Name { get; }
}

/// <summary>
/// Configuration for data transformation.
/// Encapsulates both transformation logic settings and column projection.
/// Immutable, thread-safe.
/// </summary>
public class TransformerConfiguration
{
    /// <summary>
    /// Transformer type key used to retrieve implementation from factory.
    /// Examples: "flatten", "projection", "aggregation", "none"
    /// </summary>
    public string TransformerType { get; init; } = "none";

    /// <summary>
    /// Transformer-specific settings (transformation logic configuration).
    /// 
    /// Examples:
    /// - Flatten: { "separator": " ", "depth": 2 }
    /// - Projection: { "columns": ["id", "name", "email"] }
    /// - Aggregation: { "groupBy": ["category"], "aggregate": { "amount": "sum" } }
    /// </summary>
    public Dictionary<string, object>? Settings { get; init; }

    /// <summary>
    /// Column name mappings applied AFTER transformation.
    /// Maps transformed column name → display/output name.
    /// 
    /// Examples:
    /// - Simple rename: { "groupId": "Group ID", "dataSourceName": "Data Source Name" }
    /// - After flatten: { "user name": "User Name", "user age": "User Age" }
    /// - After projection: { "email": "Email Address", "phone": "Phone Number" }
    /// 
    /// If null or empty, columns retain their transformed names.
    /// Applied by BaseDataTransformer.ApplyColumnProjection() after core transformation.
    /// </summary>
    public Dictionary<string, string>? ColumnProjections { get; init; }

    /// <summary>
    /// Enable transformation tracing for debugging.
    /// Minimal perf impact when disabled.
    /// </summary>
    public bool EnableTracing { get; init; }

    /// <summary>
    /// Validate configuration is valid for the transformer type.
    /// Called by factory before instantiation.
    /// </summary>
    public virtual void Validate()
    {
        if (string.IsNullOrWhiteSpace(TransformerType))
        {
            throw new ArgumentException("TransformerType cannot be null or empty");
        }
    }
}