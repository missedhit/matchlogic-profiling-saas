using MatchLogic.Application.Interfaces.Transform;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Transform;

/// <summary>
/// Base class for all data transformers.
/// Encapsulates transformation configuration, column projection, and lifecycle.
/// Subclasses implement TransformRowInternalAsync() for core transformation logic.
/// 
/// Handles:
/// - Configuration management (settings, column projections)
/// - Streaming orchestration (IAsyncEnumerable)
/// - Column projection (renaming/filtering)
/// - Logging and tracing
/// - Error handling and cancellation
/// 
/// PERFORMANCE: Base class is zero-cost abstraction when tracing is disabled.
/// </summary>
internal abstract class BaseDataTransformer : IDataTransformer
{
    /// <summary>
    /// Transformer configuration (settings + column projections).
    /// </summary>
    protected readonly TransformerConfiguration Configuration;

    /// <summary>
    /// Optional logger for debugging/telemetry.
    /// </summary>
    protected readonly ILogger? Logger;

    /// <summary>
    /// Cache of column projections (original → display name).
    /// Initialized once, reused for all rows.
    /// </summary>
    private readonly Dictionary<string, string>? _columnProjectionMap;

    /// <summary>
    /// HashSet for fast lookup of projected columns.
    /// If null, all columns are included (no filtering).
    /// </summary>
    private readonly HashSet<string>? _projectedColumnSet;

    protected BaseDataTransformer(TransformerConfiguration configuration, ILogger? logger = null)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        Logger = logger;

        // Initialize column projection mappings
        (_columnProjectionMap, _projectedColumnSet) = InitializeColumnProjections(
            configuration.ColumnProjections);
    }

    public abstract string Name { get; }

    /// <summary>
    /// Core transformation logic: implemented by subclass.
    /// Receives raw row, returns transformed rows.
    /// Should NOT apply column projections; base class handles that.
    /// </summary>
    protected abstract Task<IEnumerable<IDictionary<string, object>>> TransformRowInternalAsync(
        IDictionary<string, object> row,
        CancellationToken cancellationToken);

    /// <summary>
    /// Main entry point: transform rows with optional column projection.
    /// Handles streaming, cancellation, and error propagation.
    /// PERFORMANCE: Streaming-based, no buffering.
    /// </summary>
    public virtual async IAsyncEnumerable<IDictionary<string, object>> TransformAsync(
        IAsyncEnumerable<IDictionary<string, object>> sourceRows,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var inputRowCount = 0;
        var outputRowCount = 0;

        await foreach (var row in sourceRows.WithCancellation(cancellationToken))
        {
            inputRowCount++;
            cancellationToken.ThrowIfCancellationRequested();

            IEnumerable<IDictionary<string, object>> transformedRows;
            try
            {
                // Remove _id and _metadata if present
                row.Remove("_id");
                row.Remove("_metadata");

                // Core transformation: try multi-row first, fallback to single-row
                transformedRows = await TransformRowInternalAsync(row, cancellationToken);               
                // Apply column projection (renaming) to each transformed row if configured
                if (_columnProjectionMap != null)
                {
                    transformedRows = transformedRows.Select(transformedRow =>
                        ApplyColumnProjection(transformedRow, _columnProjectionMap, _projectedColumnSet)).ToList();
                }

                outputRowCount += transformedRows.Count();
            }
            catch (OperationCanceledException)
            {
                Logger?.LogWarning("{TransformerName}: Transformation cancelled after {InputRows} input rows, {OutputRows} output rows",
                    Name, inputRowCount, outputRowCount);
                throw;
            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "{TransformerName}: Error transforming row {RowCount}",
                    Name, inputRowCount);
                throw;
            }

            // Yield each transformed row
            foreach (var transformedRow in transformedRows)
            {
                TraceRow(outputRowCount, transformedRow);
                yield return transformedRow;
            }
        }

        Logger?.LogInformation("{TransformerName}: Transformed {InputRows} input rows to {OutputRows} output rows",
            Name, inputRowCount, outputRowCount);
    }

    

    /// <summary>
    /// Apply column projection (renaming/filtering).
    /// PERFORMANCE: O(n) where n = projected columns (usually << total columns).
    /// </summary>
    private IDictionary<string, object> ApplyColumnProjection(
        IDictionary<string, object> row,
        Dictionary<string, string> projectionMap,
        HashSet<string>? projectedSet)
    {
        // If projection set is defined, only include those columns
        // Otherwise, rename all columns according to the map
        var result = projectedSet != null
            ? new Dictionary<string, object>(projectedSet.Count, StringComparer.Ordinal)
            : new Dictionary<string, object>(row.Count, StringComparer.Ordinal);

        foreach (var kvp in row)
        {
            // Skip column if filtering is enabled and column is not in projection set
            if (projectedSet != null && !projectedSet.Contains(kvp.Key))
            {
                continue;
            }

            // Use mapped name if available, otherwise use original name
            string outputName = projectionMap.TryGetValue(kvp.Key, out var mapped)
                ? mapped
                : kvp.Key;

            result[outputName] = kvp.Value;
        }

        return result;
    }

    /// <summary>
    /// Initialize column projections from configuration.
    /// Returns (projection map, projection set) tuple.
    /// Projection map: original → display name
    /// Projection set: columns to include (or null for all)
    /// </summary>
    private static (Dictionary<string, string>?, HashSet<string>?) InitializeColumnProjections(
        Dictionary<string, string>? projections)
    {
        if (projections == null || projections.Count == 0)
        {
            return (null, null);
        }

        var projectionMap = new Dictionary<string, string>(projections.Count, StringComparer.Ordinal);
        var projectionSet = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (originalName, displayName) in projections)
        {
            projectionMap[originalName] = displayName;
            projectionSet.Add(originalName);
        }

        return (projectionMap, projectionSet);
    }

    /// <summary>
    /// Trace row transformation (debugging only).
    /// Zero cost when tracing is disabled.
    /// </summary>
    [System.Diagnostics.Conditional("DEBUG")]
    private void TraceRow(int rowCount, IDictionary<string, object> row)
    {
        if (!Configuration.EnableTracing)
            return;

        var columnInfo = string.Join(", ", row.Keys.Take(5));
        if (row.Keys.Count > 5)
            columnInfo += "...";

        Logger?.LogDebug("{TransformerName}: Row {RowCount} has columns [{Columns}]",
            Name, rowCount, columnInfo);
    }

    public virtual void Dispose() { }

    #region Helper Methods

    protected static object GetNumericValue(IDictionary<string, object> dict, string key)
    {
        if (!dict.TryGetValue(key, out var value))
            return 0;

        return GetNumericValue(value);
    }

    protected static object GetNumericValue(object? value)
    {
        if (value == null)
            return 0;

        // Handle MongoDB BSON numeric types
        if (value is IDictionary<string, object> bsonValue)
        {
            if (bsonValue.TryGetValue("$numberLong", out var longVal))
                return Convert.ToInt64(longVal);
            if (bsonValue.TryGetValue("$numberInt", out var intVal))
                return Convert.ToInt32(intVal);
            if (bsonValue.TryGetValue("$numberDouble", out var doubleVal))
                return Convert.ToDouble(doubleVal);
        }

        if (value is string strValue && double.TryParse(strValue, out var parsedDouble))
            return parsedDouble;

        return Convert.ToDouble(value);
    }

    protected static long GetLongValue(IDictionary<string, object> dict, string key)
    {
        var numericValue = GetNumericValue(dict, key);
        return Convert.ToInt64(numericValue);
    }

    protected static double GetDoubleValue(IDictionary<string, object> dict, string key)
    {
        var numericValue = GetNumericValue(dict, key);
        return Convert.ToDouble(numericValue);
    }

    protected static bool GetBooleanValue(IDictionary<string, object> dict, string key)
    {
        if (!dict.TryGetValue(key, out var value))
            return false;

        if (value is bool boolValue)
            return boolValue;

        if (value is string strValue)
            return bool.TryParse(strValue, out var parsedBool) && parsedBool;

        return Convert.ToBoolean(value);
    }

    protected static string? GetStringValue(IDictionary<string, object> dict, string key)
    {
        return dict.TryGetValue(key, out var value) ? value?.ToString() : null;
    }

    protected static List<MappedFieldRow>? ExtractFieldMappings(Dictionary<string, object>? settings)
    {
        if (settings?.TryGetValue("fieldMappings", out var mappingsObj) != true)
            return null;

        return mappingsObj as List<MappedFieldRow>;
    }

    protected static Dictionary<Guid, string>? ExtractDataSourceDict(Dictionary<string, object>? settings)
    {
        if (settings?.TryGetValue("dataSourceDict", out var dictObj) != true)
            return null;

        return dictObj as Dictionary<Guid, string>;
    }
    protected static Dictionary<string, Dictionary<string, string>> BuildFieldMappingDictionary(List<MappedFieldRow>? fieldMappings)
    {
        var mappingDict = new Dictionary<string, Dictionary<string, string>>();

        if (fieldMappings == null)
            return mappingDict;

        foreach (var mappingRow in fieldMappings.Where(row => row.Include))
        {
            foreach (var fieldByDataSource in mappingRow.FieldByDataSource)
            {
                var field = fieldByDataSource.Value;

                if (field?.Mapped == true && !string.IsNullOrWhiteSpace(field.FieldName))
                {
                    if (!mappingDict.ContainsKey(field.FieldName))
                    {
                        mappingDict[field.FieldName] = new Dictionary<string, string>();
                    }

                    mappingDict[field.FieldName][field.DataSourceId.ToString()] = field.FieldName;
                }
            }
        }

        return mappingDict;
    }
    #endregion
}