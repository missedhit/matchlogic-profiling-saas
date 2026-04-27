using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Storage;

/// <summary>
/// Interface for persisting and retrieving match graphs.
/// Implementation handles serialization details.
/// </summary>
public interface IMatchGraphStorage
{
    /// <summary>
    /// Saves a MatchGraphDME to persistent storage
    /// </summary>
    Task SaveMatchGraphAsync(
        IDataStore dataStore,
        string collectionName,
        MatchGraphDME graph,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Loads a MatchGraphDME from persistent storage
    /// </summary>
    Task<MatchGraphDME> LoadMatchGraphAsync(
        IDataStore dataStore,
        string collectionName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if a match graph exists in the collection
    /// </summary>
    Task<bool> GraphExistsAsync(
        IDataStore dataStore,
        string collectionName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets metadata about the stored graph without loading the entire graph
    /// </summary>
    Task<MatchGraphMetadata> GetGraphMetadataAsync(
        IDataStore dataStore,
        string collectionName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Metadata about a stored graph (without loading the full graph)
/// </summary>
public class MatchGraphMetadata
{
    public Guid GraphId { get; set; }
    public Guid ProjectId { get; set; }
    public int TotalNodes { get; set; }
    public int TotalEdges { get; set; }
    public long CompressedSizeBytes { get; set; }
    public long UncompressedSizeBytes { get; set; }
    public double CompressionRatio { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime SavedAt { get; set; }

    public string CompressedSizeMB => $"{CompressedSizeBytes / (1024.0 * 1024.0):F2} MB";
    public string UncompressedSizeMB => $"{UncompressedSizeBytes / (1024.0 * 1024.0):F2} MB";
    public string CompressionPercentage => $"{(1 - CompressionRatio) * 100:F1}%";
}
