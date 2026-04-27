using MatchLogic.Application.Common;
using System.Collections.Generic;

namespace MatchLogic.Infrastructure.Configuration;

/// <summary>
/// Configuration model for driving store selection per repository via appsettings.
/// Lives under "StoreSettings" section.
///
/// Example appsettings.json:
/// "StoreSettings": {
///   "Default": "MongoDB",
///   "Overrides": {
///     "JobStatusRepository": "ProgressMongoDB"
///   }
/// }
/// </summary>
public class StoreSettings
{
    public const string SectionName = "StoreSettings";

    /// <summary>
    /// Default store used when no override or [UseStore] attribute is found.
    /// Accepted values mirror the StoreType enum: MongoDB, ProgressMongoDB,
    /// LiteDb, ProgressLiteDb, InMemory.
    /// </summary>
    public string Default { get; set; } = nameof(StoreType.MongoDB);

    /// <summary>
    /// Per-repository overrides keyed by the short class name (not the full namespace).
    /// Example key: "JobStatusRepository"  (not "MatchLogic.Infrastructure.Repository.JobStatusRepository")
    /// </summary>
    public Dictionary<string, string> Overrides { get; set; } = new();
}