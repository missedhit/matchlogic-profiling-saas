namespace MatchLogic.Api.Common;
/// <summary>
/// Feature flags for the application.
/// </summary>
public class FeatureFlags
{
    /// <summary>
    /// Gets or sets a value indicating whether to enable advance profiling.
    /// </summary>
    public bool AdvanceProfiling { get; set; }
    public bool Cleansing { get; set; }
}