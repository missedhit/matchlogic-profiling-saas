namespace MatchLogic.Application.Licensing;

public class LicenseSettings
{
    public const string SectionName = "LicenseSettings";

    /// <summary>
    /// Days import stays allowed after online license check fails.
    /// Only relevant when LicenseServerUrl is configured.
    /// </summary>
    public int GracePeriodDays { get; set; } = 7;

    /// <summary>
    /// License validation server URL.
    /// Null or empty = skip online check entirely (offline-only mode).
    /// </summary>
    public string? LicenseServerUrl { get; set; }

    /// <summary>How often to re-validate with license server (hours).</summary>
    public int OnlineCheckIntervalHours { get; set; } = 24;

    /// <summary>Number of days for the auto-provisioned trial on first launch.</summary>
    public int AutoTrialDays { get; set; } = 14;

    /// <summary>Maximum records allowed during the auto-provisioned trial.</summary>
    public long AutoTrialMaxRecords { get; set; } = 10000;
}
