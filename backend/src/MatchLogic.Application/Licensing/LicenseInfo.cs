using System;
using MatchLogic.Domain.Licensing;

namespace MatchLogic.Application.Licensing;

public class LicenseInfo
{
    public LicenseStatus Status { get; set; }
    public LicenseType Type { get; set; }
    public string? Licensee { get; set; }

    // Trial
    public int TrialDaysRemaining { get; set; }
    public int TrialDaysTotal { get; set; }
    public long RecordsUsed { get; set; }
    public long RecordsLimit { get; set; }
    public double RecordUsagePercent =>
        RecordsLimit > 0 ? Math.Round((double)RecordsUsed / RecordsLimit * 100, 1) : 0;

    // Full license
    public DateTime? ExpiresAt { get; set; }
    public bool IsExpiringSoon =>
        ExpiresAt.HasValue && ExpiresAt.Value < DateTime.UtcNow.AddDays(14);

    // Grace period
    public int GraceDaysRemaining { get; set; }

    /// <summary>
    /// True when Tampered, NotActivated, or LicenseExpired.
    /// Middleware blocks ALL write routes. Frontend shows full-screen overlay.
    /// </summary>
    public bool IsFullyBlocked =>
        Status == LicenseStatus.Tampered ||
        Status == LicenseStatus.NotActivated ||
        Status == LicenseStatus.LicenseExpired;

    // UI
    public bool CanImport { get; set; }
    public bool ShowUpgradeBanner { get; set; }
    public string StatusMessage { get; set; } = string.Empty;
}
