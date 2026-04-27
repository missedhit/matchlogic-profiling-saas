using System;
using System.Collections.Generic;
using MatchLogic.Domain.Entities.Common;

namespace MatchLogic.Domain.Licensing;

/// <summary>
/// Singleton license document — one per deployment, stored in MongoDB "License" collection.
/// Extends IEntity (not AuditableEntity) because audit fields would interfere with HMAC-protected timestamps.
/// </summary>
public class License : IEntity
{
    public LicenseType Type { get; set; } = LicenseType.Trial;

    /// <summary>
    /// Absolute expiry date — from JWT for both trial and full licenses. Mandatory.
    /// Trial: issuedAt + trialDays + graceDays (calculated in generate-license.js).
    /// Full: explicit expiry date from the JWT.
    /// </summary>
    public DateTime ExpiresAt { get; set; }

    // -- Trial fields -------------------------------------------------------

    /// <summary>UTC timestamp of when the license was first activated on this server. Informational only — not used for expiry calculation.</summary>
    public DateTime FirstRunAt { get; set; }

    /// <summary>Total records imported across all projects. Trial limit enforced against this.</summary>
    public long TotalRecordsImported { get; set; }

    /// <summary>Trial duration in days — set from JWT on activation (informational, for display).</summary>
    public int TrialDays { get; set; }

    /// <summary>Max records importable during trial — set from JWT on activation.</summary>
    public long TrialMaxRecords { get; set; }

    /// <summary>How many times the trial has been extended via extension keys.</summary>
    public int TrialExtensionCount { get; set; }

    // -- License identity ---------------------------------------------------

    /// <summary>Raw license key JWT provided by the licensee.</summary>
    public string? LicenseKey { get; set; }

    /// <summary>Parsed licensee name from the license key.</summary>
    public string? Licensee { get; set; }

    /// <summary>Unique license ID from the key. Used for online validation.</summary>
    public string? LicenseId { get; set; }

    /// <summary>Hardware fingerprint embedded in the license key.</summary>
    public string? LicensedFingerprint { get; set; }

    // -- Online check -------------------------------------------------------

    /// <summary>Last time online validation with license server succeeded.</summary>
    public DateTime? LastOnlineCheckAt { get; set; }

    // -- Integrity ----------------------------------------------------------

    /// <summary>
    /// All license IDs that have been activated on this installation.
    /// Prevents replay attacks — same key cannot be activated twice.
    /// </summary>
    public List<string> UsedLicenseIds { get; set; } = new();

    /// <summary>
    /// Composite HMAC-SHA256 over all protected fields.
    /// Any direct MongoDB edit to any field breaks this → LicenseStatus.Tampered.
    /// </summary>
    public string IntegrityHmac { get; set; } = string.Empty;
}

public enum LicenseType
{
    Trial = 0,
    Full  = 1
}

public enum LicenseStatus
{
    /// <summary>All good — no restrictions.</summary>
    Active = 0,

    /// <summary>Trial period has expired — import blocked.</summary>
    TrialExpired = 1,

    /// <summary>Trial record limit reached — import blocked.</summary>
    TrialLimitExceeded = 2,

    /// <summary>Online check failed but within grace period — import allowed, warning shown.</summary>
    GracePeriod = 3,

    /// <summary>Grace period expired — import blocked until online check succeeds.</summary>
    GraceExpired = 4,

    /// <summary>License key invalid, tampered, or fingerprint mismatch.</summary>
    Invalid = 5,

    /// <summary>Full license expiry date has passed.</summary>
    LicenseExpired = 6,

    /// <summary>License data tampered in MongoDB. FULL APP BLOCK. Only recoverable by purchasing a new license key.</summary>
    Tampered = 7,

    /// <summary>No license activated. FULL APP BLOCK. User must activate a trial or full license key.</summary>
    NotActivated = 8
}
