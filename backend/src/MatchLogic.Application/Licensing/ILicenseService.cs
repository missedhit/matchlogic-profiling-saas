using System.Threading.Tasks;

namespace MatchLogic.Application.Licensing;

public interface ILicenseService
{
    /// <summary>Current license status — used by middleware, endpoints, and frontend polling.</summary>
    Task<LicenseInfo> GetLicenseInfoAsync();

    /// <summary>Returns true if data import is currently allowed.</summary>
    Task<bool> IsImportAllowedAsync();

    /// <summary>
    /// Called after a successful import.
    /// Increments the HMAC-protected record count for trial tracking.
    /// </summary>
    Task IncrementRecordCountAsync(long recordsAdded);

    /// <summary>
    /// Called when a datasource or project is deleted.
    /// Decrements the trial record count to reclaim quota.
    /// </summary>
    Task DecrementRecordCountAsync(long recordsRemoved);

    /// <summary>
    /// Validate and activate a full license key.
    /// Checks JWT signature, hardware fingerprint, and runs online check if configured.
    /// </summary>
    Task<LicenseActivationResult> ActivateLicenseAsync(string licenseKey);

    /// <summary>
    /// Returns the hardware fingerprint of this server.
    /// Customer sends this when requesting a license key.
    /// </summary>
    Task<string> GetServerFingerprintAsync();

    /// <summary>
    /// Run online validation against license server.
    /// No-op if LicenseSettings.LicenseServerUrl is null.
    /// </summary>
    Task RunOnlineCheckAsync();
}
