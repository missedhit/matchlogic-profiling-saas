namespace MatchLogic.Application.Licensing;

public class LicenseActivationResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public LicenseInfo? LicenseInfo { get; set; }
}
