using MatchLogic.Application.Licensing;

namespace MatchLogic.Api.Handlers.License.Activate;

public class ActivateLicenseCommand : IRequest<Result<LicenseActivationResult>>
{
    public string LicenseKey { get; set; } = string.Empty;
}
