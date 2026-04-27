using MatchLogic.Application.Licensing;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.License.Activate;

public class ActivateLicenseHandler : IRequestHandler<ActivateLicenseCommand, Result<LicenseActivationResult>>
{
    private readonly ILicenseService _licenseService;
    private readonly ILogger<ActivateLicenseHandler> _logger;

    public ActivateLicenseHandler(
        ILicenseService licenseService,
        ILogger<ActivateLicenseHandler> logger)
    {
        _licenseService = licenseService;
        _logger = logger;
    }

    public async Task<Result<LicenseActivationResult>> Handle(
        ActivateLicenseCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("[ActivateLicenseHandler] Activation attempt");

            var result = await _licenseService.ActivateLicenseAsync(request.LicenseKey);

            if (!result.Success)
            {
                _logger.LogWarning("[ActivateLicenseHandler] Activation failed: {Error}", result.Error);
                return Result<LicenseActivationResult>.Error(result.Error ?? "Activation failed.");
            }

            _logger.LogInformation("[ActivateLicenseHandler] License activated successfully");
            return Result<LicenseActivationResult>.Success(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[ActivateLicenseHandler] Activation failed unexpectedly");
            return Result<LicenseActivationResult>.Error("License activation failed. Please try again.");
        }
    }
}
