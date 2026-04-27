using MatchLogic.Application.Licensing;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.License.GetFingerprint;

public class GetFingerprintHandler : IRequestHandler<GetFingerprintQuery, Result<GetFingerprintResponse>>
{
    private readonly ILicenseService _licenseService;
    private readonly ILogger<GetFingerprintHandler> _logger;

    public GetFingerprintHandler(
        ILicenseService licenseService,
        ILogger<GetFingerprintHandler> logger)
    {
        _licenseService = licenseService;
        _logger = logger;
    }

    public async Task<Result<GetFingerprintResponse>> Handle(
        GetFingerprintQuery request,
        CancellationToken cancellationToken)
    {
        try
        {
            var fingerprint = await _licenseService.GetServerFingerprintAsync();
            return Result<GetFingerprintResponse>.Success(
                new GetFingerprintResponse(fingerprint));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[GetFingerprintHandler] Failed to get server fingerprint");
            return Result<GetFingerprintResponse>.Error("Failed to retrieve server fingerprint.");
        }
    }
}
