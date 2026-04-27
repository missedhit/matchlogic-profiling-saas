using MatchLogic.Application.Licensing;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.License.GetInfo;

public class GetLicenseInfoHandler : IRequestHandler<GetLicenseInfoQuery, Result<LicenseInfo>>
{
    private readonly ILicenseService _licenseService;
    private readonly ILogger<GetLicenseInfoHandler> _logger;

    public GetLicenseInfoHandler(
        ILicenseService licenseService,
        ILogger<GetLicenseInfoHandler> logger)
    {
        _licenseService = licenseService;
        _logger = logger;
    }

    public async Task<Result<LicenseInfo>> Handle(
        GetLicenseInfoQuery request,
        CancellationToken cancellationToken)
    {
        try
        {
            var info = await _licenseService.GetLicenseInfoAsync();
            return Result<LicenseInfo>.Success(info);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[GetLicenseInfoHandler] Failed to retrieve license info");
            return Result<LicenseInfo>.Error("Failed to retrieve license information.");
        }
    }
}
