using MatchLogic.Application.Interfaces.MatchConfiguration;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Delete;

public class DeleteMatchConfigurationHandler(IMatchConfigurationService matchConfigurationService,ILogger<DeleteMatchConfigurationHandler> logger) : IRequestHandler<DeleteMatchConfigurationRequest, Result<DeleteMatchConfigurationResponse>>
{
    public async Task<Result<DeleteMatchConfigurationResponse>> Handle(DeleteMatchConfigurationRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("Handling DeleteMatchConfigurationRequest for MatchConfigurationId: {MatchConfigurationId}", request.MatchConfigurationId);
        // Call the service to delete the match configuration
        await matchConfigurationService.DeleteDataSourcePairAsync(request.MatchConfigurationId);
        return Result<DeleteMatchConfigurationResponse>.Success(new DeleteMatchConfigurationResponse(request.MatchConfigurationId, "Match configuration deleted successfully."));
    }
}
