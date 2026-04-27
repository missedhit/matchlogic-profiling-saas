using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.MatchConfiguration;
namespace MatchLogic.Api.Handlers.MatchDefinition.Get;
public class MatchDefinitionHandler : IRequestHandler<MatchDefinitionRequest, Result<MatchDefinitionResponse>>
{
    private readonly ILogger<MatchDefinitionHandler> _logger;
    private readonly IMatchConfigurationService _matchConfiguration;

    public MatchDefinitionHandler(IMatchConfigurationService matchConfiguration, ILogger<MatchDefinitionHandler> logger)
    {
        _matchConfiguration = matchConfiguration;
        _logger = logger;
    }
    async Task<Result<MatchDefinitionResponse>> IRequestHandler<MatchDefinitionRequest, Result<MatchDefinitionResponse>>.Handle(MatchDefinitionRequest request, CancellationToken cancellationToken)
    {        
        var projectId = request.projectId;
        var matchDef = await _matchConfiguration.GetMappedRowConfigurationByProjectIdAsync(projectId);
        var matchSetting = await _matchConfiguration.GetSettingsByProjectIdAsync(projectId);

        if (matchDef == null)
        {
            _logger.LogError("No matchdefinition foubd for  projectId: {projectId}", projectId);
            return Result.NotFound($"No data found for job ID: {projectId}");
        }

        var dto = matchDef;
        
        var response = new MatchDefinitionResponse
        {
            MatchDefinition = matchDef,
            MatchSetting = matchSetting,
        };

        _logger.LogInformation("Match defintion found successfully. projectId: {projectId}", projectId);

        return new Result<MatchDefinitionResponse>(response);
    }
}
