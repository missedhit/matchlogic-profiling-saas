using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using Mapster;
using MatchLogic.Application.Common;
using MatchLogic.Api.Handlers.MatchDefinition.Create;
using MatchLogic.Application.Interfaces.MatchConfiguration;

namespace MatchLogic.Api.Handlers.MatchDefinition.Update;
public class UpdateMatchDefinitionHandler : IRequestHandler<UpdateMatchDefinitionCommand, Result<UpdateMatchDefinitionResponse>>
{
    private readonly ILogger<UpdateMatchDefinitionHandler> _logger;
    private readonly IMatchConfigurationService _matchConfiguration;

    public UpdateMatchDefinitionHandler(IMatchConfigurationService matchConfiguration, ILogger<UpdateMatchDefinitionHandler> logger)
    {
        _matchConfiguration = matchConfiguration;
        _logger = logger;
    }
    async Task<Result<UpdateMatchDefinitionResponse>> IRequestHandler<UpdateMatchDefinitionCommand, Result<UpdateMatchDefinitionResponse>>.Handle(UpdateMatchDefinitionCommand request, CancellationToken cancellationToken)
    {
        await _matchConfiguration.DeleteCollectionAsync(request.MatchDefinition.Id);
        request.MatchDefinition.Id = Guid.Empty;
        var matchDefinitionId = await _matchConfiguration.SaveMappedRowConfigurationAsync(request.MatchDefinition);
        var matchSettingId = await _matchConfiguration.SaveSettingsAsync(request.MatchSetting);


        var matchDefinition = await _matchConfiguration.GetMappedRowConfigurationByProjectIdAsync(request.MatchDefinition.ProjectId);
        var matchSettings = await _matchConfiguration.GetSettingsByProjectIdAsync(request.MatchSetting.ProjectId);

        var response = new UpdateMatchDefinitionResponse
        {
            MatchDefinition = matchDefinition,
            MatchSetting = matchSettings
        };

        return new Result<UpdateMatchDefinitionResponse>(response);
    }
}
