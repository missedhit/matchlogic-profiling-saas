using Ardalis.Result;
using MediatR;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using Mapster;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.MatchConfiguration;

namespace MatchLogic.Api.Handlers.MatchDefinition.Create;
public class CreateMatchDefinitionHandler : IRequestHandler<CreateMatchDefinitionCommand, Result<CreateMatchDefinitionResponse>>
{
    private readonly ILogger<CreateMatchDefinitionHandler> _logger;
    private readonly IMatchConfigurationService _matchConfiguration;

    public CreateMatchDefinitionHandler(IMatchConfigurationService matchConfiguration, ILogger<CreateMatchDefinitionHandler> logger)
    {
        _matchConfiguration = matchConfiguration;
        _logger = logger;
    }
    async Task<Result<CreateMatchDefinitionResponse>> IRequestHandler<CreateMatchDefinitionCommand, Result<CreateMatchDefinitionResponse>>.Handle(CreateMatchDefinitionCommand request, CancellationToken cancellationToken)
    {

        var matchDefinitionId = await _matchConfiguration.SaveMappedRowConfigurationAsync(request.MatchDefinition);
        var matchSettingId = await _matchConfiguration.SaveSettingsAsync(request.MatchSetting);


        var matchDefinition = await _matchConfiguration.GetMappedRowConfigurationByProjectIdAsync(request.MatchDefinition.ProjectId);
        var matchSettings = await _matchConfiguration.GetSettingsByProjectIdAsync(request.MatchSetting.ProjectId);

        var response = new CreateMatchDefinitionResponse
        {
            MatchDefinition = matchDefinition,
            MatchSetting = matchSettings
        };

        return new Result<CreateMatchDefinitionResponse>(response);        
    }
}
