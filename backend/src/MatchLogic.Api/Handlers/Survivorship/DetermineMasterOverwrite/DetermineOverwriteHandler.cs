using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using Mapster;
using MatchLogic.Application.Common;
using MatchLogic.Domain.Entities.Common;
using System.Collections.Generic;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;

namespace MatchLogic.Api.Handlers.Survivorship.DetermineOverwrite;
public class DetermineOverwriteHandler(IProjectService projectService
        , ILogger<DetermineOverwriteHandler> logger) : IRequestHandler<DetermineOverwriteCommand, Result<DetermineOverwriteResponse>>
{
    async Task<Result<DetermineOverwriteResponse>> IRequestHandler<DetermineOverwriteCommand, Result<DetermineOverwriteResponse>>.Handle(DetermineOverwriteCommand request, CancellationToken cancellationToken)
    {
        var stepInformation = new List<StepConfiguration>
        {            
            // Add Matching result
            new(StepType.Overwrite,new Dictionary<string, object> { { "ProjectId", request.ProjectId } },dataSourceIds:[request.ProjectId])
        };

        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);

        return Result<DetermineOverwriteResponse>.Success(new DetermineOverwriteResponse(queuedRun));
    }
}
