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

namespace MatchLogic.Api.Handlers.MatchResult.Create;
public class CreateMatchResultHandler(IProjectService projectService
        , ILogger<CreateMatchResultHandler> logger) : IRequestHandler<CreateMatchResultCommand, Result<CreateMatchResultResponse>>
{
    async Task<Result<CreateMatchResultResponse>> IRequestHandler<CreateMatchResultCommand, Result<CreateMatchResultResponse>>.Handle(CreateMatchResultCommand request, CancellationToken cancellationToken)
    {
        var stepInformation = new List<StepConfiguration>
        {            
            // Add Matching result
            new(StepType.Match,new Dictionary<string, object> { { "ProjectId", request.ProjectId } },dataSourceIds:[request.ProjectId])
        };

        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);

        return Result<CreateMatchResultResponse>.Success(new CreateMatchResultResponse(queuedRun));
    }
}
