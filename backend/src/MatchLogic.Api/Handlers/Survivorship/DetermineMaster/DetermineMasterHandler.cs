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

namespace MatchLogic.Api.Handlers.Survivorship.DetermineMaster;
public class DetermineMasterHandler(IProjectService projectService
        , ILogger<DetermineMasterHandler> logger) : IRequestHandler<DetermineMasterCommand, Result<DetermineMasterResponse>>
{
    async Task<Result<DetermineMasterResponse>> IRequestHandler<DetermineMasterCommand, Result<DetermineMasterResponse>>.Handle(DetermineMasterCommand request, CancellationToken cancellationToken)
    {
        var stepInformation = new List<StepConfiguration>
        {            
            // Add Matching result
            new(StepType.Merge,new Dictionary<string, object> { { "ProjectId", request.ProjectId } },dataSourceIds:[request.ProjectId])
        };

        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);

        return Result<DetermineMasterResponse>.Success(new DetermineMasterResponse(queuedRun));
    }
}
