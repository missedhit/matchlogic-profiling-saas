using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataProfile.GenerateProfile;

public class GenerateDataProfileHandler(IProjectService projectService) : IRequestHandler<GenerateDataProfileRequest, Result<GenerateDataProfileResponse>>
{
    public async Task<Result<GenerateDataProfileResponse>> Handle(GenerateDataProfileRequest request, CancellationToken cancellationToken)
    {
        var stepInformation = new List<StepConfiguration>
        {
            // Add Profiling step
            new StepConfiguration(StepType.Profile, request.DataSourceIds.ToArray())
        };

        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);
        return Result<GenerateDataProfileResponse>.Success(new GenerateDataProfileResponse(queuedRun));
    }
}
