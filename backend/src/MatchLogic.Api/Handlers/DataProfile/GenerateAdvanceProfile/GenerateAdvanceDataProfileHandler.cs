using MatchLogic.Application.Interfaces.Project;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Domain.Import;
using System.Collections.Generic;
using System;
using MatchLogic.Application.Features.Project;
namespace MatchLogic.Api.Handlers.DataProfile.GenerateAdvanceProfile;


public class GenerateAdvanceDataProfileHandler(IProjectService projectService)
    : IRequestHandler<GenerateAdvanceDataProfileRequest, Result<GenerateAdvanceDataProfileResponse>>
{
    public async Task<Result<GenerateAdvanceDataProfileResponse>> Handle(GenerateAdvanceDataProfileRequest request, CancellationToken cancellationToken)
    {
        var stepInformation = new List<StepConfiguration>
        {
            // Add Advance Profiling step
            new(StepType.AdvanceProfile, request.DataSourceIds.ToArray())
        };

        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);
        return Result<GenerateAdvanceDataProfileResponse>.Success(new GenerateAdvanceDataProfileResponse(queuedRun));

    }
}
