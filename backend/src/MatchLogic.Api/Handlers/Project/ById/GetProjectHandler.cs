using Ardalis.Result;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Project.ById;

public class GetProjectHandler(IProjectService projectService) : IRequestHandler<GetProjectRequest, Result<GetProjectResponse>>
{
    public async Task<Result<GetProjectResponse>> Handle(GetProjectRequest request, CancellationToken cancellationToken)
    {
        var project = await projectService.GetProjectById(request.Id);

        return Result<GetProjectResponse>.Success(
        new GetProjectResponse(
            project.Id,
            project.Name,
            project.Description,
            project.CreatedAt,
            project.ModifiedAt));

    }
}

