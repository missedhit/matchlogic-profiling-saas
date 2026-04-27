using MatchLogic.Application.Interfaces.Project;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Project.Create;

public class CreateProjectHandler(IProjectService projectService, ILogger<CreateProjectHandler> logger) : IRequestHandler<CreateProjectRequest, Result<CreateProjectResponse>>
{
    public async Task<Result<CreateProjectResponse>> Handle(CreateProjectRequest request, CancellationToken cancellationToken)
    {
        var newProj = await projectService.CreateProject(request.Name, request.Description);
        return Result<CreateProjectResponse>.Success(new CreateProjectResponse(newProj.Id, newProj.Name, newProj.Description, newProj.CreatedAt, newProj.ModifiedAt));
    }
}
