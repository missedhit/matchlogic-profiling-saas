using MatchLogic.Application.Interfaces.Project;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Project.Delete;

public class DeleteProjectHandler(IProjectService projectService) : IRequestHandler<DeleteProjectRequest, Result<DeleteProjectResponse>>
{
    public async Task<Result<DeleteProjectResponse>> Handle(DeleteProjectRequest request, CancellationToken cancellationToken)
    {
        await projectService.DeleteProject(request.Guid);
        return Result<DeleteProjectResponse>.NoContent();
    }
}
