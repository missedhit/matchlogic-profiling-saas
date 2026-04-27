using MatchLogic.Application.Interfaces.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Project.Update;

public class UpdateProjectHandler : IRequestHandler<UpdateProjectRequest, Result<UpdateProjectResponse>>
{
    private readonly IProjectService _projectService;
    private readonly ILogger<UpdateProjectHandler> _logger;

    public UpdateProjectHandler(IProjectService projectService, ILogger<UpdateProjectHandler> logger)
    {
        _projectService = projectService;
        _logger = logger;
    }

    public async Task<Result<UpdateProjectResponse>> Handle(UpdateProjectRequest request, CancellationToken cancellationToken)
    {
        var project = await _projectService.UpdateProject(request.Id, request.Name, request.Description);
        if (project == null)
        {
            return Result<UpdateProjectResponse>.NotFound();
        }
        return Result<UpdateProjectResponse>.Success(
            new UpdateProjectResponse
            (
                Id: project.Id,
                Name: project.Name,
                Description: project.Description,
                CreatedAt: project.CreatedAt,
                ModifiedAt: project.ModifiedAt
            ));
    }
}
