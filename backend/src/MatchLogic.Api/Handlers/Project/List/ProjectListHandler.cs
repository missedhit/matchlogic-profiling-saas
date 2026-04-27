using MatchLogic.Application.Interfaces.Project;
using Mapster;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Project.List;

public class ProjectListHandler(IProjectService projectService,ILogger<ProjectListHandler> logger) : IRequestHandler<ProjectListRequest, Result<List<ProjectListResponse>>>
{
    public async Task<Result<List<ProjectListResponse>>> Handle(ProjectListRequest request, CancellationToken cancellationToken)
    {
        var result = await projectService.GetAllProjects();

        if (result == null)
        {
            logger.LogError("No projects found");
            return Result<List<ProjectListResponse>>.NotFound("No Projects Found");
        }

        if (!result.Any())
        {
            logger.LogError("No projects found");
            return Result<List<ProjectListResponse>>.NotFound("No Projects Found");
        }
        var items = result.Adapt<List<ProjectListResponse>>();
        return Result<List<ProjectListResponse>>.Success(items);
    }
}
