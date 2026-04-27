using MatchLogic.Application.Interfaces.Project;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Delete;

public class DeleteDataSourceHandler(IProjectService projectService)
    : IRequestHandler<DeleteDataSourceRequest, Result<DeleteDataSourceResponse>>
{
    public async Task<Result<DeleteDataSourceResponse>> Handle(DeleteDataSourceRequest request, CancellationToken cancellationToken)
    {
        await projectService.RemoveDataSource(request.ProjectId, request.Id);
        return Result<DeleteDataSourceResponse>.NoContent();
    }
}
