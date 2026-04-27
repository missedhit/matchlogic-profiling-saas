using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using System;
using System.Threading;
using System.Threading.Tasks;


namespace MatchLogic.Api.Handlers.DataSource.Update;
public class UpdateDataSourceHandler(IProjectService projectService) : IRequestHandler<UpdateDataSourceRequest, Result<UpdateDataSourceResponse>>
{
    public async Task<Result<UpdateDataSourceResponse>> Handle(UpdateDataSourceRequest request, CancellationToken cancellationToken)
    {
        var updatedDataSource = await projectService.RenameDataSourceAsync(
               request.Id,
               request.Name);

        return Result<UpdateDataSourceResponse>.Success(new UpdateDataSourceResponse(
            Id: updatedDataSource.Id,
            Name: updatedDataSource.Name,
            ModifiedAt: updatedDataSource.ModifiedAt
            ));
    }
}
