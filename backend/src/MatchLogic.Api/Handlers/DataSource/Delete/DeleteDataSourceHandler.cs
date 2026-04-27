using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Licensing;
using System;
using System.Threading;
using System.Threading.Tasks;


namespace MatchLogic.Api.Handlers.DataSource.Delete;
public class DeleteDataSourceHandler(
    IProjectService projectService,
    IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
    ILicenseService licenseService) : IRequestHandler<DeleteDataSourceRequest, Result<DeleteDataSourceResponse>>
{
    public async Task<Result<DeleteDataSourceResponse>> Handle(DeleteDataSourceRequest request, CancellationToken cancellationToken)
    {
        // Fetch record count before deletion so we can reclaim trial quota
        var dataSource = await dataSourceRepository.GetByIdAsync(
            request.Id, Constants.Collections.DataSources);
        var recordCount = dataSource?.RecordCount ?? 0;

        await projectService.RemoveDataSource(request.ProjectId, request.Id);

        // Reclaim trial record quota
        if (recordCount > 0)
            await licenseService.DecrementRecordCountAsync(recordCount);

        return Result<DeleteDataSourceResponse>.NoContent();
    }
}
