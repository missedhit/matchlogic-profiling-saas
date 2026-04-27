using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Application.Licensing;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Project.Delete;

public class DeleteProjectHandler(IProjectService projectService, 
                                  IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
ILicenseService licenseService, ISchedulerService schedulerService) : IRequestHandler<DeleteProjectRequest, Result<DeleteProjectResponse>>
{
    public async Task<Result<DeleteProjectResponse>> Handle(DeleteProjectRequest request, CancellationToken cancellationToken)
    {
        var dataSources = await dataSourceRepository.QueryAsync(
             ds => ds.ProjectId == request.Guid,
             Constants.Collections.DataSources);
        var totalRecords = dataSources.Sum(ds => ds.RecordCount);
        await schedulerService.DeleteSchedulesByProjectAsync(request.Guid);
        await projectService.DeleteProject(request.Guid);
        // Reclaim trial record quota
        if (totalRecords > 0)
            await licenseService.DecrementRecordCountAsync(totalRecords);
            
        return Result<DeleteProjectResponse>.NoContent();
    }
}
