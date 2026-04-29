using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Refresh;

public class RefreshDataSourceHandler(IProjectService projectService, ILogger<RefreshDataSourceHandler> logger)
    : IRequestHandler<RefreshDataSourceRequest, Result<RefreshDataSourceResponse>>
{
    public async Task<Result<RefreshDataSourceResponse>> Handle(RefreshDataSourceRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("RefreshDataSourceResponse: Configuring ConnectionInfo with : {Request}", request);


        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = new[] { request.DataSourceId };

        // Add import step
        stepInformation.Add(new StepConfiguration(
            StepType.Import,
           new Dictionary<string, object>
            {
                { "fileImportId", request.FileImportId }
            },
        dataSourceIds
        ));
        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);
        logger.LogInformation("RefreshDataSourceHandler: Started JobRun : {queuedRun}", queuedRun);
        return Result<RefreshDataSourceResponse>.Success(new RefreshDataSourceResponse(queuedRun));
    }
}
