using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Create;

public class CreateDataSourceHandler(IProjectService projectService, ILogger<CreateDataSourceHandler> logger)
    : IRequestHandler<CreateDataSourceRequest, Result<CreateDataSourceResponse>>
{
    public async Task<Result<CreateDataSourceResponse>> Handle(CreateDataSourceRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("CreateDataSourceHandler: Configuring ConnectionInfo with : {Request}", request);

        // For file-based data sources, ConnectionDetails.Parameters carries a FileId
        // pointing at the FileImport doc; we do NOT resolve to a local FilePath here
        // because /tmp paths are ephemeral per ECS task. DataImportCommand resolves
        // the FileId at job-execution time.
        List<Domain.Project.DataSource> dataSources = [];
        foreach (var item in request.DataSources)
        {
            Domain.Project.DataSource DataSource = new()
            {
                Id = Guid.NewGuid(),
                Name = item.Name ?? item.TableName,
                Type = request.Connection.Type,
                ConnectionDetails = request.Connection,
                Configuration = new DataSourceConfiguration
                {
                    Query = item.Query,
                    TableOrSheet = item.TableName,
                    ColumnMappings = item.ColumnMappings
                }
            };
            dataSources.Add(DataSource);
        }

        await projectService.AddDataSource(request.ProjectId, dataSources);

        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = dataSources.Select(x => x.Id).ToArray();

        stepInformation.Add(new StepConfiguration(
            StepType.Import,
            dataSourceIds
        ));
        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);
        logger.LogInformation("CreateDataSourceHandler: Started JobRun : {queuedRun}", queuedRun);
        return Result<CreateDataSourceResponse>.Success(new CreateDataSourceResponse(queuedRun));
    }
}
