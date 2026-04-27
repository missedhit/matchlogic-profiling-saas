using MatchLogic.Api.Handlers.DataSource.Base;
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

public class CreateDataSourceHandler(IProjectService projectService,ILogger<CreateDataSourceHandler> logger) : BaseConnectionInfoHandler, IRequestHandler<CreateDataSourceRequest, Result<CreateDataSourceResponse>>
{
    // Sets the ConnectionInfo object for Preview Services using the DataReader function.

    public async Task<Result<CreateDataSourceResponse>> Handle(CreateDataSourceRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("CreateDataSourceHandler: Configuring ConnectionInfo with : {Request}", request);
        List<Domain.Project.DataSource> dataSources = [];
        foreach (var item in request.DataSources)
        {
            Domain.Project.DataSource DataSource = new()
            {
                Id = Guid.NewGuid(),
                Name = item.Name ?? item.TableName, // If no data source name is provided, use Table name
                Type = request.Connection.Type,
                ConnectionDetails = ConfigureConnectionInfo(request.Connection),
                Configuration = new DataSourceConfiguration
                {
                    Query = item.Query,
                    TableOrSheet = item.TableName,
                    ColumnMappings = item.ColumnMappings // Ensures each column name is unique by using a dictionary for column mappings     
                }
            };
            dataSources.Add(DataSource);
        }
        
        
        await projectService.AddDataSource(request.ProjectId, dataSources);

        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = dataSources.Select(x => x.Id).ToArray();

        // Add import step
        stepInformation.Add(new StepConfiguration(
            StepType.Import,
            dataSourceIds
        ));
        var queuedRun = await projectService.StartNewRun(request.ProjectId, stepInformation);
        logger.LogInformation("CreateDataSourceHandler: Started JobRun : {queuedRun}", queuedRun);
        return Result<CreateDataSourceResponse>.Success(new CreateDataSourceResponse(queuedRun));
    }
}
