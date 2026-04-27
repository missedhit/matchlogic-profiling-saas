using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.DependencyInjection;

namespace MatchLogic.Api.IntegrationTests.Builders;
public class DataSourceBuilder
{
    private readonly DataSource _dataSource = new();
    private Project _project;
    private readonly IServiceProvider _serviceProvider;
    private readonly IProjectService _projectService;

    public DataSourceBuilder(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _projectService = serviceProvider.GetRequiredService<IProjectService>();
    }

    public DataSourceBuilder WithName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name, nameof(name));
        //if (string.IsNullOrWhiteSpace(name))
        //    throw new ArgumentException("Name cannot be null or empty.", nameof(name));
        _dataSource.Name = name;
        return this;
    }

    public DataSourceBuilder WithType(DataSourceType type)
    {
        _dataSource.Type = type;
        return this;
    }

    public DataSourceBuilder WithConnectionDetails(BaseConnectionInfo connectionDetails)
    {
        _dataSource.ConnectionDetails = connectionDetails;
        return this;
    }

    public DataSourceBuilder WithConfiguration(DataSourceConfiguration configuration)
    {
        _dataSource.Configuration = configuration;
        return this;
    }

    public DataSourceBuilder WithProject(Project project)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nameof(project));
        //_project = project ?? throw new ArgumentNullException(nameof(project));
        _project = project;
        _dataSource.ProjectId = _project.Id;
        return this;
    }


    public DataSource BuildDomain()
    {
        return _dataSource;
    }

    public async Task<(DataSource DataSource, ProjectRun ProjectRun)> AddSingleDataSourceAsync(string dataSourceName)
    {
        var dataSources = await AddMultipleDataSourcesAsync([dataSourceName]);
        return (dataSources.DataSources!.First(), dataSources.ProjectRun);

    }
    public async Task<(List<DataSource> DataSources, ProjectRun ProjectRun)> AddMultipleDataSourcesAsync(string[] dataSourceNames)
    {
        if (_project == null)
        {
            _project = await new ProjectBuilder(_serviceProvider)
                .BuildAsync();
        }

        FileImport testFile = await new FileImportBuilder(_serviceProvider)
            .WithProjectId(_project.Id)
            .BuildAsync();

        // Convert the array of data source names to a list of DataSourceRequest objects
        var ds = dataSourceNames.Select(name => new DataSourceRequest(name, "Sheet1", [])).ToList();

        var handerRequest = new CreateDataSourceRequest
        (
                            ProjectId: _project.Id,
                            Connection: new BaseConnectionInfo
                            {
                                Type = DataSourceType.Excel,
                                Parameters =
                                {
                                    ["FileId"] = testFile.Id.ToString(),
                                    ["FilePath"] = testFile.FilePath,
                                    ["HasHeaders"] = "true"
                                }
                            },
                            DataSources: ds
                        );

        return await AddMultipleDataSourcesAsync(handerRequest);

    }

    public async Task<(DataSource DataSource, ProjectRun ProjectRun)> AddSingleDataSourceAsync(CreateDataSourceRequest request)
    {
        var dataSources = await AddMultipleDataSourcesAsync(request);
        return (dataSources.DataSources!.First(), dataSources.ProjectRun);
    }

    public async Task<(List<DataSource> DataSources, ProjectRun ProjectRun)> AddMultipleDataSourcesAsync(CreateDataSourceRequest request)
    {
        var connectionInfo = request.Connection;
        List<DataSource> dataSources = [];
        foreach (var item in request.DataSources)
        {
            DataSource dataSource = new()
            {
                Id = Guid.NewGuid(),
                Name = item.Name ?? item.TableName, // If no data source name is provided, use the sheet name
                Type = connectionInfo.Type,
                ConnectionDetails = connectionInfo,
                CreatedAt = DateTime.UtcNow,
                ModifiedAt = DateTime.UtcNow,
                Configuration = new DataSourceConfiguration
                {
                    TableOrSheet = item.TableName,
                    ColumnMappings = item.ColumnMappings // Ensures each column name is unique by using a dictionary for column mappings     
                }
            };
            dataSources.Add(dataSource);
        }

        // Ensure the collection is not modified during enumeration
        var dataSourcesCopy = new List<Domain.Project.DataSource>(dataSources);
        await _projectService.AddDataSource(request.ProjectId, dataSourcesCopy);

        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = dataSourcesCopy.Select(x => x.Id).ToArray();

        // Add import step
        stepInformation.Add(new StepConfiguration(
        StepType.Import,
        dataSourceIds
        ));

        var queuedRun = await _projectService.StartNewRun(request.ProjectId, stepInformation);
        // Wait for the data source to be added
        Task.Delay(5000).Wait();
        return (dataSourcesCopy, queuedRun);
    }
}