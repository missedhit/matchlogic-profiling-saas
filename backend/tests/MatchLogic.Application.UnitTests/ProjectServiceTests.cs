using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.BackgroundJob;
using LiteDB;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPOI.SS.Formula.Functions;
using NPOI.XSSF.UserModel;
using ColumnMapping = MatchLogic.Domain.Project.ColumnMapping;

namespace MatchLogic.Application.UnitTests;
public class ProjectServiceTests : IDisposable, IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly ILogger<ProjectService> _logger;
    private readonly IProjectService _projectService;
    private readonly IDataStore _dataStore;
    private readonly IGenericRepository<Project, Guid> _projectRepository;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<Domain.Entities.MatchDefinition, Guid> _matchDefinitionRepository;
    private readonly IGenericRepository<CleaningRules, Guid> _cleaningRuleRepository;
    private readonly IGenericRepository<MergeRules, Guid> _mergeRulesRepository;
    private readonly IGenericRepository<ExportSettings, Guid> _exportSettingsRepository;
    IBackgroundJobQueue<ProjectJobInfo> _jobQueue;

    private readonly ProjectBackgroundService _backgroundService;
    private readonly IServiceProvider _serviceProvider;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly CompletionTracker _completionTracker;
    private readonly IGenericRepository<StepJob, Guid> _stepJobRepository;

    private readonly ISecureParameterHandler _encryptionService;

    private const string TestServer = "(localdb)\\MSSQLLocalDB";
    private const string TestDatabase = "MatchLogicTest2";
    public ProjectServiceTests()
    {
        _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<ProjectService>();


        _completionTracker = new CompletionTracker();
        _cancellationTokenSource = new CancellationTokenSource();
        //_stepJobRepository = new GenericRepository<StepJob, Guid>(storeFactory);

        // Create service collection
        IServiceCollection services = new ServiceCollection();

        // 1. Build configuration (in-memory or from file)
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
            // Add any required settings for encryption, e.g.:
            { "Security:MasterKey", "TestMasterKey123456789012345678901234" }
            })
            .Build();

        // 2. Register IConfiguration
        services.AddSingleton<IConfiguration>(config);
        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, _dbJobPath);



        // Add event bus and job event publisher
        services.AddSingleton<IEventBus, TestEventBus>();
        // Add job event publisher that tracks completion
        services.AddScoped<IJobEventPublisher>(sp =>
            new TestJobEventPublisher(_completionTracker, new TestEventBus()));

        _serviceProvider = services.BuildServiceProvider();

        _dataStore = _serviceProvider.GetRequiredService<IDataStore>();
        _encryptionService = _serviceProvider.GetRequiredService<ISecureParameterHandler>();
        _projectService = _serviceProvider.GetRequiredService<IProjectService>();
        _projectRunRepository = _serviceProvider.GetRequiredService<IGenericRepository<ProjectRun, Guid>>();
        _projectRepository = _serviceProvider.GetRequiredService<IGenericRepository<Project, Guid>>();
        _stepJobRepository = _serviceProvider.GetRequiredService<IGenericRepository<StepJob, Guid>>();
        _completionTracker = new CompletionTracker();
        _dataSourceRepository = _serviceProvider.GetRequiredService<IGenericRepository<DataSource, Guid>>();
        _matchDefinitionRepository = _serviceProvider.GetRequiredService<IGenericRepository<MatchDefinition, Guid>>();
        _cleaningRuleRepository = _serviceProvider.GetRequiredService<IGenericRepository<CleaningRules, Guid>>();
        _mergeRulesRepository = _serviceProvider.GetRequiredService<IGenericRepository<MergeRules, Guid>>();

        var hostedServices = _serviceProvider.GetServices<IHostedService>().ToList();
        _backgroundService = hostedServices.OfType<ProjectBackgroundService>().FirstOrDefault();
        _jobQueue = _serviceProvider.GetRequiredService<IBackgroundJobQueue<ProjectJobInfo>>();
    }

    [Fact]
    public async Task CreateProject_WithValidInput_ShouldCreateAndReturnProject()
    {
        // Arrange
        var name = "Test Project";
        var description = "Test Description";
        var retentionRuns = 3;

        // Act
        var result = await _projectService.CreateProject(name, description, retentionRuns);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(name, result.Name);
        Assert.Equal(description, result.Description);
        Assert.Equal(retentionRuns, result.RetentionRuns);
        Assert.NotEqual(Guid.Empty, result.Id);

        var savedProject = (await _projectRepository.GetAllAsync(Constants.Collections.Projects)).FirstOrDefault();
        Assert.NotNull(savedProject);
        Assert.Equal(result.Id, savedProject.Id);

        _dataStore.Dispose();
    }

    [Fact]
    public async Task GetAllProjects_ShouldReturnAllProjects()
    {
        // Arrange
        var projects = new List<Project>
        {
            new() { Id = Guid.NewGuid(), Name = "Project 1" },
            new() { Id = Guid.NewGuid(), Name = "Project 2" }
        };

        foreach (var project in projects)
        {
            await _projectRepository.InsertAsync(project, Constants.Collections.Projects);
        }

        // Act
        var result = await _projectService.GetAllProjects();

        // Assert
        Assert.Equal(projects.Count, result.Count);
        foreach (var project in projects)
        {
            Assert.Contains(result, p => p.Id == project.Id && p.Name == project.Name);
        }

        _dataStore.Dispose();
    }

    [Fact]
    public async Task DeleteProject_ShouldDeleteProjectAndRelatedData()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Project to Delete"
        };

        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var projectRun = new ProjectRun
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
        };
        await _projectRunRepository.InsertAsync(projectRun, Constants.Collections.ProjectRuns);
        await _stepJobRepository.InsertAsync(new StepJob
        {

            StepData = new List<StepData>
                    {
                        new() { CollectionName = "Collection1" },
                        new() { CollectionName = "Collection2" }
                    }

        }, Constants.Collections.StepJobs);
        var matchDefinition = new Domain.Entities.MatchDefinition
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            Name = "Test Match"
        };
        await _matchDefinitionRepository.InsertAsync(matchDefinition, Constants.Collections.MatchDefinition);

        // Act
        await _projectService.DeleteProject(projectId);

        // Assert
        var remainingProject = await _projectRepository.GetAllAsync(Constants.Collections.Projects);
        Assert.Empty(remainingProject);

        var remainingRuns = await _projectRunRepository.QueryAsync(x => x.ProjectId == projectId, Constants.Collections.ProjectRuns);
        Assert.Empty(remainingRuns);

        var remainingMatches = await _matchDefinitionRepository.QueryAsync(x => x.ProjectId == projectId, Constants.Collections.MatchDefinition);
        Assert.Empty(remainingMatches);

        _dataStore.Dispose();
    }

    [Fact]
    public async Task GetAllProjects_WhenEmpty_ShouldReturnEmptyList()
    {
        // Act
        var result = await _projectService.GetAllProjects();

        // Assert
        Assert.Empty(result);

        _dataStore.Dispose();
    }

    [Fact]
    public async Task CreateProject_WithDefaultRetentionRuns_ShouldUseDefaultValue()
    {
        // Arrange
        var name = "Test Project";
        var description = "Test Description";

        // Act
        var result = await _projectService.CreateProject(name, description, 2);

        // Assert
        Assert.Equal(2, result.RetentionRuns);

        var savedProject = (await _projectRepository.GetAllAsync(Constants.Collections.Projects)).FirstOrDefault();
        Assert.NotNull(savedProject);
        Assert.Equal(2, savedProject.RetentionRuns);

        _dataStore.Dispose();
    }

    [Fact]
    public async Task UpdateProject_WithValidInput_ShouldUpdateAndReturnProject()
    {
        // Arrange
        var project = new Project
        {
            Id = Guid.NewGuid(),
            Name = "Original Name",
            Description = "Original Description",
            RetentionRuns = 2,
            CreatedAt = DateTime.Now.AddDays(-1),
            ModifiedAt = DateTime.Now.AddDays(-1)
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var newName = "Updated Name";
        var newDescription = "Updated Description";
        var newRetentionRuns = 3;

        // Act
        var result = await _projectService.UpdateProject(project.Id, newName, newDescription, newRetentionRuns);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(newName, result.Name);
        Assert.Equal(newDescription, result.Description);
        Assert.Equal(newRetentionRuns, result.RetentionRuns);
        Assert.True(result.ModifiedAt > project.ModifiedAt);

        var savedProject = await _projectRepository.GetByIdAsync(project.Id, Constants.Collections.Projects);
        Assert.Equal(newName, savedProject.Name);
        Assert.Equal(newDescription, savedProject.Description);
        Assert.Equal(newRetentionRuns, savedProject.RetentionRuns);
    }

    [Fact]
    public async Task UpdateProject_WithInvalidId_ShouldThrowException()
    {
        // Arrange
        var nonExistentId = Guid.NewGuid();

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _projectService.UpdateProject(nonExistentId, "New Name", "New Description", 3));
    }

    [Fact]
    public async Task AddDataSource_WithValidExcelDataSource_ShouldAddDataSourceAndCreateImportStep()
    {
        var filePath = Path.Combine(Path.GetTempPath(), "test3.xlsx");
        CreateTestExcelFile(filePath);
        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var excelDataSources = new List<DataSource>
    {
        new DataSource
        {
            Id = Guid.NewGuid(),
            Name = "Test Excel File",
            Type = DataSourceType.Excel,
            ConnectionDetails = new BaseConnectionInfo
            {
                Parameters = new Dictionary<string, string>
                {
                    { "FilePath", filePath },
                    { "HasHeaders", "true" }
                }
            },
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow
        }
    };

        _completionTracker.Reset();
        _completionTracker.ExpectJobs(1);
        // Act
        await _projectService.AddDataSource(projectId, excelDataSources);
        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = excelDataSources.Select(x => x.Id).ToArray();

        // Add import step
        stepInformation.Add(new StepConfiguration(
            StepType.Import,
            dataSourceIds
        ));
        var queuedRun = await _projectService.StartNewRun(projectId, stepInformation);

        // Wait for job completion with timeout
        var completionTask = await _completionTracker.WaitForCompletion(TimeSpan.FromSeconds(500));
        Assert.True(completionTask, "Job did not complete within timeout period");

        await Task.Delay(100);

        // Assert
        // 1. Verify DataSource was saved
        var savedDataSources = await _dataSourceRepository.QueryAsync(
            ds => ds.ProjectId == projectId,
            Constants.Collections.DataSources
        );

        Assert.Single(savedDataSources);
        var savedDataSource = savedDataSources.First();
        Assert.Equal(excelDataSources[0].Id, savedDataSource.Id);
        Assert.Equal(DataSourceType.Excel, savedDataSource.Type);
        Assert.Equal(projectId, savedDataSource.ProjectId);

        // 2. Verify ProjectRun was created
        var projectRuns = await _projectRunRepository.QueryAsync(
            r => r.ProjectId == projectId,
            Constants.Collections.ProjectRuns
        );


        Assert.Single(projectRuns);
        var run = projectRuns.First();
        Assert.Equal(RunStatus.Completed, run.Status);

        var runSteps = await _stepJobRepository.QueryAsync(s => s.RunId == run.Id, Constants.Collections.StepJobs);

        // 3. Verify Import Step was created
        Assert.Single(runSteps);
        var step = runSteps.First();
        Assert.Equal(StepType.Import, step.Type);
        Assert.Equal(RunStatus.Completed, step.Status);
        Assert.NotNull(step.Configuration);
        Assert.Equal(savedDataSource.Id, step.Configuration[Constants.FieldNames.DataSourceId]);

        // 4. Verify imported data
        Assert.Single(step.StepData);
        Assert.NotEmpty(step.StepData[0].CollectionName);
        var documents = await _dataStore.GetAllAsync<BsonDocument>(step.StepData[0].CollectionName);
        Assert.Equal(2, documents.Count);
        //Assert.Equal(savedDataSource.Name, step.Configuration["DataSourceName"]);
    }

    [Fact]
    public async Task AddDataSource_WithValidSQLDataSource_ShouldAddDataSourceAndCreateImportStep()
    {
        await SetupTestDatabase();

        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var excelDataSources = new List<DataSource>
    {
        new DataSource
        {
            Id = Guid.NewGuid(),
            Name = "Test SQL Server",
            Type = DataSourceType.SQLServer,
            ConnectionDetails = new BaseConnectionInfo
            {
                Parameters = new Dictionary<string, string>
                {
                    { "Server", TestServer },
                    { "Database", TestDatabase },
                    { "AuthType", "Windows" },
                    //{ "TableName", "Users" },
                    //{ "SchemaName", "dbo" },
                },
            },
            Configuration = new DataSourceConfiguration
                {
                    TableOrSheet = "dbo.Users",
                    ColumnMappings = new Dictionary<string, ColumnMapping>
                    {
                        { "Id", new ColumnMapping { SourceColumn = "Id", TargetColumn = "Id", Include = true } },
                        { "Name", new ColumnMapping { SourceColumn = "Name", TargetColumn = "FirstName", Include = true } },
                        { "Email", new ColumnMapping { SourceColumn = "Email", TargetColumn = "Email", Include = false } }
                    }
                },
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow
        }
    };

        _completionTracker.Reset();
        _completionTracker.ExpectJobs(1);
        // Act
        await _projectService.AddDataSource(projectId, excelDataSources);

        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = excelDataSources.Select(x => x.Id).ToArray();

        // Add import step
        stepInformation.Add(new StepConfiguration(
            StepType.Import,
            dataSourceIds
        ));

        var queuedRun = await _projectService.StartNewRun(projectId, stepInformation);
        // Wait for job completion with timeout
        var completionTask = await _completionTracker.WaitForCompletion(TimeSpan.FromSeconds(500));
        Assert.True(completionTask, "Job did not complete within timeout period");

        await Task.Delay(100);

        // Assert
        // 1. Verify DataSource was saved
        var savedDataSources = await _dataSourceRepository.QueryAsync(
            ds => ds.ProjectId == projectId,
            Constants.Collections.DataSources
        );

        Assert.Single(savedDataSources);
        var savedDataSource = savedDataSources.First();
        Assert.Equal(excelDataSources[0].Id, savedDataSource.Id);
        Assert.Equal(DataSourceType.SQLServer, savedDataSource.Type);
        Assert.Equal(projectId, savedDataSource.ProjectId);
        var orignalParameters = await _encryptionService.DecryptSensitiveParametersAsync(excelDataSources[0].ConnectionDetails.Parameters, savedDataSource.Id);
        var encryptedParameters = await _encryptionService.DecryptSensitiveParametersAsync(savedDataSource.ConnectionDetails.Parameters, savedDataSource.Id);
        foreach (var param in orignalParameters)
        {
            Assert.True(encryptedParameters.ContainsKey(param.Key));
            Assert.Equal(param.Value, encryptedParameters[param.Key]);
        }
        //Assert.Equal(projectId, savedDataSource.ConnectionDetails.Parameters);

        // 2. Verify ProjectRun was created
        var projectRuns = await _projectRunRepository.QueryAsync(
            r => r.ProjectId == projectId,
            Constants.Collections.ProjectRuns
        );


        Assert.Single(projectRuns);
        var run = projectRuns.First();
        Assert.Equal(RunStatus.Completed, run.Status);

        var runSteps = await _stepJobRepository.QueryAsync(s => s.RunId == run.Id, Constants.Collections.StepJobs);

        // 3. Verify Import Step was created
        Assert.Single(runSteps);
        var step = runSteps.First();
        Assert.Equal(StepType.Import, step.Type);
        Assert.Equal(RunStatus.Completed, step.Status);
        Assert.NotNull(step.Configuration);
        Assert.Equal(savedDataSource.Id, step.Configuration[Constants.FieldNames.DataSourceId]);
        //Assert.Equal(savedDataSource.Name, step.Configuration["DataSourceName"]);

        var collectionName = step.StepData.First().CollectionName;

        var collection = await _dataStore.GetAllAsync<BsonDocument>(collectionName);
        var importedData = collection;

        Assert.Equal(2, importedData.Count);
    }
    [Fact]
    public async Task AddDataSource_WithMultipleExcelDataSources_ShouldAddAllDataSourcesAndCreateImportSteps()
    {
        var file1 = Path.Combine(Path.GetTempPath(), "test4.xlsx");
        CreateTestExcelFile(file1);

        var file2 = Path.Combine(Path.GetTempPath(), "test5.xlsx");
        CreateTestExcelFile(file2);

        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var excelDataSources = new List<DataSource>
    {
        new DataSource
        {
            Id = Guid.NewGuid(),
            Name = "Excel File 1",
            Type = DataSourceType.Excel,
            ConnectionDetails = new BaseConnectionInfo
            {
                Parameters = new Dictionary<string, string>
                {
                    { "FilePath", file1 },
                    { "HasHeaders", "true" }
                }
            }
        },
        new DataSource
        {
            Id = Guid.NewGuid(),
            Name = "Excel File 2",
            Type = DataSourceType.Excel,
            ConnectionDetails = new BaseConnectionInfo
            {
                Parameters = new Dictionary<string, string>
                {
                    { "FilePath", file2 },
                    { "HasHeaders", "true" }
                }
            }
        }
    };

        _completionTracker.Reset();
        _completionTracker.ExpectJobs(2);
        // Act
        await _projectService.AddDataSource(projectId, excelDataSources);

        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = excelDataSources.Select(x => x.Id).ToArray();

        // Add import step
        stepInformation.Add(new StepConfiguration(
            StepType.Import,
            dataSourceIds
        ));

        var queuedRun = await _projectService.StartNewRun(projectId, stepInformation);

        var completionTask = await _completionTracker.WaitForCompletion(TimeSpan.FromSeconds(20));

        // Assert
        // 1. Verify DataSources were saved
        var savedDataSources = await _dataSourceRepository.QueryAsync(
            ds => ds.ProjectId == projectId,
            Constants.Collections.DataSources
        );

        Assert.Equal(2, savedDataSources.Count);
        Assert.All(savedDataSources, ds =>
        {
            Assert.Equal(DataSourceType.Excel, ds.Type);
            Assert.Equal(projectId, ds.ProjectId);
        });

        // 2. Verify ProjectRun was created
        var projectRuns = await _projectRunRepository.QueryAsync(
            r => r.ProjectId == projectId,
            Constants.Collections.ProjectRuns
        );

        Assert.Single(projectRuns);
        var run = projectRuns.First();
        Assert.Equal(RunStatus.Completed, run.Status);

        var runSteps = await _stepJobRepository.QueryAsync(s => s.RunId == run.Id, Constants.Collections.StepJobs);

        // 3. Verify Import Steps were created
        Assert.Equal(2, runSteps.Count);
        Assert.All(runSteps, step =>
        {
            Assert.Equal(StepType.Import, step.Type);
            Console.WriteLine(step.Status);
            Assert.True(step.Status == RunStatus.Completed);
            Assert.NotNull(step.Configuration);
            Assert.Contains(excelDataSources, ds =>
                ds.Id.Equals(step.Configuration[Constants.FieldNames.DataSourceId]));
        });
    }
    [Fact]
    public async Task AddDataSource_WithNoDataSources_ShouldNotCreateRun()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var emptyDataSources = new List<DataSource>();

        // Act
        await _projectService.AddDataSource(projectId, emptyDataSources);

        // Assert
        var projectRuns = await _projectRunRepository.QueryAsync(
            r => r.ProjectId == projectId,
            Constants.Collections.ProjectRuns
        );

        Assert.Empty(projectRuns);
    }


    [Fact]
    public async Task RemoveDataSource_ShouldDeleteDataSourceAndRelatedData()
    {
        var filePath = Path.Combine(Path.GetTempPath(), "test6.xlsx");
        CreateTestExcelFile(filePath);
        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var excelDataSources = new List<DataSource>
    {
        new DataSource
        {
            Id = Guid.NewGuid(),
            Name = "Test Excel File",
            Type = DataSourceType.Excel,
            ConnectionDetails = new BaseConnectionInfo
            {
                Parameters = new Dictionary<string, string>
                {
                    { "FilePath", filePath },
                    { "HasHeaders", "true" }
                }
            },
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow
        }
    };

        _completionTracker.Reset();
        _completionTracker.ExpectJobs(1);
        // Act
        await _projectService.AddDataSource(projectId, excelDataSources);
        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = excelDataSources.Select(x => x.Id).ToArray();

        // Add import step
        stepInformation.Add(new StepConfiguration(
            StepType.Import,
            dataSourceIds
        ));
        var projectRun = await _projectService.StartNewRun(projectId, stepInformation);

        // Wait for job completion with timeout
        var completionTask = await _completionTracker.WaitForCompletion(TimeSpan.FromSeconds(500));
        Assert.True(completionTask, "Job did not complete within timeout period");

        await Task.Delay(100);

        var stepJob = await _stepJobRepository.QueryAsync(x => x.RunId == projectRun.Id, Constants.Collections.StepJobs);

        var collectionName = stepJob[0].StepData[0].CollectionName;
        var dataSource = excelDataSources[0];
        // Act
        await _projectService.RemoveDataSource(projectId, dataSource.Id);

        // Assert
        // 1. Verify data source is deleted
        var dataSourceExists = await _dataSourceRepository.GetByIdAsync(dataSource.Id, Constants.Collections.DataSources);
        Assert.Null(dataSourceExists);

        // 2. Verify step job is deleted
        var stepJobExists = await _stepJobRepository.GetByIdAsync(stepJob[0].Id, Constants.Collections.StepJobs);
        Assert.Null(stepJobExists);

        // 3. Verify collection is deleted
        var collection = await _dataStore.GetAllAsync<BsonDocument>(collectionName);
        Assert.Empty(collection);

        // 4. Verify project run is deleted (since it only had import steps)
        var projectRunExists = await _projectRunRepository.GetByIdAsync(projectRun.Id, Constants.Collections.ProjectRuns);
        Assert.Null(projectRunExists);
    }

    [Fact]
    public async Task RemoveDataSource_WithRunHavingOtherSteps_ShouldNotDeleteRun()
    {
        var filePath = Path.Combine(Path.GetTempPath(), "test7.xlsx");
        CreateTestExcelFile(filePath);
        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var excelDataSources = new List<DataSource>
    {
        new DataSource
        {
            Id = Guid.NewGuid(),
            Name = "Test Excel File",
            Type = DataSourceType.Excel,
            ConnectionDetails = new BaseConnectionInfo
            {
                Parameters = new Dictionary<string, string>
                {
                    { "FilePath", filePath },
                    { "HasHeaders", "true" }
                }
            },
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow
        }
    };

        _completionTracker.Reset();
        _completionTracker.ExpectJobs(1);
        // Act
        await _projectService.AddDataSource(projectId, excelDataSources);
        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = excelDataSources.Select(x => x.Id).ToArray();

        // Add import step
        stepInformation.Add(new StepConfiguration(
            StepType.Import,
            dataSourceIds
        ));


        var projectRun = await _projectService.StartNewRun(projectId, stepInformation);

        // Create another step job of different type in the same run
        var matchStepJob = new StepJob
        {
            Id = Guid.NewGuid(),
            RunId = projectRun.Id,
            Type = StepType.Match,
            Status = RunStatus.Completed
        };
        await _stepJobRepository.InsertAsync(matchStepJob, Constants.Collections.StepJobs);

        // Wait for job completion with timeout
        var completionTask = await _completionTracker.WaitForCompletion(TimeSpan.FromSeconds(500));
        Assert.True(completionTask, "Job did not complete within timeout period");

        await Task.Delay(100);

        var stepJob = await _stepJobRepository.QueryAsync(x => x.RunId == projectRun.Id && x.Type == StepType.Import, Constants.Collections.StepJobs);

        var collectionName = stepJob[0].StepData[0].CollectionName;
        var dataSource = excelDataSources[0];

        // Act
        await _projectService.RemoveDataSource(projectId, dataSource.Id);

        // Assert
        // 1. Verify data source is deleted
        var dataSourceExists = await _dataSourceRepository.GetByIdAsync(dataSource.Id, Constants.Collections.DataSources);
        Assert.Null(dataSourceExists);

        // 2. Verify import step job is deleted
        var importStepJobExists = await _stepJobRepository.GetByIdAsync(stepJob[0].Id, Constants.Collections.StepJobs);
        Assert.Null(importStepJobExists);

        // 3. Verify collection is deleted
        var collection = await _dataStore.GetAllAsync<BsonDocument>(collectionName);
        Assert.Empty(collection);

        // 4. Verify project run is NOT deleted (since it had non-import steps)
        var projectRunExists = await _projectRunRepository.GetByIdAsync(projectRun.Id, Constants.Collections.ProjectRuns);
        Assert.NotNull(projectRunExists);

        // 5. Verify the match step job still exists
        var matchStepJobExists = await _stepJobRepository.GetByIdAsync(matchStepJob.Id, Constants.Collections.StepJobs);
        Assert.NotNull(matchStepJobExists);
    }

    [Fact]
    public async Task RemoveDataSource_WithNoRelatedStepJobs_ShouldOnlyDeleteDataSource()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        // Create a data source
        var dataSource = new DataSource
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            Name = "Test Data Source",
            Type = DataSourceType.Excel
        };
        await _dataSourceRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act
        await _projectService.RemoveDataSource(projectId, dataSource.Id);

        // Assert
        // Verify data source is deleted
        var dataSourceExists = await _dataSourceRepository.GetByIdAsync(dataSource.Id, Constants.Collections.DataSources);
        Assert.Null(dataSourceExists);

        // Make sure no exceptions were thrown when no step jobs were found
    }

    private void CreateTestExcelFile(string testExcelPath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var sheet2 = workbook.CreateSheet("Sheet2");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("Name");
        headerRow.CreateCell(1).SetCellValue("Age");

        // Add test data
        var row1 = sheet.CreateRow(1);
        row1.CreateCell(0).SetCellValue("John Doe");
        row1.CreateCell(1).SetCellValue(30);

        var row2 = sheet.CreateRow(2);
        row2.CreateCell(0).SetCellValue("Jane Smith");
        row2.CreateCell(1).SetCellValue(25);

        using var fileStream = File.Create(testExcelPath);
        workbook.Write(fileStream);
    }

    private async Task SetupTestDatabase()
    {
        using var masterConnection = new Microsoft.Data.SqlClient.SqlConnection(
            $"Server={TestServer};Database=master;Integrated Security=True");
        await masterConnection.OpenAsync();

        // Create test database
        var createDbCmd = $@"
                IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{TestDatabase}')
                BEGIN
                    CREATE DATABASE {TestDatabase}
                END";
        using (var commanda = new Microsoft.Data.SqlClient.SqlCommand(createDbCmd, masterConnection))
        {
            await commanda.ExecuteNonQueryAsync();
        }

        // Create test tables
        using var testConnection = new Microsoft.Data.SqlClient.SqlConnection(
            $"Server={TestServer};Database={TestDatabase};Integrated Security=True");
        await testConnection.OpenAsync();

        var createTableCmd = @"
                IF OBJECT_ID('dbo.Users', 'U') IS NULL
                CREATE TABLE dbo.Users (
                    Id INT PRIMARY KEY IDENTITY(1,1),
                    Name NVARCHAR(100) NULL,
                    Email NVARCHAR(255) NOT NULL
                )
                
                IF NOT EXISTS (SELECT * FROM dbo.Users)
                BEGIN
                    INSERT INTO dbo.Users VALUES
                    ('John Doe', 'john@example.com'),
                    ('Jane Smith', 'jane@example.com')
                END";

        using var command = new Microsoft.Data.SqlClient.SqlCommand(createTableCmd, testConnection);
        await command.ExecuteNonQueryAsync();
    }
    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        _backgroundService.StopAsync(_cancellationTokenSource.Token).Wait();
        _cancellationTokenSource.Dispose();

        if (_dataStore != null)
        {
            _dataStore.Dispose();
        }

        using var connection = new Microsoft.Data.SqlClient.SqlConnection(
                $"Server={TestServer};Database=master;Integrated Security=True");
        connection.Open();

        var killSessionsCmd = $@"
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{TestDatabase}')
                BEGIN
                    ALTER DATABASE {TestDatabase} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE {TestDatabase};
                END";

        using var command = new Microsoft.Data.SqlClient.SqlCommand(killSessionsCmd, connection);
        command.ExecuteNonQuery();
    }

    public async Task InitializeAsync()
    {
        await _backgroundService.StartAsync(_cancellationTokenSource.Token);
        await Task.Delay(100);
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
        //throw new NotImplementedException();
    }
}
public class CompletionTracker
{
    private TaskCompletionSource<bool> _completionSource;
    private int _expectedJobCount;
    private int _completedJobCount;
    private readonly object _lock = new();

    public CompletionTracker()
    {
        _completionSource = new TaskCompletionSource<bool>();
    }

    public void Reset()
    {
        lock (_lock)
        {
            _expectedJobCount = 0;
            _completedJobCount = 0;
            _completionSource = new TaskCompletionSource<bool>();
        }
    }

    public void ExpectJobs(int count)
    {
        lock (_lock)
        {
            _expectedJobCount = count;
            _completedJobCount = 0;
            _completionSource = new TaskCompletionSource<bool>();
        }
    }

    public void JobCompleted()
    {
        lock (_lock)
        {
            _completedJobCount++;
            if (_completedJobCount >= _expectedJobCount)
            {
                _completionSource.TrySetResult(true);
            }
        }
    }

    public async Task<bool> WaitForCompletion(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await _completionSource.Task.WaitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public bool IsComplete()
    {
        lock (_lock)
        {
            return _completedJobCount >= _expectedJobCount;
        }
    }
}
// First, create a test implementation of IEventBus
public class TestEventBus : IEventBus
{
    public TestEventBus()
    {
    }

    public Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : BaseEvent
    {
        (@event as JobEvent).Message.Contains($"Completed");

        return Task.CompletedTask;
    }
}

public class TestJobEventPublisher : JobEventPublisher, IJobEventPublisher
{
    private readonly CompletionTracker _completionTracker;
    private readonly ILogger<TestJobEventPublisher> _logger;

    public TestJobEventPublisher(CompletionTracker completionTracker, IEventBus eventBus) : base(eventBus)
    {
        _completionTracker = completionTracker;
        _logger = LoggerFactory.Create(builder => builder.AddConsole())
            .CreateLogger<TestJobEventPublisher>();
    }

    public new Task PublishJobStartedAsync(Guid jobId, int totalSteps, string message = null, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Job {JobId} started: {Message}", jobId, message);
        return Task.CompletedTask;
    }


    public new Task PublishJobCompletedAsync(Guid jobId, string message = null, FlowStatistics statistics = null, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Job {JobId} completed: {Message}", jobId, message);
        _completionTracker.JobCompleted();
        return Task.CompletedTask;
    }

    public new Task PublishJobFailedAsync(Guid jobId, string error, FlowStatistics statistics = null, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Job {JobId} completed: {Message}", jobId, "");
        _completionTracker.JobCompleted();
        return Task.CompletedTask;
    }
}