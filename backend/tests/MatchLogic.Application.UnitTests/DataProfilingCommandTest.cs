using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using Moq;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Project.Commands;
using MatchLogic.Infrastructure.Project.DataProfiling;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPOI.XSSF.UserModel;

namespace MatchLogic.Application.UnitTests;

public class DataProfilingCommandTest
{
    private readonly string _dbPath;
    private ILogger<DataProfilingCommand> _logger;
    private DataProfilingCommand _command;
    private readonly IServiceProvider _serviceProvider;
    private readonly CompletionTracker _completionTracker;

    private readonly IDataStore _dataStore;
    private readonly IDataProfiler _dataProfiler;
    private readonly IProjectService _projectService;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepRespository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly ICommandContext _commandContext;
    private readonly IMigrationService _migrationService;
    private readonly string _testExcelPath;
    private IGenericRepository<DataSnapshot, Guid> _snapshotRepo;
    private IGenericRepository<FileImport, Guid> _fileImportRepo;

    private ISchemaValidationService _schemaValidationService;

    public DataProfilingCommandTest()
    {
        _testExcelPath = Path.Combine(Path.GetTempPath(), "testProfile.xlsx");

        _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();
        _logger = new NullLogger<DataProfilingCommand>();
        _completionTracker = new CompletionTracker();

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

        _dataStore = _serviceProvider.GetService<IDataStore>();

        //_cleaningRulesRepository = _serviceProvider.GetService<IGenericRepository<EnhancedCleaningRules, Guid>>();

        _jobEventPublisher = _serviceProvider.GetService<IJobEventPublisher>();

        _dataSourceRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        _projectRunRepository = _serviceProvider.GetService<IGenericRepository<ProjectRun, Guid>>();
        _stepRespository = _serviceProvider.GetService<IGenericRepository<StepJob, Guid>>();
        //_cleansingModule = _serviceProvider.GetService<ICleansingModule>();
        _projectService = _serviceProvider.GetService<IProjectService>();
        _dataProfiler = _serviceProvider.GetService<IDataProfiler>();

        _migrationService = _serviceProvider.GetService<IMigrationService>();

        _schemaValidationService = _serviceProvider.GetRequiredService<ISchemaValidationService>();
        _snapshotRepo = _serviceProvider.GetService<IGenericRepository<DataSnapshot, Guid>>();
        _fileImportRepo = _serviceProvider.GetService<IGenericRepository<FileImport, Guid>>();

        CreateTestExcelFile();

    }

    [Fact]
    public async Task ExecCommandAsync_WithValidInputs_CallsProfilerAndReturnsCorrectStepData()
    {
        await _migrationService.InitializeDatabase();
        var _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        var _genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        var _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        var _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        //var _dataReaderFactory = _serviceProvider.GetService<IDataReaderFactory>();
        //var _strategyFactory = _serviceProvider.GetService<IParameterStrategyFactory>();
        var logger = new NullLogger<DataImportCommand>();

        var _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        var _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();

        DataImportCommand dataImportCommand = new DataImportCommand(
       _dataSourceService,
       _projectService,
       _jobEventPublisher,
       _genericRepository,
       _recordHasher,
       logger,
       _dataStore,
       _projectRunRepository,
       _stepRespository,
       _connectionBuilder,
       _columnFilter,
       _secureParameterHandler,
       Mock.Of<IOAuthTokenService>(),
           new RemoteFileConnectorFactory(),
           _snapshotRepo,
           _fileImportRepo,
           _schemaValidationService
   );

        // Arrange
        var dataSourceId = Guid.NewGuid();
        var stepId = Guid.NewGuid();
        //var context = new Mock<ICommandContext>();
        var runId = Guid.NewGuid();
        var projectId = Guid.NewGuid();

        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };

        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var stepI = new StepJob
        {
            Id = stepId,
            Type = StepType.Import,
            RunId = runId,
            DataSourceId = dataSourceId,
            Configuration = new Dictionary<string, object>
        {
            { "DataSourceId", dataSourceId }
        }
        };
        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
            //Steps = new List<StepJob> { step }
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(stepI, Constants.Collections.StepJobs);

        stepId = Guid.NewGuid();
        var jobId = Guid.NewGuid();

        var step = new StepJob
        {
            Id = stepId,
            Type = StepType.Profile,
            DataSourceId = dataSourceId,
            RunId = runId,
            Configuration = new Dictionary<string, object>
            {
                [Constants.FieldNames.DataSourceId] = dataSourceId
            }
        };

        await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);

        var context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        var dataSource = CreateExcelDataSource(dataSourceId);
        var encryptedParameters = await _secureParameterHandler.EncryptSensitiveParametersAsync(dataSource.ConnectionDetails.Parameters, dataSourceId);
        dataSource.ConnectionDetails.Parameters = encryptedParameters;
        await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act
        await dataImportCommand.ExecuteAsync(context, stepI);

        var run = await _projectRunRepository.GetByIdAsync(runId, Constants.Collections.ProjectRuns);

        // Arrange
        var command = CreateCommand();

        context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);

        // Act
        await command.ExecuteAsync(context, step);

        var stepJob = await _stepRespository.GetByIdAsync(stepId, Constants.Collections.StepJobs);
        var profileCollectionName = stepJob.StepData.First().CollectionName;

        var collectionData = await _dataStore.GetStreamFromTempCollection(profileCollectionName, CancellationToken.None).ToListAsync();

        Assert.Equal(4, Convert.ToInt32(collectionData.First()["TotalRecords"]));
        Assert.Equal(dataSourceId, collectionData.First()["DataSourceId"]);
    }
    #region Helper Methods

    private DataProfilingCommand CreateCommand()
    {
        return new DataProfilingCommand(
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRespository,
            _dataStore,
            _dataProfiler,
            _dataSourceRepository,
            _logger);
    }

    private void CreateTestExcelFile()
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var sheet2 = workbook.CreateSheet("Sheet2");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("Name");
        headerRow.CreateCell(1).SetCellValue("Age");
        headerRow.CreateCell(2).SetCellValue("Email");

        // Add test data
        var row1 = sheet.CreateRow(1);
        row1.CreateCell(0).SetCellValue("John Doe");
        row1.CreateCell(1).SetCellValue(25);
        row1.CreateCell(2).SetCellValue("john.doe@example.com");

        var row2 = sheet.CreateRow(2);
        row2.CreateCell(0).SetCellValue("Jane Smith");
        row2.CreateCell(1).SetCellValue(30);
        row2.CreateCell(2).SetCellValue("jane.smith@example.com");

        var row3 = sheet.CreateRow(3);
        row3.CreateCell(0).SetCellValue("Jane Smith");
        row3.CreateCell(1).SetCellValue(35);
        row3.CreateCell(2).SetCellValue("jane.smith_35@example.com");

        var row4 = sheet.CreateRow(4);
        row4.CreateCell(0).SetCellValue("Jane Smith");
        row4.CreateCell(1).SetCellValue(40);
        row4.CreateCell(2).SetCellValue("jane.smith@example.com");

        using var fileStream = File.Create(_testExcelPath);
        workbook.Write(fileStream);
    }
    private DataSource CreateExcelDataSource(Guid dataSourceId)
    {
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", _testExcelPath },
                { "HasHeaders", "true" }
            }
        };
        return new DataSource
        {
            Id = dataSourceId,
            Type = DataSourceType.Excel,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "Sheet1",
                ColumnMappings = new Dictionary<string, MatchLogic.Domain.Project.ColumnMapping>
                {
                    { "Name", new MatchLogic.Domain.Project.ColumnMapping { SourceColumn = "Name", TargetColumn = "Name", Include = true } },
                    { "Age", new MatchLogic.Domain.Project.ColumnMapping { SourceColumn = "Age", TargetColumn = "Age", Include = true } },
                    { "Email", new MatchLogic.Domain.Project.ColumnMapping { SourceColumn = "Email", TargetColumn = "Email", Include = true } }
                }
            }
        };
    }
    #endregion
}
