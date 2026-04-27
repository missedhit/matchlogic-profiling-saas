using Ardalis.Result;
using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Features.Transform;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Project.Commands;
using MatchLogic.Infrastructure.Project.DataProfiling;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class DataExportCommandTest
{
    private readonly string _dbPath;
    private ILogger<DataExportCommand> _logger;
    private DataExportCommand _command;
    private readonly IServiceProvider _serviceProvider;
    private readonly CompletionTracker _completionTracker;

    private readonly IColumnFilter _columnFilter;
    private readonly IDataStore _dataStore;
    private readonly IExportDataWriterStrategyFactory exportDataWriterStrategy;
    private readonly IDataTransformerFactory dataTransformerFactory;

    private readonly IProjectService _projectService;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepRespository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<DataExportOptions, Guid> _dataExportRepository;
    private readonly IAutoMappingService _autoMappingService;
    private readonly ICommandContext _commandContext;
    private readonly IMigrationService _migrationService;
    private readonly string _testExcelPath;

    private readonly Dictionary<string, string> exportTypes = new()
     {
        { "profile", "profile_{0}" },
        { "profileStats", "profile_{0}_RowReferenceDocument" },
        { "cleanse", "cleanse_{0}" },
        { "pairs", "pairs_{0}" },
        { "groups", "groups_{0}" },
        { "finalExport", "finalExport_{0}" }
    };

    public DataExportCommandTest()
    {
        _testExcelPath = Path.Combine(Path.GetTempPath(), "testProfile.xlsx");

        //_dbPath = Path.GetTempFileName();
        _dbPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData)
            , "MatchLogicApi"
                , "Database"
                , "MatchLogic.db");
        _logger = new NullLogger<DataExportCommand>();
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
        services.AddLogging(builder => builder.AddConsole());
        // 2. Register IConfiguration
        services.AddSingleton<IConfiguration>(config);
        services.AddApplicationSetup(_dbPath);

        services.AddLogging(builder => builder.AddConsole());

        // Add event bus and job event publisher
        services.AddSingleton<IEventBus, TestEventBus>();
        // Add job event publisher that tracks completion
        services.AddScoped<IJobEventPublisher>(sp =>
            new TestJobEventPublisher(_completionTracker, new TestEventBus()));

        _serviceProvider = services.BuildServiceProvider();

        _dataStore = _serviceProvider.GetService<IDataStore>();
        _columnFilter = _serviceProvider.GetService<IColumnFilter>();

        //_cleaningRulesRepository = _serviceProvider.GetService<IGenericRepository<EnhancedCleaningRules, Guid>>();

        _jobEventPublisher = _serviceProvider.GetService<IJobEventPublisher>();
        _autoMappingService = _serviceProvider.GetService<IAutoMappingService>();

        _dataSourceRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        _projectRunRepository = _serviceProvider.GetService<IGenericRepository<ProjectRun, Guid>>();
        _stepRespository = _serviceProvider.GetService<IGenericRepository<StepJob, Guid>>();
        //_cleansingModule = _serviceProvider.GetService<ICleansingModule>();
        _projectService = _serviceProvider.GetService<IProjectService>();
        exportDataWriterStrategy = _serviceProvider.GetService<IExportDataWriterStrategyFactory>();
        dataTransformerFactory = _serviceProvider.GetService<IDataTransformerFactory>();
        _dataExportRepository = _serviceProvider.GetService<IGenericRepository<DataExportOptions, Guid>>();

        _migrationService = _serviceProvider.GetService<IMigrationService>();

        //CreateTestExcelFile();
    }

    [Fact]
    public async Task ExecCommandAsync_WithValidInputs_CallsProfilerAndReturnsCorrectStepData()
    {
        //await _migrationService.InitializeDatabase();
        var _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        var _genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        var _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        var _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        var logger = new NullLogger<DataImportCommand>();

        var _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        var _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();

   //     DataImportCommand dataImportCommand = new DataImportCommand(
   //    _dataSourceService,
   //    _projectService,
   //    _jobEventPublisher,
   //    _genericRepository,
   //    _recordHasher,
   //    logger,
   //    _dataStore,
   //    _projectRunRepository,
   //    _stepRespository,
   //    _connectionBuilder,
   //    _columnFilter,
   //    _secureParameterHandler
   //);

        // Arrange
        var dataSourceId = Guid.NewGuid();
        var stepId = Guid.NewGuid();
        //var context = new Mock<ICommandContext>();
        var runId = Guid.NewGuid();
        //var projectId = Guid.NewGuid();
        var projectId = Guid.Parse("68a910a1-dc92-4187-9945-e5f84059d6c2");
        var documentId = Guid.Parse("68a910a1-dc92-4187-9945-e5f84059d6c2");

        

        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };

        #region Import DataSource
        /*
         await _projectRepository.InsertAsync(project, Constants.Collections.Projects)
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

        await _stepRespository.InsertAsync(stepI, Constants.Collections.StepJobs);*/
        #endregion

        var dataExportOptions = await SetupDataOptions(documentId, new DataExportOptions
        {
            ProjectId = projectId,
            TableName = "TestTable_groups",
            SchemaName = "dbo",
            //TransformerType = "flatten",
            //TransformerType = "group_flatten",
            //TransformerType = "pair_flatten",
            //ViewType = "finalExport",
            ViewType = "groups",
            BulkCopy = false,
            ExportOnlyColumnsAndRows = false,
            ForceDBDataTypeFromInput = false,
            ColumnMappings = [],
            ConnectionConfig = new BaseConnectionInfo()
            {
                Type = DataSourceType.SQLServer,
                Parameters = new Dictionary<string, string>
                {
                    { "Server", "DESKTOP-BK4UHTV\\SQLEXPRESS" },
                    { "Database", "Segility_DB" },
                    { "AuthType", "Windows" },
                    { "Username", "sadmin" },
                    { "Password", "admin123" },
                    { "TrustServerCertificate", "true" }
                }
            }
        });
        stepId = Guid.NewGuid();
        var jobId = Guid.NewGuid();

        var step = new StepJob
        {
            Id = stepId,
            Type = StepType.Export,
            DataSourceId = dataSourceId,
            RunId = runId,
            Configuration = new Dictionary<string, object>
            {
                [Constants.FieldNames.ExportId] = dataExportOptions.Id
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
        await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);

        var context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        //var dataSource = CreateExcelDataSource(dataSourceId);
        //var encryptedParameters = await _secureParameterHandler.EncryptSensitiveParametersAsync(dataSource.ConnectionDetails.Parameters, dataSourceId);
        //dataSource.ConnectionDetails.Parameters = encryptedParameters;
        //await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act
        //await dataImportCommand.ExecuteAsync(context, step);

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
    private DataExportCommand CreateCommand()
    {
        return new DataExportCommand(
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRespository,
            _dataSourceRepository,
            _dataExportRepository,
            _logger,
            _columnFilter,
            _dataStore,
            exportDataWriterStrategy,
            dataTransformerFactory,
            _autoMappingService
            );
    }
    private async Task<DataExportOptions> SetupDataOptions(Guid DocumentId, DataExportOptions request)
    {
        if (!exportTypes.TryGetValue(request.ViewType, out var _arrayIndexFormat))
        {
            throw new ArgumentException($"Invalid TabName: {request.ViewType}");
        }

        var collectionName = string.Format(_arrayIndexFormat, GuidCollectionNameConverter.ToValidCollectionName(DocumentId));
        var dataExportOptions = new DataExportOptions
        {
            Id = Guid.NewGuid(),
            ProjectId = request.ProjectId,
            TableName = request.TableName,
            SchemaName = request.SchemaName,
            //TransformerType = request.TransformerType ?? "none",
            ViewType = request.ViewType,
            BulkCopy = request.BulkCopy,
            ExportOnlyColumnsAndRows = request.ExportOnlyColumnsAndRows,
            ForceDBDataTypeFromInput = request.ForceDBDataTypeFromInput,
            ColumnMappings = request.ColumnMappings ?? [],
            ConnectionConfig = request.ConnectionConfig,
            CollectionName = collectionName,
            CreatedDate = DateTime.UtcNow
        };


        await _dataExportRepository.InsertAsync(dataExportOptions, Constants.Collections.ExportSettings);

        return dataExportOptions;
        //var stepInformation = new List<StepConfiguration>
        //{            
        //    // Add Export result
        //    new(StepType.Export,
        //    new Dictionary<string, object> {
        //        { "ExportId", dataExportOptions.Id }
        //    },
        //    dataSourceIds:[dataExportOptions.Id])
        //};

        //var queuedRun = await _projectService.StartNewRun(ProjectId, stepInformation);
    }
}
