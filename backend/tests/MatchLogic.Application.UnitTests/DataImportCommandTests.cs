using MatchLogic.Application.Common;
using MatchLogic.Application.EventHandlers;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Project.Commands;
using MatchLogic.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NPOI.XSSF.UserModel;
using System;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Application.UnitTests;
public class DataImportCommandTests
{
    private readonly string _dbPath;
    private IDataSourceService _dataSourceService;
    private ISecureParameterHandler _secureParameterHandler;
    private IProjectService _projectService;
    private IJobEventPublisher _jobEventPublisher;
    private IGenericRepository<DomainDataSource, Guid> _genericRepository;
    private IRecordHasher _recordHasher;
    private IDataStore _dataStore;
    private ILogger<DataImportCommand> _logger;
    private ILogger<ConnectionBuilder> _connlogger;
    private IGenericRepository<ProjectRun, Guid> _projectRunRespository;
    private IGenericRepository<StepJob, Guid> _stepRespository;
    private IGenericRepository<DataSnapshot, Guid> _snapshotRepo;
    private IGenericRepository<FileImport, Guid> _fileImportRepo;
    private readonly string _testExcelPath;
    private DataImportCommand _command;
    private IColumnFilter _columnFilter;
    private IConnectionBuilder _connectionBuilder;
    private ISchemaValidationService _schemaValidationService;
    private readonly IServiceProvider _serviceProvider;
    public DataImportCommandTests()
    {
        _dbPath = Path.GetTempFileName();
        var _jobdbPath = Path.GetTempFileName();
        _logger = new NullLogger<DataImportCommand>();
        IServiceCollection services = new ServiceCollection();
        _testExcelPath = Path.Combine(Path.GetTempPath(), "test2.xlsx");

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
            // Add any required settings for encryption, e.g.:
            { "Security:MasterKey", "TestMasterKey123456789012345678901234" }
            })
            .Build();

        services.AddSingleton<IConfiguration>(config);
        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, _jobdbPath);

        services.AddMediatR(cfg => {
            cfg.RegisterServicesFromAssembly(typeof(DatabaseUpdateEventHandler).Assembly);
        });
        _serviceProvider = services.BuildServiceProvider();

        var encryptLogger = _serviceProvider.GetService<IEncryptionService>();
        _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
        _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        _projectService = _serviceProvider.GetService<IProjectService>();
        _jobEventPublisher = _serviceProvider.GetService<IJobEventPublisher>();
        _genericRepository = _serviceProvider.GetService<IGenericRepository<DomainDataSource, Guid>>();
        _projectRunRespository = _serviceProvider.GetService<IGenericRepository<ProjectRun, Guid>>();
        _stepRespository = _serviceProvider.GetService<IGenericRepository<StepJob, Guid>>();
        _snapshotRepo = _serviceProvider.GetService<IGenericRepository<DataSnapshot, Guid>>();
        _fileImportRepo = _serviceProvider.GetService<IGenericRepository<FileImport, Guid>>();
        _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        _schemaValidationService = _serviceProvider.GetRequiredService<ISchemaValidationService>();
        _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        _dataStore = _serviceProvider.GetService<IDataStore>();
        CreateTestExcelFile();
    }
    [Fact]
    public async Task ExecCommandAsync_ValidDataSource_ReturnsStepData()
    {
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();
        _command = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _genericRepository,
            _recordHasher,
            _logger,
            _dataStore,
            _projectRunRespository,
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
        var step = new StepJob
        {
            Id = stepId,
            Type = StepType.Import,
            RunId = runId,
            DataSourceId = dataSourceId,

        };
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };

        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        await _projectRunRespository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
            //Steps = new List<StepJob> { step }
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);
        var context = new CommandContext(runId, projectId, stepId, _projectRunRespository, _stepRespository);
        var dataSource = CreateExcelDataSource(dataSourceId);
        var encryptedParameters = await _secureParameterHandler.EncryptSensitiveParametersAsync(dataSource.ConnectionDetails.Parameters, dataSourceId);
        dataSource.ConnectionDetails.Parameters = encryptedParameters;
        await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);


        //var strategy = new Mock<IDataSourceParameterStrategy>();
        //strategy.Setup(s => s.BuildParameters(It.IsAny<DataSource>()))
        //    .Returns(new DataSourceParameters { ConnectionPath = "test", Query = "test" });        


        //context.Setup(c => c.CreateCollectionName(stepId, It.IsAny<string>()))
        //    .Returns($"import_{dataSourceId}");

        // Act
        await _command.ExecuteAsync(context, step);

        var run = await _genericRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);
        var result = await _stepRespository.GetByIdAsync(stepId, Constants.Collections.StepJobs);

        var collectionName = DatasetNames.SnapshotRows(run.ActiveSnapshotId.Value);

        //// Assert
        Assert.NotNull(result);
        Assert.Equal(result.StepData.FirstOrDefault().StepJobId, stepId);
        //Assert.Equal(result.StepData.FirstOrDefault().CollectionName, $"import_{dataSourceId}");

        var collectionData = await _dataStore.GetStreamFromTempCollection(result.StepData.FirstOrDefault().CollectionName, CancellationToken.None).ToListAsync();

        Assert.Equal(2, collectionData.Count);
        Assert.Equal("John Doe", collectionData.First()["Name"]);
        Assert.Equal(30, Convert.ToInt32(collectionData.First()["Age"]));
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

        // Add test data
        var row1 = sheet.CreateRow(1);
        row1.CreateCell(0).SetCellValue("John Doe");
        row1.CreateCell(1).SetCellValue(30);

        var row2 = sheet.CreateRow(2);
        row2.CreateCell(0).SetCellValue("Jane Smith");
        row2.CreateCell(1).SetCellValue(25);

        using var fileStream = File.Create(_testExcelPath);
        workbook.Write(fileStream);
    }
    // Add this new test method inside your existing DataImportCommandTests class.
    // No changes required to your existing import test.

    [Fact]
    public async Task ExecuteAsync_Refresh_WithFileImportId_CreatesNewSnapshot_UpdatesActiveSnapshot_AndLoadsNewData()
    {
        // Arrange
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();

        _command = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _genericRepository,
            _recordHasher,
            _logger,
            _dataStore,
            _projectRunRespository,
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

        var dataSourceId = Guid.NewGuid();
        var projectId = Guid.NewGuid();

        // Create project
        await _projectRepository.InsertAsync(new Project
        {
            Id = projectId,
            Name = "Refresh Test Project",
            Description = "Refresh Test Description"
        }, Constants.Collections.Projects);

        // Create datasource pointing to initial excel file (_testExcelPath)
        var dataSource = CreateExcelDataSource(dataSourceId);
        var encryptedParameters = await _secureParameterHandler.EncryptSensitiveParametersAsync(
            dataSource.ConnectionDetails.Parameters, dataSourceId);
        dataSource.ConnectionDetails.Parameters = encryptedParameters;

        await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // ---------- First import run ----------
        var runId1 = Guid.NewGuid();
        var stepId1 = Guid.NewGuid();

        await _projectRunRespository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId1,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        var step1 = new StepJob
        {
            Id = stepId1,
            Type = StepType.Import,
            RunId = runId1,
            DataSourceId = dataSourceId,
        };

        await _stepRespository.InsertAsync(step1, Constants.Collections.StepJobs);

        var context1 = new CommandContext(runId1, projectId, stepId1, _projectRunRespository, _stepRespository);

        await _command.ExecuteAsync(context1, step1);

        // Capture ActiveSnapshotId after first import
        var dsAfterFirst = await _genericRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);
        Assert.NotNull(dsAfterFirst);
        Assert.NotNull(dsAfterFirst.ActiveSnapshotId);

        var snapshot1Id = dsAfterFirst.ActiveSnapshotId.Value;
        var snapshot1Collection = DatasetNames.SnapshotRows(snapshot1Id);

        var snapshot1Rows = await _dataStore.GetStreamFromTempCollection(snapshot1Collection, CancellationToken.None).ToListAsync();
        Assert.Equal(2, snapshot1Rows.Count);
        Assert.Equal("John Doe", snapshot1Rows.First()["Name"]);
        Assert.Equal(30, Convert.ToInt32(snapshot1Rows.First()["Age"]));

        // ---------- Create a refreshed excel file with same schema but different data ----------
        var refreshedExcelPath = Path.Combine(Path.GetTempPath(), $"refresh_test_{Guid.NewGuid():N}.xlsx");
        CreateExcelFileForRefresh(refreshedExcelPath);

        // Create FileImport record (this is what UI will pass as fileImportId)
        var fileImportId = Guid.NewGuid();
        var fileImport = new FileImport
        {
            Id = fileImportId,
            ProjectId = projectId,
            DataSourceType = DataSourceType.Excel,
            FileName = Path.GetFileName(refreshedExcelPath),
            OriginalName = Path.GetFileName(refreshedExcelPath),
            FilePath = refreshedExcelPath,
            FileSize = new FileInfo(refreshedExcelPath).Length,
            FileExtension = Path.GetExtension(refreshedExcelPath),
            CreatedDate = DateTime.UtcNow
        };

        await _fileImportRepo.InsertAsync(fileImport, Constants.Collections.ImportFile);

        // ---------- Refresh import run (same StepType.Import, with fileImportId in step.Configuration) ----------
        var runId2 = Guid.NewGuid();
        var stepId2 = Guid.NewGuid();

        await _projectRunRespository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId2,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        var step2 = new StepJob
        {
            Id = stepId2,
            Type = StepType.Import,
            RunId = runId2,
            DataSourceId = dataSourceId,
            Configuration = new Dictionary<string, object>
            {
                ["fileImportId"] = fileImportId.ToString()
            }
        };

        await _stepRespository.InsertAsync(step2, Constants.Collections.StepJobs);

        var context2 = new CommandContext(runId2, projectId, stepId2, _projectRunRespository, _stepRespository);

        await _command.ExecuteAsync(context2, step2);

        // Assert: ActiveSnapshotId changed
        var dsAfterRefresh = await _genericRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);
        Assert.NotNull(dsAfterRefresh);
        Assert.NotNull(dsAfterRefresh.ActiveSnapshotId);

        var snapshot2Id = dsAfterRefresh.ActiveSnapshotId.Value;
        Assert.NotEqual(snapshot1Id, snapshot2Id);

        // Assert: LatestFileImportId updated
        Assert.Equal(fileImportId, dsAfterRefresh.LatestFileImportId);

        // Assert: new snapshot collection contains refreshed data
        var snapshot2Collection = DatasetNames.SnapshotRows(snapshot2Id);
        var snapshot2Rows = await _dataStore.GetStreamFromTempCollection(snapshot2Collection, CancellationToken.None).ToListAsync();

        Assert.Equal(2, snapshot2Rows.Count);
        Assert.Equal("John Doe", snapshot2Rows.First()["Name"]);
        Assert.Equal(31, Convert.ToInt32(snapshot2Rows.First()["Age"]));

        // Assert: two snapshots exist for this datasource
        var snaps = await _snapshotRepo.QueryAsync(s => s.DataSourceId == dataSourceId, Constants.Collections.DataSnapshots);
        Assert.True(snaps.Count >= 2);
    }

    // Helper method for refresh test (add inside same test class)
    private void CreateExcelFileForRefresh(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row (same schema)
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("Name");
        headerRow.CreateCell(1).SetCellValue("Age");

        // Different test data to confirm refresh worked
        var row1 = sheet.CreateRow(1);
        row1.CreateCell(0).SetCellValue("John Doe");
        row1.CreateCell(1).SetCellValue(31);

        var row2 = sheet.CreateRow(2);
        row2.CreateCell(0).SetCellValue("Jane Smith");
        row2.CreateCell(1).SetCellValue(26);

        using var fs = File.Create(filePath);
        workbook.Write(fs);
    }

    private DomainDataSource CreateExcelDataSource(Guid dataSourceId)
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
                ColumnMappings = new Dictionary<string, ColumnMapping>
                    {
                        { "Name", new ColumnMapping { SourceColumn = "Name", TargetColumn = "Name", Include = true } },
                        { "Age", new ColumnMapping { SourceColumn = "Age", TargetColumn = "Age", Include = true } }
                    }
            }
        };
    }
}

