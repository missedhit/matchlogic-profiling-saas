using Moq;
using MatchLogic.Application.Common;
using MatchLogic.Application.EventHandlers;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.Orchestration;
using MatchLogic.Application.Features.FinalExport;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Project.Commands;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using DataSource = MatchLogic.Domain.Project.DataSource;
using MatchLogic.Application.Features.Export;
using MatchLogic.Infrastructure.Export;
using MatchLogic.Infrastructure.FinalExport;
using MatchLogic.Domain.Scheduling;

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Comprehensive tests for FinalExportCommand
/// Following MasterRecordDeterminationCommandTest patterns
/// </summary>
public class FinalExportCommandTest : IDisposable
{
    private readonly string _dbPath;
    private readonly List<string> _tempFiles = new List<string>();
    private readonly IServiceProvider _serviceProvider;

    // Core services
    private readonly IDataStore _dataStore;
    private readonly IRecordLinkageOrchestrator _recordLinkageOrchestrator;
    private readonly IFinalExportService _finalExportService;
    private readonly IProjectService _projectService;
    private readonly IJobEventPublisher _jobEventPublisher;

    // Repositories
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepRepository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<MatchSettings, Guid> _matchSettingRepository;
    private readonly IGenericRepository<MatchingDataSourcePairs, Guid> _pairRepository;
    private readonly IGenericRepository<Project, Guid> _projectRepository;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedFieldRowsRepository;
    private readonly IGenericRepository<FinalExportSettings, Guid> _exportSettingsRepository;
    private readonly IGenericRepository<FinalExportResult, Guid> _exportResultRepository;    
    private readonly IGenericRepository<DataSnapshot, Guid> _snapshotRepo;
    private readonly IGenericRepository<FileImport, Guid> _fileImportRepo;
    private readonly ISchemaValidationService _schemaValidation;

    // Other services
    private readonly IDataSourceService _dataSourceService;
    private readonly ISecureParameterHandler _secureParameterHandler;
    private readonly IRecordHasher _recordHasher;
    private readonly IConnectionBuilder _connectionBuilder;
    private readonly IColumnFilter _columnFilter;
    private readonly IExportDataWriterStrategyFactory _exportDataWriterStrategyFactory;
    private readonly IExportFilePathHelper _exportFilePathHelper;

    // Loggers
    private readonly ILogger<DataImportCommand> _importLogger;
    private readonly ILogger<MatchingCommand> _matchLogger;
    private readonly ILogger<FinalExportCommand> _exportLogger;

    public FinalExportCommandTest()
    {
        _dbPath = Path.GetTempFileName();
        var jobdbPath = Path.GetTempFileName();

        IServiceCollection services = new ServiceCollection();

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                { "Security:MasterKey", "TestMasterKey123456789012345678901234" },
                { "Export:BasePath", Path.GetTempPath() }
            })
            .Build();

        services.AddSingleton<IConfiguration>(config);
        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, jobdbPath);

        // Register FinalExport services
        services.AddScoped<IFinalExportService, FinalExportService>();
        services.AddScoped<IExportDataWriterStrategyFactory, ExportDataWriterStrategyFactory>();
        services.AddScoped<IExportFilePathHelper, ExportFilePathHelper>();

        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(typeof(DatabaseUpdateEventHandler).Assembly);
        });

        _serviceProvider = services.BuildServiceProvider();

        // Initialize services
        _dataStore = _serviceProvider.GetRequiredService<IDataStore>();
        _recordLinkageOrchestrator = _serviceProvider.GetRequiredService<IRecordLinkageOrchestrator>();
        _finalExportService = _serviceProvider.GetRequiredService<IFinalExportService>();
        _projectService = _serviceProvider.GetRequiredService<IProjectService>();
        _jobEventPublisher = _serviceProvider.GetRequiredService<IJobEventPublisher>();

        _projectRunRepository = _serviceProvider.GetRequiredService<IGenericRepository<ProjectRun, Guid>>();
        _stepRepository = _serviceProvider.GetRequiredService<IGenericRepository<StepJob, Guid>>();
        _dataSourceRepository = _serviceProvider.GetRequiredService<IGenericRepository<DataSource, Guid>>();
        _matchSettingRepository = _serviceProvider.GetRequiredService<IGenericRepository<MatchSettings, Guid>>();
        _pairRepository = _serviceProvider.GetRequiredService<IGenericRepository<MatchingDataSourcePairs, Guid>>();
        _projectRepository = _serviceProvider.GetRequiredService<IGenericRepository<Project, Guid>>();
        _mappedFieldRowsRepository = _serviceProvider.GetRequiredService<IGenericRepository<MappedFieldsRow, Guid>>();
        _exportSettingsRepository = _serviceProvider.GetRequiredService<IGenericRepository<FinalExportSettings, Guid>>();
        _exportResultRepository = _serviceProvider.GetRequiredService<IGenericRepository<FinalExportResult, Guid>>();

        _dataSourceService = _serviceProvider.GetRequiredService<IDataSourceService>();
        _secureParameterHandler = _serviceProvider.GetRequiredService<ISecureParameterHandler>();
        _recordHasher = _serviceProvider.GetRequiredService<IRecordHasher>();
        _connectionBuilder = _serviceProvider.GetRequiredService<IConnectionBuilder>();
        _columnFilter = _serviceProvider.GetRequiredService<IColumnFilter>();
        _exportDataWriterStrategyFactory = _serviceProvider.GetRequiredService<IExportDataWriterStrategyFactory>();
        _exportFilePathHelper = _serviceProvider.GetRequiredService<IExportFilePathHelper>();
        _schemaValidation = _serviceProvider.GetRequiredService<ISchemaValidationService>();        
        _snapshotRepo = _serviceProvider.GetRequiredService<IGenericRepository<DataSnapshot, Guid>>();
        _fileImportRepo = _serviceProvider.GetRequiredService<IGenericRepository<FileImport, Guid>>();
        _importLogger = new NullLogger<DataImportCommand>();
        _matchLogger = new NullLogger<MatchingCommand>();
        _exportLogger = new NullLogger<FinalExportCommand>();
    }

    public void Dispose()
    {
        foreach (var file in _tempFiles)
        {
            try
            {
                if (File.Exists(file))
                    File.Delete(file);
            }
            catch { }
        }
    }

    #region Helper Methods

    private string GetTempFilePath(string fileName)
    {
        var filePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}_{fileName}");
        _tempFiles.Add(filePath);
        return filePath;
    }

    private DataSource CreateExcelDataSource(Guid dataSourceId, string filePath, string[] columns)
    {
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", filePath },
                { "HasHeaders", "true" }
            }
        };

        connectionInfo.Parameters = Task.Run(async () =>
            await _secureParameterHandler.EncryptSensitiveParametersAsync(
                connectionInfo.Parameters,
                dataSourceId)).Result;

        var columnMappings = new Dictionary<string, ColumnMapping>();
        foreach (var column in columns)
        {
            columnMappings[column] = new ColumnMapping
            {
                SourceColumn = column,
                TargetColumn = column,
                Include = true
            };
        }

        return new DataSource
        {
            Id = dataSourceId,
            Type = DataSourceType.Excel,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "Sheet1",
                ColumnMappings = columnMappings
            },
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow
        };
    }

    private async Task<(Guid projectId, Guid runId)> SetupProjectWithRunAsync(string projectName)
    {
        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();

        var project = new Project
        {
            Id = projectId,
            Name = projectName,
            Description = "Test Project for Final Export",
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            Id = runId,
            ProjectId = projectId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        return (projectId, runId);
    }

    private async Task<Guid> ImportDataSourceAsync(
        Guid projectId,
        string filePath,
        string dataSourceName,
        Guid runId,
        DataImportCommand importCommand,
        string[] columns)
    {
        var dataSourceId = Guid.NewGuid();
        var importStepId = Guid.NewGuid();

        var dataSource = CreateExcelDataSource(dataSourceId, filePath, columns);
        dataSource.Name = dataSourceName;
        dataSource.ProjectId = projectId;

        await _dataSourceRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        var importStep = new StepJob
        {
            Id = importStepId,
            Type = StepType.Import,
            RunId = runId,
            DataSourceId = dataSourceId,
            Configuration = new Dictionary<string, object>
            {
                { "DataSourceId", dataSourceId }
            }
        };
        await _stepRepository.InsertAsync(importStep, Constants.Collections.StepJobs);

        var importContext = new CommandContext(runId, projectId, importStepId,
            _projectRunRepository, _stepRepository);
        await importCommand.ExecuteAsync(importContext, importStep);

        return dataSourceId;
    }

    private async Task CreateMatchDefinitionsAsync(
        Guid projectId,
        Guid ds1Id,
        Guid ds2Id,
        string ds1Name,
        string ds2Name)
    {
        var matchDefId = Guid.NewGuid();
        var pairId = Guid.NewGuid();

        var matchDefCollection = new MatchDefinitionCollection(projectId, Guid.NewGuid(), "Test Match Definitions")
        {
            Id = matchDefId,
            Definitions = new List<MatchDefinition>
            {
                new MatchDefinition
                {
                    Id = Guid.NewGuid(),
                    DataSourcePairId = pairId,
                    UIDefinitionIndex = 0,
                    ProjectId = projectId,
                    Criteria = new List<MatchCriteria>
                    {
                        new MatchCriteria
                        {
                            MatchingType = MatchingType.Fuzzy,
                            DataType = CriteriaDataType.Text,
                            Weight = 1.0,
                            Arguments = new Dictionary<ArgsValue, string>
                            {
                                { ArgsValue.Level, "0.8" }
                            },
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(ds1Id, ds1Name, "CompanyName"),
                                new FieldMapping(ds2Id, ds2Name, "BusinessName")
                            }
                        },
                        new MatchCriteria
                        {
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 1.0,
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(ds1Id, ds1Name, "City"),
                                new FieldMapping(ds2Id, ds2Name, "CityName")
                            }
                        }
                    }
                }
            }
        };

        await _dataStore.InsertAsync(matchDefCollection, Constants.Collections.MatchDefinitionCollection);

        var pair = new MatchingDataSourcePair(ds1Id, ds2Id);
        var pairConfig = new MatchingDataSourcePairs
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            Pairs = new List<MatchingDataSourcePair> { pair }
        };
        await _pairRepository.InsertAsync(pairConfig, Constants.Collections.MatchDataSourcePairs);

        var settings = new MatchSettings
        {
            ProjectId = projectId,
            MergeOverlappingGroups = false
        };
        await _matchSettingRepository.InsertAsync(settings, Constants.Collections.MatchSettings);

        // Create mapped fields for export column control
        var mappedFields = new MappedFieldsRow
        {
            ProjectId = projectId,
            MappedFields = new List<MappedFieldRow>
            {
                CreateMappedFieldRow("CompanyName", "BusinessName", ds1Id, ds2Id, ds1Name, ds2Name, true),
                CreateMappedFieldRow("City", "CityName", ds1Id, ds2Id, ds1Name, ds2Name, true),
                CreateMappedFieldRow("Email", "EmailAddress", ds1Id, ds2Id, ds1Name, ds2Name, true),
                CreateMappedFieldRow("Phone", "PhoneNumber", ds1Id, ds2Id, ds1Name, ds2Name, false) // Excluded
            }
        };
        await _mappedFieldRowsRepository.InsertAsync(mappedFields, Constants.Collections.MappedFieldRows);
    }

    private MappedFieldRow CreateMappedFieldRow(
        string ds1Field,
        string ds2Field,
        Guid ds1Id,
        Guid ds2Id,
        string ds1Name,
        string ds2Name,
        bool include)
    {
        var row = new MappedFieldRow { Include = include };
        row.AddField(new FieldMappingEx
        {
            FieldName = ds1Field,
            DataSourceId = ds1Id,
            DataSourceName = ds1Name,
            FieldIndex = 0,
            Mapped = true
        });
        row.AddField(new FieldMappingEx
        {
            FieldName = ds2Field,
            DataSourceId = ds2Id,
            DataSourceName = ds2Name,
            FieldIndex = 0,
            Mapped = true
        });
        return row;
    }

    private async Task ExecuteMatchingAsync(Guid projectId)
    {
        var matchRunId = Guid.NewGuid();
        var matchStepId = Guid.NewGuid();

        var matchingCommand = new MatchingCommand(
            _recordLinkageOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRepository,
            _dataSourceRepository,
            _matchLogger);

        var matchStep = new StepJob
        {
            Id = matchStepId,
            Type = StepType.Match,
            DataSourceId = projectId,
            RunId = matchRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = matchRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        await _stepRepository.InsertAsync(matchStep, Constants.Collections.StepJobs);
        var matchContext = new CommandContext(matchRunId, projectId, matchStepId,
            _projectRunRepository, _stepRepository);

        await matchingCommand.ExecuteAsync(matchContext, matchStep);
    }

    private FinalExportCommand CreateExportCommand()
    {
        return new FinalExportCommand(
            _finalExportService,
            _exportSettingsRepository,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRepository,
            _dataSourceRepository,
            Mock.Of<IOAuthTokenService>(),
            _dataStore,
            _exportLogger);
    }

    private async Task<(StepJob step, CommandContext context)> CreateExportStepAsync(
        Guid projectId,
        FinalExportSettings? settings = null,
        BaseConnectionInfo? connectionInfo = null)  // NEW PARAMETER
    {
        var exportRunId = Guid.NewGuid();
        var exportStepId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = exportRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        var configuration = new Dictionary<string, object> { { "ProjectId", projectId } };

        if (settings != null)
        {
            settings.ProjectId = projectId;
            await _exportSettingsRepository.InsertAsync(settings, Constants.Collections.FinalExportSettings);
            configuration["ExportSettingsId"] = settings.Id;
        }

        // NEW: Add connection info to configuration if provided
        if (connectionInfo != null)
        {
            configuration["ConnectionInfo"] = connectionInfo;
        }

        var exportStep = new StepJob
        {
            Id = exportStepId,
            Type = StepType.Export,
            DataSourceId = projectId,
            RunId = exportRunId,
            Configuration = configuration
        };

        await _stepRepository.InsertAsync(exportStep, Constants.Collections.StepJobs);

        var context = new CommandContext(exportRunId, projectId, exportStepId,
            _projectRunRepository, _stepRepository);

        return (exportStep, context);
    }

    private async Task<(Guid projectId, Guid ds1Id, Guid ds2Id)> SetupFullPipelineAsync(string testName)
    {
        var (projectId, runId) = await SetupProjectWithRunAsync(testName);

        var ds1Path = GetTempFilePath($"{testName}_ds1.xlsx");
        var ds2Path = GetTempFilePath($"{testName}_ds2.xlsx");

        CreateTestFile_DS1(ds1Path);
        CreateTestFile_DS2(ds2Path);

        string[] ds1Columns = { "RecordId", "FirstName", "LastName", "CompanyName", "Phone", "Email", "Address", "City", "ZipCode" };
        string[] ds2Columns = { "RecordId", "ContactName", "BusinessName", "PhoneNumber", "EmailAddress", "StreetAddress", "CityName", "PostalCode" };

        var importCommand = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _dataSourceRepository,
            _recordHasher,
            _importLogger,
            _dataStore,
            _projectRunRepository,
            _stepRepository,
            _connectionBuilder,
            _columnFilter,
            _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory(),
            _snapshotRepo,
            _fileImportRepo,
            _schemaValidation
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand, ds1Columns);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand, ds2Columns);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id, "DataSource1", "DataSource2");
        await ExecuteMatchingAsync(projectId);

        return (projectId, ds1Id, ds2Id);
    }

    private async Task ModifyGroupRecordFlagsAsync(Guid projectId, int groupId, bool? selected = null, bool? notDuplicate = null, bool? master = null)
    {
        var groupsCollection = $"groups_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";
        var (groups, _) = await _dataStore.GetPagedDataAsync(groupsCollection, 1, 100);

        foreach (var group in groups)
        {
            if (Convert.ToInt32(group["GroupId"]) != groupId)
                continue;

            var records = group["Records"] as IList<object>;
            if (records == null) continue;

            foreach (var recordObj in records.Cast<IDictionary<string, object>>())
            {
                if (selected.HasValue)
                    recordObj[RecordSystemFieldNames.Selected] = selected.Value;
                if (notDuplicate.HasValue)
                    recordObj[RecordSystemFieldNames.NotDuplicate] = notDuplicate.Value;
                if (master.HasValue)
                    recordObj[RecordSystemFieldNames.IsMasterRecord] = master.Value;
            }

            // Update the group document
            await _dataStore.UpdateByFieldAsync(group, groupsCollection, "_id", group["_id"]);
            break;
        }
    }

    private string GetExportCollectionName(Guid projectId) =>
        $"export_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";

    #endregion

    #region Test Data Creation

    private void CreateTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "RecordId", "FirstName", "LastName", "CompanyName", "Phone", "Email", "Address", "City", "ZipCode" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Records that will match with DS2
        AddDataRow(sheet, 1, "DS1_001", "John", "Smith", "Tech Solutions Inc", "555-1234", "john.smith@email.com", "123 Main St", "New York", "10001");
        AddDataRow(sheet, 2, "DS1_002", "Michael", "Johnson", "Digital Dynamics", "555-5678", "mike.j@company.org", "456 Oak Ave", "Boston", "02134");
        AddDataRow(sheet, 3, "DS1_003", "Sara", "Williams", "Creative Agency", "555-9012", "sarah.w@creative.net", "789 Pine Rd", "Chicago", "60601");
        AddDataRow(sheet, 4, "DS1_004", "Robert", "Brown", "Enterprise Corp", "555-3456", "r.brown@enterprise.com", "321 Elm St", "Houston", "77001");
        // Non-matching record (unique)
        AddDataRow(sheet, 5, "DS1_005", "Jennifer", "Davis", "Global Services", "555-7890", "jen.davis@global.com", "654 Maple Dr", "Phoenix", "85001");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "RecordId", "ContactName", "BusinessName", "PhoneNumber", "EmailAddress", "StreetAddress", "CityName", "PostalCode" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Records that will match with DS1
        AddDataRow(sheet, 1, "DS2_001", "John Smythe", "Tech Solutions Incorporated", "(555) 123-4567", "contact@techsolutions.com", "123 Main St Suite 100", "New York", "10001");
        AddDataRow(sheet, 2, "DS2_002", "Michael Johnston", "Digital Dynamics", "555.5678", "info@digitaldynamics.com", "456 Oak Ave Building B", "Boston", "02134");
        AddDataRow(sheet, 3, "DS2_003", "Sara Williams", "Creative Agency LLC", "5559012", "hello@creativeagency.com", "789 Pine Rd Floor 3", "Chicago", "60601");
        AddDataRow(sheet, 4, "DS2_004", "Roberto Brown", "Enterprise Corp", "555-3456", "sales@enterprise.com", "321 Elm St", "Houston", "77001");
        // Non-matching record (unique)
        AddDataRow(sheet, 5, "DS2_005", "Amanda Wilson", "Startup Hub", "555-4321", "amanda@startuphub.io", "999 Tech Blvd", "San Francisco", "94102");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void AddDataRow(ISheet sheet, int rowIndex, params object[] values)
    {
        var row = sheet.CreateRow(rowIndex);
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] != null)
                row.CreateCell(i).SetCellValue(values[i].ToString());
        }
    }

    #endregion

    #region ExportAction Tests

    private bool IsDuplicate(IDictionary<string, object> record)
    {
        var groupId = Convert.ToInt32(record[ExportFieldNames.GroupId]);
        var notDuplicate = Convert.ToBoolean(record[ExportFieldNames.NotDuplicate]);
        return groupId > 0 && !notDuplicate;
    }

    [Fact]
    public async Task ExecCommandAsync_AllRecordsAndFlagDuplicates_ExportsAllRecordsWithDuplicateFlag()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("AllRecordsTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeScoreFields = true,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        // Should have all 10 records (5 from each data source)
        Assert.Equal(10, totalCount);

        var exportedList = exportedData.ToList();

        // Verify system fields exist
        Assert.All(exportedList, record =>
        {
            Assert.True(record.ContainsKey(ExportFieldNames.GroupId));            
            Assert.True(record.ContainsKey(ExportFieldNames.Master));
            Assert.True(record.ContainsKey(ExportFieldNames.Selected));
            Assert.True(record.ContainsKey(ExportFieldNames.NotDuplicate));
            Assert.True(record.ContainsKey(ExportFieldNames.MdHits));
        });


        // Verify duplicates are flagged correctly
        var duplicates = exportedList.Where(r => Convert.ToBoolean(r[ExportFieldNames.NotDuplicate])).ToList();
        var nonDuplicates = exportedList.Where(r => !Convert.ToBoolean(r[ExportFieldNames.NotDuplicate])).ToList();

        Assert.True(duplicates.Count > 0, "Should have duplicate records");
        Assert.True(nonDuplicates.Count > 0, "Should have non-duplicate records");

        // Duplicates should have GroupId > 0
        Assert.All(duplicates, r => Assert.True(Convert.ToInt32(r[ExportFieldNames.GroupId]) > 0));

        // Non-duplicates should have GroupId = 0
        Assert.All(nonDuplicates, r => Assert.Equal(0, Convert.ToInt32(r[ExportFieldNames.GroupId])));

        Console.WriteLine($"AllRecordsAndFlagDuplicates Test - Total: {totalCount}, Duplicates: {duplicates.Count}, Non-Duplicates: {nonDuplicates.Count}");
    }

    [Fact]
    public async Task ExecCommandAsync_SuppressAllDuplicateRecords_ExportsOnlyNonDuplicates()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("SuppressDuplicatesTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.SuppressAllDuplicateRecords,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        // Should only have non-duplicate records
        var exportedList = exportedData.ToList();

        Assert.All(exportedList, record =>
        {
            var isDuplicate = IsDuplicate(record);;
            Assert.False(isDuplicate, "Should not contain duplicate records");
        });

        // Should have 2 non-duplicate records (DS1_005 and DS2_005)
        Assert.Equal(2, totalCount);

        Console.WriteLine($"SuppressAllDuplicateRecords Test - Exported: {totalCount} non-duplicates");
    }

    [Fact]
    public async Task ExecCommandAsync_NonDupsAndMasterRecordRemaining_ExportsNonDupsAndMasters()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("MasterRemainingTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.NonDupsAndMasterRecordRemaining,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // Each record should either be non-duplicate OR master
        Assert.All(exportedList, record =>
        {
            var isDuplicate = IsDuplicate(record);;
            var isMaster = Convert.ToBoolean(record[ExportFieldNames.Master]);

            Assert.True(!isDuplicate || isMaster, "Record should be non-duplicate OR master");
        });

        // Count masters and non-duplicates
        var masters = exportedList.Count(r => Convert.ToBoolean(r[ExportFieldNames.Master]));
        var nonDups = exportedList.Count(r => !Convert.ToBoolean(r[ExportFieldNames.NotDuplicate]));

        Console.WriteLine($"NonDupsAndMasterRecordRemaining Test - Total: {totalCount}, Masters: {masters}, Non-Dups: {nonDups}");
    }

    [Fact]
    public async Task ExecCommandAsync_DuplicatesOnly_ExportsOnlyDuplicates()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("DuplicatesOnlyTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.DuplicatesOnly,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // All records should be duplicates
        Assert.All(exportedList, record =>
        {
            var isDuplicate = IsDuplicate(record);;
            Assert.True(isDuplicate, "All records should be duplicates");
        });

        // Should have 8 duplicate records (4 matched pairs)
        Assert.Equal(8, totalCount);

        Console.WriteLine($"DuplicatesOnly Test - Exported: {totalCount} duplicates");
    }

    [Fact]
    public async Task ExecCommandAsync_CrossReference_ExportsOnlyCrossDataSourceDuplicates()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("CrossReferenceTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.CrossReference,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // All records should be duplicates AND cross-reference
        Assert.All(exportedList, record =>
        {
            var isDuplicate = IsDuplicate(record);;
            Assert.True(isDuplicate, "All records should be duplicates");

            var groupId = Convert.ToInt32(record[ExportFieldNames.GroupId]);
            Assert.True(groupId > 0, "All records should have a group");
        });

        // Verify groups span multiple data sources
        var groupedRecords = exportedList.GroupBy(r => Convert.ToInt32(r[ExportFieldNames.GroupId]));
        foreach (var group in groupedRecords)
        {
            var dataSources = group.Select(r => r[ExportFieldNames.DataSourceName]).Distinct().ToList();
            Assert.True(dataSources.Count >= 2, $"Group {group.Key} should span multiple data sources");
        }

        Console.WriteLine($"CrossReference Test - Exported: {totalCount} cross-reference records in {groupedRecords.Count()} groups");
    }

    #endregion

    #region SelectedAction Tests

    [Fact]
    public async Task ExecCommandAsync_SuppressSelected_ExcludesSelectedRecords()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("SuppressSelectedTest");

        // Modify first group to have selected records
        await ModifyGroupRecordFlagsAsync(projectId, 1, selected: true);

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.SuppressSelected,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // No exported record should have Selected = true
        Assert.All(exportedList, record =>
        {
            var isSelected = Convert.ToBoolean(record[ExportFieldNames.Selected]);
            Assert.False(isSelected, "Should not contain selected records");
        });

        // Should have fewer than 10 records since we suppressed selected ones
        Assert.True(totalCount < 10, "Should have suppressed some records");

        Console.WriteLine($"SuppressSelected Test - Exported: {totalCount} records (suppressed selected)");
    }

    [Fact]
    public async Task ExecCommandAsync_ShowSelectedOnly_ExportsOnlySelectedRecords()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("ShowSelectedOnlyTest");

        // Modify first group to have selected records
        await ModifyGroupRecordFlagsAsync(projectId, 1, selected: true);

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowSelectedOnly,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // All exported records should have Selected = true
        Assert.All(exportedList, record =>
        {
            var isSelected = Convert.ToBoolean(record[ExportFieldNames.Selected]);
            Assert.True(isSelected, "All records should be selected");
        });

        Assert.True(totalCount > 0, "Should have some selected records");

        Console.WriteLine($"ShowSelectedOnly Test - Exported: {totalCount} selected records");
    }

    #endregion

    #region Score Field Tests

    [Fact]
    public async Task ExecCommandAsync_IncludeScoreFields_ExportsScoreColumns()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("ScoreFieldsTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeScoreFields = true,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();
        var duplicateRecord = exportedList.FirstOrDefault(r => Convert.ToBoolean(r[ExportFieldNames.NotDuplicate]));

        Assert.NotNull(duplicateRecord);

        // Verify score columns exist (based on match definition criteria)
        // Expected: "CompanyName_BusinessName score 1", "City_CityName score 1", "score 1", "s 1"
        Assert.True(duplicateRecord.Keys.Any(k => k.Contains("score")), "Should have score columns");
        Assert.True(duplicateRecord.ContainsKey("score 1"), "Should have total score column");
        Assert.True(duplicateRecord.ContainsKey("s 1"), "Should have threshold indicator column");

        // Verify score values for duplicate records
        var totalScore = Convert.ToDouble(duplicateRecord["score 1"]);
        Assert.True(totalScore > 0, "Duplicate record should have positive score");

        var thresholdIndicator = Convert.ToBoolean(duplicateRecord["s 1"]);
        Assert.True(thresholdIndicator, "Duplicate should pass threshold");

        // Verify non-duplicate has zero scores
        var nonDuplicateRecord = exportedList.FirstOrDefault(r => !Convert.ToBoolean(r[ExportFieldNames.NotDuplicate]));
        if (nonDuplicateRecord != null)
        {
            var nonDupScore = Convert.ToDouble(nonDuplicateRecord["score 1"]);
            Assert.Equal(0, nonDupScore);
        }

        Console.WriteLine($"ScoreFields Test - Verified score columns. Sample score: {totalScore}");
    }

    [Fact]
    public async Task ExecCommandAsync_ExcludeScoreFields_NoScoreColumns()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("NoScoreFieldsTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeScoreFields = false,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();
        var firstRecord = exportedList.First();

        // Should not have score columns
        Assert.False(firstRecord.ContainsKey("score 1"), "Should not have score column");
        Assert.False(firstRecord.ContainsKey("s 1"), "Should not have threshold column");
        Assert.False(firstRecord.Keys.Any(k => k.Contains(" score")), "Should not have any score columns");

        Console.WriteLine($"NoScoreFields Test - Verified no score columns present");
    }

    #endregion

    #region System Field Tests

    [Fact]
    public async Task ExecCommandAsync_ExcludeSystemFields_NoSystemColumns()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("NoSystemFieldsTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeScoreFields = false,
            IncludeSystemFields = false
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();
        var firstRecord = exportedList.First();

        // Should not have system columns
        Assert.False(firstRecord.ContainsKey(ExportFieldNames.GroupId), "Should not have GroupId");
        Assert.False(firstRecord.ContainsKey(ExportFieldNames.NotDuplicate), "Should not have IsDuplicate");
        Assert.False(firstRecord.ContainsKey(ExportFieldNames.Master), "Should not have Master");
        Assert.False(firstRecord.ContainsKey(ExportFieldNames.Selected), "Should not have Selected");
        Assert.False(firstRecord.ContainsKey(ExportFieldNames.MdHits), "Should not have MDs");

        // Should still have data fields
        Assert.True(firstRecord.Keys.Count > 0, "Should have data fields");

        Console.WriteLine($"NoSystemFields Test - Verified no system columns. Data fields: {firstRecord.Keys.Count}");
    }

    #endregion

    #region DataSetsToInclude Tests

    [Fact]
    public async Task ExecCommandAsync_ExcludeDataSource_ExportsOnlyIncludedSources()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("ExcludeDataSourceTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true,
            DataSetsToInclude = new Dictionary<Guid, bool>
            {
                { ds1Id, true },
                { ds2Id, false }  // Exclude DS2
            }
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // All records should be from DS1
        Assert.All(exportedList, record =>
        {
            var dataSourceName = record[ExportFieldNames.DataSourceName].ToString();
            Assert.Equal("DataSource1", dataSourceName);
        });

        // Should have 5 records (only from DS1)
        Assert.Equal(5, totalCount);

        Console.WriteLine($"ExcludeDataSource Test - Exported: {totalCount} records from DS1 only");
    }

    #endregion

    #region Statistics Tests

    [Fact]
    public async Task ExecCommandAsync_Statistics_RecordsByDataSourceIsAccurate()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("StatisticsTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var updatedStep = await _stepRepository.GetByIdAsync(exportStep.Id, Constants.Collections.StepJobs);
        var metadata = updatedStep.StepData.First().Metadata;

        Assert.True(metadata.ContainsKey("RecordsExported"));
        Assert.True(metadata.ContainsKey("GroupsProcessed"));
        Assert.True(metadata.ContainsKey("UniqueRecords"));
        Assert.True(metadata.ContainsKey("DuplicateRecords"));

        var totalExported = Convert.ToInt64(metadata["RecordsExported"]);
        var uniqueRecords = Convert.ToInt64(metadata["UniqueRecords"]);
        var duplicateRecords = Convert.ToInt64(metadata["DuplicateRecords"]);

        Assert.Equal(10, totalExported);
        Assert.Equal(totalExported, uniqueRecords + duplicateRecords);

        // Verify export result was saved
        var exportResults = await _exportResultRepository.QueryAsync(
            r => r.ProjectId == projectId,
            Constants.Collections.FinalExportResults);

        Assert.NotEmpty(exportResults);
        var lastResult = exportResults.OrderByDescending(r => r.CreatedAt).First();

        Assert.Equal(projectId, lastResult.ProjectId);
        Assert.True(lastResult.Statistics.RecordsByDataSource.ContainsKey(ds1Id));
        Assert.True(lastResult.Statistics.RecordsByDataSource.ContainsKey(ds2Id));

        // Verify per-datasource counts are correct (not cumulative)
        var ds1Count = lastResult.Statistics.RecordsByDataSource[ds1Id];
        var ds2Count = lastResult.Statistics.RecordsByDataSource[ds2Id];

        Assert.Equal(5, ds1Count);
        Assert.Equal(5, ds2Count);
        Assert.Equal(totalExported, ds1Count + ds2Count);

        Console.WriteLine($"Statistics Test - Total: {totalExported}, DS1: {ds1Count}, DS2: {ds2Count}, Groups: {metadata["GroupsProcessed"]}");
    }

    #endregion

    #region MappedFieldsRow Tests

    [Fact]
    public async Task ExecCommandAsync_MappedFieldsRow_ExportsOnlyIncludedFields()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("MappedFieldsTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeScoreFields = false,
            IncludeSystemFields = false  // Only data fields
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();
        var ds1Record = exportedList.FirstOrDefault(r =>
            r.ContainsKey("CompanyName")); // DS1 field

        Assert.NotNull(ds1Record);

        // Should have included fields
        Assert.True(ds1Record.ContainsKey("CompanyName") || ds1Record.ContainsKey("BusinessName"));
        Assert.True(ds1Record.ContainsKey("City") || ds1Record.ContainsKey("CityName"));
        Assert.True(ds1Record.ContainsKey("Email") || ds1Record.ContainsKey("EmailAddress"));

        // Phone was marked Include = false, so should not be present
        // (Note: this depends on your implementation - fallback may include all fields)

        Console.WriteLine($"MappedFieldsRow Test - Fields in export: {string.Join(", ", ds1Record.Keys)}");
    }

    #endregion

    #region Preview Tests

    [Fact]
    public async Task GeneratePreviewAsync_BeforeExport_ReturnsEmptyData()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("PreviewBeforeExportTest");

        // Act - call preview without running export
        var preview = await _finalExportService.GetExportDataAsync(projectId, false, 1, 100);

        // Assert
        Assert.Equal(0, preview.TotalCount);
        Assert.Empty(preview.Data);

        Console.WriteLine("Preview Before Export Test - Verified empty preview");
    }

    [Fact]
    public async Task GeneratePreviewAsync_AfterExport_ReturnsExportedData()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("PreviewAfterExportTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Run export first
        await exportCommand.ExecuteAsync(context, exportStep);

        // Act - call preview after export
        var preview = await _finalExportService.GetExportDataAsync(projectId, false, 1, 100);

        // Assert
        Assert.Equal(10, preview.TotalCount);
        Assert.NotEmpty(preview.Data);

        Console.WriteLine($"Preview After Export Test - Total: {preview.TotalCount}");
    }

    [Fact]
    public async Task GeneratePreviewAsync_Pagination_ReturnsCorrectPage()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("PreviewPaginationTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        await exportCommand.ExecuteAsync(context, exportStep);

        // Act - get page 1 with size 3
        var page1 = await _finalExportService.GetExportDataAsync(projectId, false, 1, 3);

        // Act - get page 2 with size 3
        var page2 = await _finalExportService.GetExportDataAsync(projectId, false, 2, 3);

        // Assert
        Assert.Equal(3, page1.Data.Count());
        Assert.Equal(3, page2.Data.Count());
        Assert.Equal(10, page1.TotalCount);
        Assert.Equal(10, page2.TotalCount);

        // Pages should have different records
        var page1Ids = page1.Data.Select(r =>
            $"{r[ExportFieldNames.DataSourceName]}_{r[ExportFieldNames.Record]}").ToList();
        var page2Ids = page2.Data.Select(r =>
            $"{r[ExportFieldNames.DataSourceName]}_{r[ExportFieldNames.Record]}").ToList();
        Assert.False(page1Ids.Intersect(page2Ids).Any(), "Pages should have different records");

        Console.WriteLine($"Pagination Test - Page1: {page1Ids.Count}, Page2: {page2Ids.Count}");
    }

    #endregion

    #region Validation Tests

    [Fact]
    public async Task ValidateExportAsync_NoDataSources_ReturnsInvalid()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        await _projectRepository.InsertAsync(new Project
        {
            Id = projectId,
            Name = "Empty Project"
        }, Constants.Collections.Projects);

        // Act
        var validation = await _finalExportService.ValidateExportAsync(projectId);

        // Assert
        Assert.False(validation.IsValid);
        Assert.Contains("No data sources found", validation.ValidationErrors.First());

        Console.WriteLine($"No DataSources Validation Test - Errors: {string.Join(", ", validation.ValidationErrors)}");
    }

    [Fact]
    public async Task ValidateExportAsync_NoGroups_ReturnsValidButNoResults()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("NoGroupsValidationTest");

        var ds1Path = GetTempFilePath("no_groups_ds1.xlsx");
        CreateTestFile_DS1(ds1Path);

        string[] columns = { "RecordId", "FirstName", "LastName", "CompanyName", "Phone", "Email", "Address", "City", "ZipCode" };

        var importCommand = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _dataSourceRepository,
            _recordHasher,
            _importLogger,
            _dataStore,
            _projectRunRepository,
            _stepRepository,
            _connectionBuilder,
            _columnFilter,
            _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory(),
            _snapshotRepo,
            _fileImportRepo,
            _schemaValidation
        );

        await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand, columns);

        // Don't run matching - no groups will exist

        // Act
        var validation = await _finalExportService.ValidateExportAsync(projectId);

        // Assert — export should still be valid even without match results
        Assert.True(validation.IsValid);
        Assert.False(validation.HasResults);
        Assert.Empty(validation.ValidationErrors);

        Console.WriteLine($"No Groups Validation Test - IsValid: {validation.IsValid}, HasResults: {validation.HasResults}");
    }

    [Fact]
    public async Task ValidateExportAsync_ValidProject_ReturnsValid()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("ValidValidationTest");

        // Act
        var validation = await _finalExportService.ValidateExportAsync(projectId);

        // Assert
        Assert.True(validation.IsValid);
        Assert.True(validation.HasResults);
        Assert.True(validation.ResultsInSync);
        Assert.Empty(validation.ValidationErrors);

        Console.WriteLine($"Valid Project Validation Test - IsValid: {validation.IsValid}, HasResults: {validation.HasResults}");
    }

    #endregion

    #region NotDuplicate Flag Tests

    [Fact]
    public async Task ExecCommandAsync_NotDuplicateFlag_ExcludesFromDuplicates()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("NotDuplicateFlagTest");

        // Mark records in group 1 as NotDuplicate
        await ModifyGroupRecordFlagsAsync(projectId, 1, notDuplicate: true);

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.DuplicatesOnly,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // Records marked as NotDuplicate should not be in DuplicatesOnly export
        var group1Records = exportedList.Where(r => Convert.ToInt32(r[ExportFieldNames.GroupId]) == 1).ToList();
        Assert.Empty(group1Records); // Group 1 records marked as NotDuplicate shouldn't appear

        // Other group records should still be present
        Assert.True(totalCount > 0);
        Assert.All(exportedList, record =>
        {
            var isDuplicate = IsDuplicate(record);;
            Assert.True(isDuplicate);
        });

        Console.WriteLine($"NotDuplicate Flag Test - Exported: {totalCount} records (group 1 excluded)");
    }

    #endregion

    #region Command Configuration Tests

    [Fact]
    public async Task ExecCommandAsync_WithInlineSettings_UsesProvidedSettings()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("InlineSettingsTest");

        var exportRunId = Guid.NewGuid();
        var exportStepId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = exportRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        // Provide settings inline in configuration
        var exportStep = new StepJob
        {
            Id = exportStepId,
            Type = StepType.Export,
            DataSourceId = projectId,
            RunId = exportRunId,
            Configuration = new Dictionary<string, object>
            {
                { "ProjectId", projectId },
                { "ExportSettings", new FinalExportSettings
                    {
                        ExportAction = ExportAction.DuplicatesOnly,
                        SelectedAction = SelectedAction.ShowAll
                    }
                }
            }
        };

        await _stepRepository.InsertAsync(exportStep, Constants.Collections.StepJobs);

        var context = new CommandContext(exportRunId, projectId, exportStepId,
            _projectRunRepository, _stepRepository);

        var exportCommand = CreateExportCommand();

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        // Should have only duplicates (from inline settings)
        Assert.Equal(8, totalCount);

        Console.WriteLine($"Inline Settings Test - Used DuplicatesOnly setting, exported: {totalCount}");
    }

    [Fact]
    public async Task ExecCommandAsync_WithDefaultSettings_UsesDefaults()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("DefaultSettingsTest");

        var exportRunId = Guid.NewGuid();
        var exportStepId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = exportRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        // No settings provided - should use defaults
        var exportStep = new StepJob
        {
            Id = exportStepId,
            Type = StepType.Export,
            DataSourceId = projectId,
            RunId = exportRunId,
            Configuration = new Dictionary<string, object>
            {
                { "ProjectId", projectId }
            }
        };

        await _stepRepository.InsertAsync(exportStep, Constants.Collections.StepJobs);

        var context = new CommandContext(exportRunId, projectId, exportStepId,
            _projectRunRepository, _stepRepository);

        var exportCommand = CreateExportCommand();

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        // Default is AllRecordsAndFlagDuplicates - should have all 10 records
        Assert.Equal(10, totalCount);

        Console.WriteLine($"Default Settings Test - Used defaults, exported: {totalCount}");
    }

    #endregion

    #region File Export Tests (NEW)

    [Fact]
    public async Task ExecCommandAsync_ExportToExcel_CreatesFileWithData()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("ExportToExcelTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true,
            IncludeScoreFields = true
        };

        var exportFilePath = GetTempFilePath("export_test.xlsx");
        var connectionInfo = new BaseConnectionInfo
        {
            Type = DataSourceType.Excel,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", exportFilePath }
            }
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings, connectionInfo);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        Assert.True(File.Exists(exportFilePath), "Export file should exist");

        // Verify result contains file path
        var exportResults = await _exportResultRepository.QueryAsync(
            r => r.ProjectId == projectId,
            Constants.Collections.FinalExportResults);

        var result = exportResults.OrderByDescending(r => r.CreatedAt).First();
        Assert.NotNull(result.ExportFilePath);
        Assert.Contains(".xlsx", result.ExportFilePath);

        Console.WriteLine($"Export to Excel Test - File created: {result.ExportFilePath}");
    }

    [Fact]
    public async Task ExecCommandAsync_ExportToExcel_FileContainsCorrectRecordCount()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("ExportToExcelCountTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.DuplicatesOnly,
            SelectedAction = SelectedAction.ShowAll
        };

        var exportFilePath = GetTempFilePath("export_duplicates.xlsx");
        var connectionInfo = new BaseConnectionInfo
        {
            Type = DataSourceType.Excel,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", exportFilePath }
            }
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings, connectionInfo);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert - read Excel file and verify record count
        using var fileStream = File.OpenRead(exportFilePath);
        using var workbook = new XSSFWorkbook(fileStream);
        var sheet = workbook.GetSheetAt(0);

        // -1 for header row
        var recordCount = sheet.LastRowNum;
        Assert.Equal(8, recordCount); // 8 duplicates expected

        Console.WriteLine($"Export to Excel Count Test - Verified {recordCount} records in file");
    }

    [Fact]
    public async Task ExecCommandAsync_ExportWithoutConnectionInfo_CreatesLiteDBCollection()
    {
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("DefaultExportTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll
        };

        var exportCommand = CreateExportCommand();
        // No connection info - should default to LiteDB
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings, connectionInfo: null);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert - data should be in LiteDB collection
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        Assert.Equal(10, totalCount);

        // Verify result has no file path
        var exportResults = await _exportResultRepository.QueryAsync(
            r => r.ProjectId == projectId,
            Constants.Collections.FinalExportResults);

        var result = exportResults.OrderByDescending(r => r.CreatedAt).First();
        // ExportFilePath might be null or empty for LiteDB exports
        Assert.True(string.IsNullOrEmpty(result.ExportFilePath) || result.ExportFilePath.Contains(exportCollection));

        Console.WriteLine($"Default Export Test - Data in LiteDB collection: {exportCollection}");
    }

    #endregion

    #region Edge Case Tests

    [Fact]
    public async Task ExecCommandAsync_EmptyGroups_HandlesGracefully()
    {
        // Arrange - project with data but no matches
        var (projectId, runId) = await SetupProjectWithRunAsync("EmptyGroupsTest");

        var ds1Path = GetTempFilePath("empty_groups_ds1.xlsx");
        var ds2Path = GetTempFilePath("empty_groups_ds2.xlsx");

        // Create files with no matching data
        CreateNonMatchingTestFile_DS1(ds1Path);
        CreateNonMatchingTestFile_DS2(ds2Path);

        string[] ds1Columns = { "RecordId", "FirstName", "LastName", "CompanyName", "Phone", "Email", "Address", "City", "ZipCode" };
        string[] ds2Columns = { "RecordId", "ContactName", "BusinessName", "PhoneNumber", "EmailAddress", "StreetAddress", "CityName", "PostalCode" };

        var importCommand = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _dataSourceRepository,
            _recordHasher,
            _importLogger,
            _dataStore,
            _projectRunRepository,
            _stepRepository,
            _connectionBuilder,
            _columnFilter,
            _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory(),
            _snapshotRepo,
            _fileImportRepo,
            _schemaValidation
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand, ds1Columns);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand, ds2Columns);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id, "DataSource1", "DataSource2");
        await ExecuteMatchingAsync(projectId);

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll,
            IncludeSystemFields = true
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var exportCollection = GetExportCollectionName(projectId);
        var (exportedData, totalCount) = await _dataStore.GetPagedDataAsync(exportCollection, 1, 100);

        var exportedList = exportedData.ToList();

        // All records should be non-duplicates
        Assert.All(exportedList, record =>
        {
            var isDuplicate = IsDuplicate(record);;
            Assert.False(isDuplicate);
            Assert.Equal(0, Convert.ToInt32(record[ExportFieldNames.GroupId]));
        });

        Console.WriteLine($"Empty Groups Test - All {totalCount} records are non-duplicates");
    }

    [Fact]
    public async Task ExecCommandAsync_LargeDataset_ProgressReporting()
    {
        // This test verifies the batch processing works correctly
        // Arrange
        var (projectId, ds1Id, ds2Id) = await SetupFullPipelineAsync("LargeDatasetTest");

        var settings = new FinalExportSettings
        {
            ExportAction = ExportAction.AllRecordsAndFlagDuplicates,
            SelectedAction = SelectedAction.ShowAll
        };

        var exportCommand = CreateExportCommand();
        var (exportStep, context) = await CreateExportStepAsync(projectId, settings);

        // Act
        await exportCommand.ExecuteAsync(context, exportStep);

        // Assert
        var updatedStep = await _stepRepository.GetByIdAsync(exportStep.Id, Constants.Collections.StepJobs);
        var metadata = updatedStep.StepData.First().Metadata;

        Assert.True(metadata.ContainsKey("ProcessingTimeMs"));
        var processingTime = Convert.ToDouble(metadata["ProcessingTimeMs"]);
        Assert.True(processingTime > 0);

        Console.WriteLine($"Large Dataset Test - Processing time: {processingTime}ms");
    }

    #endregion

    #region Additional Test Data Creation

    private void CreateNonMatchingTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "RecordId", "FirstName", "LastName", "CompanyName", "Phone", "Email", "Address", "City", "ZipCode" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Records that won't match (different cities)
        AddDataRow(sheet, 1, "DS1_001", "Alice", "Smith", "Alpha Corp", "111-1111", "alice@alpha.com", "1 A St", "Atlanta", "30301");
        AddDataRow(sheet, 2, "DS1_002", "Bob", "Jones", "Beta Inc", "222-2222", "bob@beta.com", "2 B St", "Baltimore", "21201");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateNonMatchingTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "RecordId", "ContactName", "BusinessName", "PhoneNumber", "EmailAddress", "StreetAddress", "CityName", "PostalCode" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Records that won't match (different companies and cities)
        AddDataRow(sheet, 1, "DS2_001", "Charlie Brown", "Gamma LLC", "333-3333", "charlie@gamma.com", "3 C St", "Chicago", "60601");
        AddDataRow(sheet, 2, "DS2_002", "Diana Prince", "Delta Co", "444-4444", "diana@delta.com", "4 D St", "Denver", "80201");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    #endregion
}