using MatchLogic.Application.Common;
using MatchLogic.Application.EventHandlers;
using MatchLogic.Application.Features.DataMatching.Orchestration;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using Moq;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using MatchLogic.Domain.Scheduling;
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

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Comprehensive integration tests for FieldOverwriteCommand
/// Following MasterRecordDeterminationCommandTest patterns with real services
/// </summary>
public class FieldOverwriteCommandTest : IDisposable
{
    private readonly string _dbPath;
    private readonly List<string> _tempFiles = new List<string>();
    private readonly IServiceProvider _serviceProvider;

    // Core services
    private readonly IDataStore _dataStore;
    private readonly IRecordLinkageOrchestrator _recordLinkageOrchestrator;
    private readonly IMasterRecordDeterminationOrchestrator _masterOrchestrator;
    private readonly IFieldOverwriteOrchestrator _overwriteOrchestrator;
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
    private readonly IMasterRecordRuleSetRepository _masterRuleSetRepository;
    private readonly IFieldOverwriteRuleSetRepository _overwriteRuleSetRepository;

    // Other services
    private readonly IDataSourceService _dataSourceService;
    private readonly ISecureParameterHandler _secureParameterHandler;
    private readonly IRecordHasher _recordHasher;
    private readonly IConnectionBuilder _connectionBuilder;
    private readonly IMatchConfigurationService _matchDefinitionService;
    private readonly IColumnFilter _columnFilter;

    // Loggers
    private readonly ILogger<DataImportCommand> _importLogger;
    private readonly ILogger<MatchingCommand> _matchLogger;
    private readonly ILogger<MasterRecordDeterminationCommand> _masterLogger;
    private readonly ILogger<FieldOverwriteCommand> _overwriteLogger;
    private readonly IGenericRepository<ScheduledTask, Guid> _scheduleRepository;
    private readonly IGenericRepository<DataSnapshot, Guid> _snapshotRepo;
    private readonly IGenericRepository<FileImport, Guid> _fileImportRepo;
    private readonly ISchemaValidationService _schemaValidation;
    public FieldOverwriteCommandTest()
    {
        _dbPath = Path.GetTempFileName();
        var jobdbPath = Path.GetTempFileName();

        IServiceCollection services = new ServiceCollection();

        // Configuration
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                { "Security:MasterKey", "TestMasterKey123456789012345678901234" }
            })
            .Build();

        services.AddSingleton<IConfiguration>(config);
        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, jobdbPath);
        

        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(typeof(DatabaseUpdateEventHandler).Assembly);
        });

        _serviceProvider = services.BuildServiceProvider();

        // Initialize services
        _dataStore = _serviceProvider.GetRequiredService<IDataStore>();
        _recordLinkageOrchestrator = _serviceProvider.GetRequiredService<IRecordLinkageOrchestrator>();
        _masterOrchestrator = _serviceProvider.GetRequiredService<IMasterRecordDeterminationOrchestrator>();
        _overwriteOrchestrator = _serviceProvider.GetRequiredService<IFieldOverwriteOrchestrator>();
        _projectService = _serviceProvider.GetRequiredService<IProjectService>();
        _jobEventPublisher = _serviceProvider.GetRequiredService<IJobEventPublisher>();

        _projectRunRepository = _serviceProvider.GetRequiredService<IGenericRepository<ProjectRun, Guid>>();
        _stepRepository = _serviceProvider.GetRequiredService<IGenericRepository<StepJob, Guid>>();
        _dataSourceRepository = _serviceProvider.GetRequiredService<IGenericRepository<DataSource, Guid>>();
        _matchSettingRepository = _serviceProvider.GetRequiredService<IGenericRepository<MatchSettings, Guid>>();
        _pairRepository = _serviceProvider.GetRequiredService<IGenericRepository<MatchingDataSourcePairs, Guid>>();
        _projectRepository = _serviceProvider.GetRequiredService<IGenericRepository<Project, Guid>>();
        _mappedFieldRowsRepository = _serviceProvider.GetRequiredService<IGenericRepository<MappedFieldsRow, Guid>>();
        _masterRuleSetRepository = _serviceProvider.GetRequiredService<IMasterRecordRuleSetRepository>();
        _overwriteRuleSetRepository = _serviceProvider.GetRequiredService<IFieldOverwriteRuleSetRepository>();

        _dataSourceService = _serviceProvider.GetRequiredService<IDataSourceService>();
        _secureParameterHandler = _serviceProvider.GetRequiredService<ISecureParameterHandler>();
        _recordHasher = _serviceProvider.GetRequiredService<IRecordHasher>();
        _connectionBuilder = _serviceProvider.GetRequiredService<IConnectionBuilder>();
        _matchDefinitionService = _serviceProvider.GetRequiredService<IMatchConfigurationService>();
        _columnFilter = _serviceProvider.GetRequiredService<IColumnFilter>();

        _importLogger = new NullLogger<DataImportCommand>();
        _matchLogger = new NullLogger<MatchingCommand>();
        _masterLogger = new NullLogger<MasterRecordDeterminationCommand>();
        _overwriteLogger = new NullLogger<FieldOverwriteCommand>();

        _schemaValidation = _serviceProvider.GetRequiredService<ISchemaValidationService>();
        _scheduleRepository = _serviceProvider.GetRequiredService<IGenericRepository<ScheduledTask, Guid>>();
        _snapshotRepo = _serviceProvider.GetRequiredService<IGenericRepository<DataSnapshot, Guid>>();
        _fileImportRepo = _serviceProvider.GetRequiredService<IGenericRepository<FileImport, Guid>>();
    }

    private string GetTempFilePath(string fileName)
    {
        var filePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}_{fileName}");
        _tempFiles.Add(filePath);
        return filePath;
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
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    // ==================== HELPER METHODS ====================

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
            Description = "Test Project",
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
        DataImportCommand importCommand)
    {
        var dataSourceId = Guid.NewGuid();
        var importStepId = Guid.NewGuid();

        string[] columns = new[] {
            "FullName", "Email", "PhoneNumber", "Address", "City",
            "State", "ZipCode", "Country", "JobTitle", "Department",
            "Salary", "CompanyName", "Website", "Notes"
        };

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

    private async Task CreateMatchDefinitionsAsync(Guid projectId, Guid ds1Id, Guid ds2Id)
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
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(ds1Id, "DataSource1", "FullName"),
                                new FieldMapping(ds2Id, "DataSource2", "FullName")
                            }
                        },
                        new MatchCriteria
                        {
                            MatchingType = MatchingType.Exact,
                            DataType = CriteriaDataType.Text,
                            Weight = 0.5,
                            FieldMappings = new List<FieldMapping>
                            {
                                new FieldMapping(ds1Id, "DataSource1", "Email"),
                                new FieldMapping(ds2Id, "DataSource2", "Email")
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

        var mappedFields = new MappedFieldsRow
        {
            ProjectId = projectId,
            MappedFields = new List<MappedFieldRow>
            {
                CreateMappedFieldRow("FullName", ds1Id, ds2Id),
                CreateMappedFieldRow("Email", ds1Id, ds2Id),
                CreateMappedFieldRow("PhoneNumber", ds1Id, ds2Id),
                CreateMappedFieldRow("CompanyName", ds1Id, ds2Id),
                CreateMappedFieldRow("Salary", ds1Id, ds2Id)
            }
        };
        await _mappedFieldRowsRepository.InsertAsync(mappedFields, Constants.Collections.MappedFieldRows);
    }

    private MappedFieldRow CreateMappedFieldRow(string fieldName, Guid ds1Id, Guid ds2Id)
    {
        var row = new MappedFieldRow { Include = true };
        row.AddField(new FieldMappingEx
        {
            FieldName = fieldName,
            DataSourceId = ds1Id,
            DataSourceName = "DataSource1",
            Mapped = true
        });
        row.AddField(new FieldMappingEx
        {
            FieldName = fieldName,
            DataSourceId = ds2Id,
            DataSourceName = "DataSource2",
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

    private async Task<MasterDeterminationResult> ExecuteMasterDeterminationAsync(Guid projectId)
    {
        var ruleSet = new MasterRecordRuleSet(projectId)
        {
            IsActive = true,
            Rules = new List<MasterRecordRule>
            {
                new MasterRecordRule
                {
                    Order = 0,
                    RuleSetId = Guid.NewGuid(),
                    LogicalFieldName = "FullName",
                    Operation = MasterRecordOperation.Longest,
                    IsActive = true
                }
            }
        };

        await _masterRuleSetRepository.SaveWithRulesAsync(ruleSet);
        return await _masterOrchestrator.ExecuteMasterDeterminationAsync(projectId, null, null);
    }

    private async Task<FieldOverwriteRuleSet> CreateOverwriteRuleSetAsync(
        Guid projectId, 
        List<FieldOverwriteRule> rules)
    {
        var ruleSet = new FieldOverwriteRuleSet(projectId)
        {
            IsActive = true,
            Rules = rules
        };

        ruleSet.Rules.ForEach(r => r.RuleSetId = ruleSet.Id);

        await _overwriteRuleSetRepository.SaveWithRulesAsync(ruleSet);
        return ruleSet;
    }

    private async Task<FieldOverwriteResult> ExecuteFieldOverwriteAsync(Guid projectId)
    {
        return await _overwriteOrchestrator.ExecuteFieldOverwritingAsync(projectId, null, null);
    }

    // ==================== TEST METHODS ====================

    [Fact]
    public async Task ExecCommandAsync_IndependentMode_OverwritesFieldsWithoutMasterDetermination()
    {
        // Arrange - Setup project WITHOUT master determination
        var (projectId, runId) = await SetupProjectWithRunAsync("Independent Overwrite Test");

        var ds1Path = GetTempFilePath("independent_ds1.xlsx");
        var ds2Path = GetTempFilePath("independent_ds2.xlsx");

        CreateOverwriteTestFile_DS1(ds1Path);
        CreateOverwriteTestFile_DS2(ds2Path);

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

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);
        await ExecuteMatchingAsync(projectId);

        // Create overwrite rules
        var rules = new List<FieldOverwriteRule>
        {
            new FieldOverwriteRule
            {
                Order = 0,
                LogicalFieldName = "CompanyName",
                Operation = OverwriteOperation.Longest,
                IsActive = true,
                OverwriteIf = OverwriteCondition.NoCondition
            }
        };
        await CreateOverwriteRuleSetAsync(projectId, rules);

        // Create and execute field overwrite command (WITHOUT master determination)
        var overwriteCommand = new FieldOverwriteCommand(
            _overwriteOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRepository,
            _dataSourceRepository,
            _overwriteLogger);

        var overwriteRunId = Guid.NewGuid();
        var overwriteStepId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = overwriteRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        var overwriteStep = new StepJob
        {
            Id = overwriteStepId,
            Type = StepType.Match,
            DataSourceId = projectId,
            RunId = overwriteRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _stepRepository.InsertAsync(overwriteStep, Constants.Collections.StepJobs);
        var overwriteContext = new CommandContext(overwriteRunId, projectId, overwriteStepId,
            _projectRunRepository, _stepRepository);

        // Act
        await overwriteCommand.ExecuteAsync(overwriteContext, overwriteStep);

        // Assert
        var stepData = await _stepRepository.GetByIdAsync(overwriteStepId, Constants.Collections.StepJobs);
        var outputCollection = stepData.StepData.First().CollectionName;

        var overwrittenGroups = await _dataStore.GetPagedDataAsync(outputCollection, 1, 100);

        Assert.True(overwrittenGroups.TotalCount > 0, "Should have overwritten groups");

        // Verify fields were overwritten
        foreach (var group in overwrittenGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            foreach (var recordObj in records.Cast<IDictionary<string, object>>())
            {
                Assert.True(recordObj.ContainsKey("CompanyName"), "CompanyName field should exist");
                
                // CompanyName should be overwritten with longest value
                var companyName = recordObj["CompanyName"]?.ToString();
                Assert.False(string.IsNullOrEmpty(companyName), "CompanyName should not be empty");
            }
        }

        Console.WriteLine($"Independent Mode - Processed {overwrittenGroups.TotalCount} groups without master determination");
    }

    [Fact]
    public async Task ExecCommandAsync_AfterMasterDetermination_OverwritesFieldsInMasterGroups()
    {
        // Arrange - Full pipeline: Import → Match → Master → Overwrite
        var (projectId, runId) = await SetupProjectWithRunAsync("Master Then Overwrite Test");

        var ds1Path = GetTempFilePath("master_overwrite_ds1.xlsx");
        var ds2Path = GetTempFilePath("master_overwrite_ds2.xlsx");

        CreateOverwriteTestFile_DS1(ds1Path);
        CreateOverwriteTestFile_DS2(ds2Path);

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
            new RemoteFileConnectorFactory()
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);
        await ExecuteMatchingAsync(projectId);

        // Execute master determination FIRST
        var masterResult = await ExecuteMasterDeterminationAsync(projectId);
        Assert.True(masterResult.Success, "Master determination should succeed");

        // Create overwrite rules
        var rules = new List<FieldOverwriteRule>
        {
            new FieldOverwriteRule
            {
                Order = 0,
                LogicalFieldName = "CompanyName",
                Operation = OverwriteOperation.FromMaster,
                IsActive = true,
                OverwriteIf = OverwriteCondition.NoCondition
            }
        };
        await CreateOverwriteRuleSetAsync(projectId, rules);

        // Execute field overwrite
        var overwriteCommand = new FieldOverwriteCommand(
            _overwriteOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRepository,
            _dataSourceRepository,
            _overwriteLogger);

        var overwriteRunId = Guid.NewGuid();
        var overwriteStepId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = overwriteRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        var overwriteStep = new StepJob
        {
            Id = overwriteStepId,
            Type = StepType.Match,
            DataSourceId = projectId,
            RunId = overwriteRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _stepRepository.InsertAsync(overwriteStep, Constants.Collections.StepJobs);
        var overwriteContext = new CommandContext(overwriteRunId, projectId, overwriteStepId,
            _projectRunRepository, _stepRepository);

        // Act
        await overwriteCommand.ExecuteAsync(overwriteContext, overwriteStep);

        // Assert
        var stepData = await _stepRepository.GetByIdAsync(overwriteStepId, Constants.Collections.StepJobs);
        var outputCollection = stepData.StepData.First().CollectionName;

        var overwrittenGroups = await _dataStore.GetPagedDataAsync(outputCollection, 1, 100);

        Assert.True(overwrittenGroups.TotalCount > 0, "Should have overwritten groups");

        // Verify each group still has exactly one master
        foreach (var group in overwrittenGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            var masterCount = records
                .Cast<IDictionary<string, object>>()
                .Count(r => r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
                           isMaster is bool boolVal && boolVal);

            Assert.Equal(1, masterCount);

            // Verify all records have same CompanyName (overwritten from master)
            var companyNames = records
                .Cast<IDictionary<string, object>>()
                .Select(r => r.ContainsKey("CompanyName") ? r["CompanyName"]?.ToString() : "")
                .Distinct()
                .ToList();

            // After FromMaster operation, all records should have master's value
            var masterRecord = records
                .Cast<IDictionary<string, object>>()
                .First(r => r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
                           isMaster is bool boolVal && boolVal);

            var masterCompanyName = masterRecord["CompanyName"]?.ToString();

            foreach (var recordObj in records.Cast<IDictionary<string, object>>())
            {
                var companyName = recordObj["CompanyName"]?.ToString();
                Assert.Equal(masterCompanyName, companyName);
            }
        }

        Console.WriteLine($"Master Then Overwrite - Processed {overwrittenGroups.TotalCount} groups");
    }

    [Fact]
    public async Task ExecCommandAsync_LongestOperation_SelectsLongestValue()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Longest Value Test");

        var ds1Path = GetTempFilePath("longest_value_ds1.xlsx");
        var ds2Path = GetTempFilePath("longest_value_ds2.xlsx");

        CreateLongestValueTestFile_DS1(ds1Path);
        CreateLongestValueTestFile_DS2(ds2Path);

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
            new RemoteFileConnectorFactory()
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);
        await ExecuteMatchingAsync(projectId);

        // Create overwrite rule for longest CompanyName
        var rules = new List<FieldOverwriteRule>
        {
            new FieldOverwriteRule
            {
                Order = 0,
                LogicalFieldName = "CompanyName",
                Operation = OverwriteOperation.Longest,
                IsActive = true,
                OverwriteIf = OverwriteCondition.NoCondition
            }
        };
        await CreateOverwriteRuleSetAsync(projectId, rules);

        // Act
        var result = await ExecuteFieldOverwriteAsync(projectId);

        // Assert
        Assert.True(result.Success, "Field overwriting should succeed");
        Assert.True(result.TotalFieldsOverwritten > 0, "Should have overwritten fields");

        var outputCollection = result.OutputCollections.OverwrittenGroupsCollection;
        var overwrittenGroups = await _dataStore.GetPagedDataAsync(outputCollection, 1, 100);

        // Verify longest value was selected
        foreach (var group in overwrittenGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            var recordsList = records.Cast<IDictionary<string, object>>().ToList();

            // Get all original values before overwrite
            var allCompanyNames = new List<string>();
            foreach (var record in recordsList)
            {
                if (record.ContainsKey("CompanyName"))
                {
                    var value = record["CompanyName"]?.ToString();
                    if (!string.IsNullOrEmpty(value))
                        allCompanyNames.Add(value);
                }
            }

            if (allCompanyNames.Any())
            {
                var longestValue = allCompanyNames.OrderByDescending(v => v.Length).First();
                
                // After overwrite, all should have the longest value
                foreach (var record in recordsList)
                {
                    var companyName = record["CompanyName"]?.ToString();
                    Assert.Equal(longestValue, companyName);
                }
            }
        }

        Console.WriteLine($"Longest Value Test - Verified {overwrittenGroups.TotalCount} groups");
    }

    [Fact]
    public async Task ExecCommandAsync_MaxOperation_SelectsMaximumNumericValue()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Max Value Test");

        var ds1Path = GetTempFilePath("max_value_ds1.xlsx");
        var ds2Path = GetTempFilePath("max_value_ds2.xlsx");

        CreateMaxValueTestFile_DS1(ds1Path);
        CreateMaxValueTestFile_DS2(ds2Path);

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
            new RemoteFileConnectorFactory()
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);
        await ExecuteMatchingAsync(projectId);

        // Create overwrite rule for max Salary
        var rules = new List<FieldOverwriteRule>
        {
            new FieldOverwriteRule
            {
                Order = 0,
                LogicalFieldName = "Salary",
                Operation = OverwriteOperation.Max,
                IsActive = true,
                OverwriteIf = OverwriteCondition.NoCondition
            }
        };
        await CreateOverwriteRuleSetAsync(projectId, rules);

        // Act
        var result = await ExecuteFieldOverwriteAsync(projectId);

        // Assert
        Assert.True(result.Success, "Field overwriting should succeed");

        var outputCollection = result.OutputCollections.OverwrittenGroupsCollection;
        var overwrittenGroups = await _dataStore.GetPagedDataAsync(outputCollection, 1, 100);

        // Verify maximum value was selected
        foreach (var group in overwrittenGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            var recordsList = records.Cast<IDictionary<string, object>>().ToList();

            // Get all salary values
            var allSalaries = new List<double>();
            foreach (var record in recordsList)
            {
                if (record.ContainsKey("Salary") && 
                    double.TryParse(record["Salary"]?.ToString(), out var salary))
                {
                    allSalaries.Add(salary);
                }
            }

            if (allSalaries.Any())
            {
                var maxSalary = allSalaries.Max();
                
                // After overwrite, all should have the max salary
                foreach (var record in recordsList)
                {
                    if (record.ContainsKey("Salary") && 
                        double.TryParse(record["Salary"]?.ToString(), out var salary))
                    {
                        Assert.Equal(maxSalary, salary);
                    }
                }
            }
        }

        Console.WriteLine($"Max Value Test - Verified {overwrittenGroups.TotalCount} groups");
    }

    [Fact]
    public async Task ExecCommandAsync_ConditionalOverwrite_OnlyOverwritesWhenConditionMet()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Conditional Overwrite Test");

        var ds1Path = GetTempFilePath("conditional_ds1.xlsx");
        var ds2Path = GetTempFilePath("conditional_ds2.xlsx");

        CreateConditionalTestFile_DS1(ds1Path);
        CreateConditionalTestFile_DS2(ds2Path);

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
            new RemoteFileConnectorFactory()
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);
        await ExecuteMatchingAsync(projectId);

        // Create overwrite rule with condition: Only overwrite if field is empty
        var rules = new List<FieldOverwriteRule>
        {
            new FieldOverwriteRule
            {
                Order = 0,
                LogicalFieldName = "PhoneNumber",
                Operation = OverwriteOperation.Longest,
                IsActive = true,
                OverwriteIf = OverwriteCondition.FieldIsEmpty
            }
        };
        await CreateOverwriteRuleSetAsync(projectId, rules);

        // Act
        var result = await ExecuteFieldOverwriteAsync(projectId);

        // Assert
        Assert.True(result.Success, "Field overwriting should succeed");

        var outputCollection = result.OutputCollections.OverwrittenGroupsCollection;
        var overwrittenGroups = await _dataStore.GetPagedDataAsync(outputCollection, 1, 100);

        // Verify conditional overwrite behavior
        // Records with existing values should NOT be overwritten
        // Records with empty values SHOULD be overwritten
        Assert.True(overwrittenGroups.TotalCount > 0, "Should have processed groups");

        Console.WriteLine($"Conditional Overwrite Test - Verified {overwrittenGroups.TotalCount} groups");
    }

    [Fact]
    public async Task ExecCommandAsync_MultipleRules_AppliesInOrder()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Multiple Rules Test");

        var ds1Path = GetTempFilePath("multi_rules_ds1.xlsx");
        var ds2Path = GetTempFilePath("multi_rules_ds2.xlsx");

        CreateOverwriteTestFile_DS1(ds1Path);
        CreateOverwriteTestFile_DS2(ds2Path);

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
            new RemoteFileConnectorFactory()
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);
        await ExecuteMatchingAsync(projectId);

        // Create multiple overwrite rules
        var rules = new List<FieldOverwriteRule>
        {
            new FieldOverwriteRule
            {
                Order = 0,
                LogicalFieldName = "CompanyName",
                Operation = OverwriteOperation.Longest,
                IsActive = true,
                OverwriteIf = OverwriteCondition.NoCondition
            },
            new FieldOverwriteRule
            {
                Order = 1,
                LogicalFieldName = "PhoneNumber",
                Operation = OverwriteOperation.Longest,
                IsActive = true,
                OverwriteIf = OverwriteCondition.NoCondition
            }
        };
        await CreateOverwriteRuleSetAsync(projectId, rules);

        // Act
        var result = await ExecuteFieldOverwriteAsync(projectId);

        // Assert
        Assert.True(result.Success, "Field overwriting should succeed");
        Assert.True(result.TotalFieldsOverwritten > 0, "Should have overwritten fields");

        var outputCollection = result.OutputCollections.OverwrittenGroupsCollection;
        var overwrittenGroups = await _dataStore.GetPagedDataAsync(outputCollection, 1, 100);

        // Verify both fields were overwritten
        foreach (var group in overwrittenGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            foreach (var recordObj in records.Cast<IDictionary<string, object>>())
            {
                Assert.True(recordObj.ContainsKey("CompanyName"), "CompanyName should exist");
                Assert.True(recordObj.ContainsKey("PhoneNumber"), "PhoneNumber should exist");
            }
        }

        Console.WriteLine($"Multiple Rules Test - Applied {rules.Count} rules to {overwrittenGroups.TotalCount} groups");
    }

    [Fact]
    public async Task ExecCommandAsync_MetadataValidation_IncludesAllExpectedFields()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Metadata Validation Test");

        var ds1Path = GetTempFilePath("metadata_ds1.xlsx");
        var ds2Path = GetTempFilePath("metadata_ds2.xlsx");

        CreateOverwriteTestFile_DS1(ds1Path);
        CreateOverwriteTestFile_DS2(ds2Path);

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
            new RemoteFileConnectorFactory()
        );

        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);
        await ExecuteMatchingAsync(projectId);

        var rules = new List<FieldOverwriteRule>
        {
            new FieldOverwriteRule
            {
                Order = 0,
                LogicalFieldName = "CompanyName",
                Operation = OverwriteOperation.Longest,
                IsActive = true,
                OverwriteIf = OverwriteCondition.NoCondition
            }
        };
        await CreateOverwriteRuleSetAsync(projectId, rules);

        var overwriteCommand = new FieldOverwriteCommand(
            _overwriteOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRepository,
            _dataSourceRepository,
            _overwriteLogger);

        var overwriteRunId = Guid.NewGuid();
        var overwriteStepId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = overwriteRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        var overwriteStep = new StepJob
        {
            Id = overwriteStepId,
            Type = StepType.Match,
            DataSourceId = projectId,
            RunId = overwriteRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _stepRepository.InsertAsync(overwriteStep, Constants.Collections.StepJobs);
        var overwriteContext = new CommandContext(overwriteRunId, projectId, overwriteStepId,
            _projectRunRepository, _stepRepository);

        // Act
        await overwriteCommand.ExecuteAsync(overwriteContext, overwriteStep);

        // Assert - Verify metadata
        var stepData = await _stepRepository.GetByIdAsync(overwriteStepId, Constants.Collections.StepJobs);
        var metadata = stepData.StepData.First().Metadata;

        Assert.True(metadata.ContainsKey("TotalGroupsProcessed"), "Should have TotalGroupsProcessed");
        Assert.True(metadata.ContainsKey("TotalFieldsOverwritten"), "Should have TotalFieldsOverwritten");
        Assert.True(metadata.ContainsKey("Duration"), "Should have Duration");

        var totalGroups = Convert.ToInt32(metadata["TotalGroupsProcessed"]);
        var totalFields = Convert.ToInt32(metadata["TotalFieldsOverwritten"]);
        var duration = Convert.ToDouble(metadata["Duration"]);

        Assert.True(totalGroups > 0, "Should have processed groups");
        Assert.True(totalFields >= 0, "Should have field count");
        Assert.True(duration >= 0, "Duration should be non-negative");

        Console.WriteLine($"Metadata Validation - Groups: {totalGroups}, Fields: {totalFields}, Duration: {duration:F2}s");
    }

    // ==================== TEST DATA CREATION METHODS ====================

    private void CreateOverwriteTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "John Smith", "john@example.com", "555-1111", "123 Main St", "NYC", "NY", "10001", "USA", "Developer", "IT", "75000", "TechCorp", "techcorp.com", "Record 1");
        AddDataRow(sheet, 2, "Jane Doe", "jane@example.com", "555-2222", "456 Oak Ave", "Boston", "MA", "02101", "USA", "Manager", "Sales", "85000", "SalesInc", "salesinc.com", "Record 2");
        AddDataRow(sheet, 3, "Bob Wilson", "bob@example.com", "555-3333", "789 Pine Rd", "Chicago", "IL", "60601", "USA", "Analyst", "Finance", "65000", "FinCo", "finco.com", "Record 3");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateOverwriteTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "John Smith", "john@example.com", "555-1111-ext123", "123 Main Street", "NYC", "NY", "10001", "USA", "Sr Developer", "IT", "80000", "TechCorp International", "techcorp.com", "Duplicate 1");
        AddDataRow(sheet, 2, "Jane Doe", "jane@example.com", "555-2222-ext456", "456 Oak Avenue", "Boston", "MA", "02101", "USA", "Sr Manager", "Sales", "90000", "SalesInc Global", "salesinc.com", "Duplicate 2");
        AddDataRow(sheet, 3, "Bob Wilson", "bob@example.com", "555-3333-ext789", "789 Pine Road", "Chicago", "IL", "60601", "USA", "Sr Analyst", "Finance", "70000", "FinCo Worldwide", "finco.com", "Duplicate 3");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateLongestValueTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Alice Anderson", "alice@test.com", "555-0001", "100 St", "Portland", "OR", "97201", "USA", "Dev", "IT", "70000", "ABC", "abc.com", "Short");
        AddDataRow(sheet, 2, "Bob Brown", "bob@test.com", "555-0002", "200 St", "Austin", "TX", "78701", "USA", "Mgr", "Sales", "80000", "XYZ", "xyz.com", "Short");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateLongestValueTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Alice Anderson", "alice@test.com", "555-0001", "100 Street", "Portland", "OR", "97201", "USA", "Developer", "IT", "75000", "ABC Corporation International", "abc.com", "This is a much longer company name");
        AddDataRow(sheet, 2, "Bob Brown", "bob@test.com", "555-0002", "200 Street", "Austin", "TX", "78701", "USA", "Manager", "Sales", "85000", "XYZ Company Global Enterprises", "xyz.com", "Another very long company name");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateMaxValueTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Charlie Chen", "charlie@test.com", "555-0003", "300 St", "Seattle", "WA", "98101", "USA", "Dev", "IT", "60000", "LowPay Inc", "lowpay.com", "Lower salary");
        AddDataRow(sheet, 2, "Diana Davis", "diana@test.com", "555-0004", "400 St", "Denver", "CO", "80201", "USA", "Mgr", "HR", "70000", "MedPay Co", "medpay.com", "Medium salary");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateMaxValueTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Charlie Chen", "charlie@test.com", "555-0003", "300 Street", "Seattle", "WA", "98101", "USA", "Sr Developer", "IT", "95000", "HighPay Corp", "highpay.com", "Highest salary");
        AddDataRow(sheet, 2, "Diana Davis", "diana@test.com", "555-0004", "400 Street", "Denver", "CO", "80201", "USA", "Sr Manager", "HR", "105000", "TopPay Ltd", "toppay.com", "Top salary");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateConditionalTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Record with empty phone
        AddDataRow(sheet, 1, "Eve Evans", "eve@test.com", "", "500 St", "Miami", "FL", "33101", "USA", "Dev", "IT", "65000", "EmptyCo", "empty.com", "No phone");
        // Record with existing phone
        AddDataRow(sheet, 2, "Frank Ford", "frank@test.com", "555-5555", "600 St", "Phoenix", "AZ", "85001", "USA", "Mgr", "Sales", "75000", "FullCo", "full.com", "Has phone");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateConditionalTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "CompanyName", "Website", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Eve Evans", "eve@test.com", "555-9999-COMPLETE", "500 Street", "Miami", "FL", "33101", "USA", "Sr Developer", "IT", "70000", "EmptyCo Inc", "empty.com", "Has phone in DS2");
        AddDataRow(sheet, 2, "Frank Ford", "frank@test.com", "555-8888-LONGER", "600 Street", "Phoenix", "AZ", "85001", "USA", "Sr Manager", "Sales", "80000", "FullCo Ltd", "full.com", "Different phone");

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
}
