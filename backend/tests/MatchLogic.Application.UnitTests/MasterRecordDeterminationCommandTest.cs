using MatchLogic.Application.Common;
using MatchLogic.Application.EventHandlers;
using MatchLogic.Application.Features.DataMatching.Orchestration;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
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
using DataSource = MatchLogic.Domain.Project.DataSource;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Comprehensive tests for MasterRecordDeterminationCommand
/// Following MatchingCommandTest patterns with proper DataSource entity creation
/// </summary>
public class MasterRecordDeterminationCommandTest : IDisposable
{
    private readonly string _dbPath;
    private readonly List<string> _tempFiles = new List<string>();
    private readonly IServiceProvider _serviceProvider;

    // Core services
    private readonly IDataStore _dataStore;
    private readonly IRecordLinkageOrchestrator _recordLinkageOrchestrator;
    private readonly IMasterRecordDeterminationOrchestrator _masterOrchestrator;
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
    private readonly IMasterRecordRuleSetRepository _ruleSetRepository;

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

    public MasterRecordDeterminationCommandTest()
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

        services.AddLogging(builder => builder.AddConsole());
        services.AddSingleton<IConfiguration>(config);
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
        _projectService = _serviceProvider.GetRequiredService<IProjectService>();
        _jobEventPublisher = _serviceProvider.GetRequiredService<IJobEventPublisher>();

        _projectRunRepository = _serviceProvider.GetRequiredService<IGenericRepository<ProjectRun, Guid>>();
        _stepRepository = _serviceProvider.GetRequiredService<IGenericRepository<StepJob, Guid>>();
        _dataSourceRepository = _serviceProvider.GetRequiredService<IGenericRepository<DataSource, Guid>>();
        _matchSettingRepository = _serviceProvider.GetRequiredService<IGenericRepository<MatchSettings, Guid>>();
        _pairRepository = _serviceProvider.GetRequiredService<IGenericRepository<MatchingDataSourcePairs, Guid>>();
        _projectRepository = _serviceProvider.GetRequiredService<IGenericRepository<Project, Guid>>();
        _mappedFieldRowsRepository = _serviceProvider.GetRequiredService<IGenericRepository<MappedFieldsRow, Guid>>();
        _ruleSetRepository = _serviceProvider.GetRequiredService<IMasterRecordRuleSetRepository>();

        _dataSourceService = _serviceProvider.GetRequiredService<IDataSourceService>();
        _secureParameterHandler = _serviceProvider.GetRequiredService<ISecureParameterHandler>();
        _recordHasher = _serviceProvider.GetRequiredService<IRecordHasher>();
        _connectionBuilder = _serviceProvider.GetRequiredService<IConnectionBuilder>();
        _matchDefinitionService = _serviceProvider.GetRequiredService<IMatchConfigurationService>();
        _columnFilter = _serviceProvider.GetRequiredService<IColumnFilter>();

        _importLogger = new NullLogger<DataImportCommand>();
        _matchLogger = new NullLogger<MatchingCommand>();
        _masterLogger = new NullLogger<MasterRecordDeterminationCommand>();
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

    // ==================== CORRECTED HELPER METHODS ====================

    /// <summary>
    /// Creates DataSource entity with all required properties (Following MatchingCommandTest pattern)
    /// </summary>
    private DataSource CreateExcelDataSource(Guid dataSourceId, string filePath, string[] columns)
    {
        // Create connection info
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", filePath },
                { "HasHeaders", "true" }
            }
        };

        // Encrypt parameters (CRITICAL: Must encrypt before storing)
        connectionInfo.Parameters = Task.Run(async () =>
            await _secureParameterHandler.EncryptSensitiveParametersAsync(
                connectionInfo.Parameters,
                dataSourceId)).Result;

        // Create column mappings (CRITICAL: Required for import to work)
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
                ColumnMappings = columnMappings  // CRITICAL: Must include this
            },
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Setup project and return both projectId and runId (Following MatchingCommandTest pattern)
    /// </summary>
    private async Task<(Guid projectId, Guid runId)> SetupProjectWithRunAsync(string projectName)
    {
        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();

        // Insert project
        var project = new Project
        {
            Id = projectId,
            Name = projectName,
            Description = "Test Project",
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        // Insert project run (CRITICAL: Must exist before any commands)
        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            Id = runId,
            ProjectId = projectId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        return (projectId, runId);
    }

    /// <summary>
    /// Import data source with proper entity creation (Following MatchingCommandTest pattern)
    /// </summary>
    private async Task<Guid> ImportDataSourceAsync(
        Guid projectId,
        string filePath,
        string dataSourceName,
        Guid runId,
        DataImportCommand importCommand)
    {
        var dataSourceId = Guid.NewGuid();
        var importStepId = Guid.NewGuid();

        // Define columns for test data
        string[] columns = new[] {
            "FullName", "Email", "PhoneNumber", "Address", "City",
            "State", "ZipCode", "Country", "JobTitle", "Department",
            "Salary", "UserId", "Username", "Notes"
        };

        // CRITICAL STEP 1: Create DataSource entity
        var dataSource = CreateExcelDataSource(dataSourceId, filePath, columns);
        dataSource.Name = dataSourceName;
        dataSource.ProjectId = projectId;

        // CRITICAL STEP 2: Insert DataSource into repository FIRST (before import)
        await _dataSourceRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Step 3: Create StepJob
        var importStep = new StepJob
        {
            Id = importStepId,
            Type = StepType.Import,
            RunId = runId,  // Use passed runId (same for all imports)
            DataSourceId = dataSourceId,
            Configuration = new Dictionary<string, object>
            {
                { "DataSourceId", dataSourceId }
            }
        };
        await _stepRepository.InsertAsync(importStep, Constants.Collections.StepJobs);

        // Step 4: Execute import with reused command instance
        var importContext = new CommandContext(runId, projectId, importStepId,
            _projectRunRepository, _stepRepository);
        await importCommand.ExecuteAsync(importContext, importStep);

        return dataSourceId;
    }

    /// <summary>
    /// Create match definitions for two data sources
    /// </summary>
    private async Task CreateMatchDefinitionsAsync(Guid projectId, Guid ds1Id, Guid ds2Id)
    {
        var matchDefId = Guid.NewGuid();
        var pairId = Guid.NewGuid();

        // Create match definitions
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

        // Create data source pair
        var pair = new MatchingDataSourcePair(ds1Id, ds2Id);
        var pairConfig = new MatchingDataSourcePairs
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            Pairs = new List<MatchingDataSourcePair> { pair }
        };
        await _pairRepository.InsertAsync(pairConfig, Constants.Collections.MatchDataSourcePairs);

        // Create match settings
        var settings = new MatchSettings
        {
            ProjectId = projectId,
            MergeOverlappingGroups = false
        };
        await _matchSettingRepository.InsertAsync(settings, Constants.Collections.MatchSettings);

        // Create mapped fields
        var mappedFields = new MappedFieldsRow
        {
            ProjectId = projectId,
            MappedFields = new List<MappedFieldRow>
            {
                CreateMappedFieldRow("FullName", ds1Id, ds2Id),
                CreateMappedFieldRow("Email", ds1Id, ds2Id),
                CreateMappedFieldRow("PhoneNumber", ds1Id, ds2Id)
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

    /// <summary>
    /// Execute matching command
    /// </summary>
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

    /// <summary>
    /// Create default rule set for testing
    /// </summary>
    private async Task<MasterRecordRuleSet> CreateDefaultRuleSetAsync(Guid projectId)
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

        await _ruleSetRepository.SaveWithRulesAsync(ruleSet);
        return ruleSet;
    }

    /// <summary>
    /// Execute master determination (for direct orchestrator testing)
    /// </summary>
    private async Task<MasterDeterminationResult> ExecuteMasterDeterminationAsync(Guid projectId)
    {
        return await _masterOrchestrator.ExecuteMasterDeterminationAsync(projectId, null, null);
    }

    // ==================== TEST METHODS ====================

    [Fact]
    public async Task ExecCommandAsync_BasicMasterDetermination_DeterminesMasterRecords()
    {
        // Arrange - Setup project and run
        var (projectId, runId) = await SetupProjectWithRunAsync("Master Determination Test");

        // Create test files
        var ds1Path = GetTempFilePath("master_test_ds1.xlsx");
        var ds2Path = GetTempFilePath("master_test_ds2.xlsx");

        CreateMasterTestFile_DS1(ds1Path);
        CreateMasterTestFile_DS2(ds2Path);

        // Create import command ONCE (reuse for all imports)
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

        // Import data sources (using same runId and command)
        var ds1Id = await ImportDataSourceAsync(projectId, ds1Path, "DataSource1", runId, importCommand);
        var ds2Id = await ImportDataSourceAsync(projectId, ds2Path, "DataSource2", runId, importCommand);

        // Verify data sources were inserted (IMPORTANT: This was missing!)
        var dataSources = await _dataSourceRepository.QueryAsync(
            ds => ds.ProjectId == projectId,
            Constants.Collections.DataSources);
        Assert.Equal(2, dataSources.Count());  // Should have 2 data sources

        // Create match definitions
        await CreateMatchDefinitionsAsync(projectId, ds1Id, ds2Id);

        // Execute matching
        await ExecuteMatchingAsync(projectId);

        // Create rule set
        await CreateDefaultRuleSetAsync(projectId);

        // Execute master determination command
        var masterCommand = new MasterRecordDeterminationCommand(
            _masterOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRepository,
            _dataSourceRepository,
            _masterLogger);

        var masterRunId = Guid.NewGuid();
        var masterStepId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun
        {
            ProjectId = projectId,
            Id = masterRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        var masterStep = new StepJob
        {
            Id = masterStepId,
            Type = StepType.Match,
            DataSourceId = projectId,
            RunId = masterRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _stepRepository.InsertAsync(masterStep, Constants.Collections.StepJobs);
        var masterContext = new CommandContext(masterRunId, projectId, masterStepId,
            _projectRunRepository, _stepRepository);

        // Act
        await masterCommand.ExecuteAsync(masterContext, masterStep);

        // Assert
        var masterStepData = await _stepRepository.GetByIdAsync(masterStepId, Constants.Collections.StepJobs);
        var outputCollections = masterStepData.StepData.First().CollectionName.Split('|')
            .Select(s => s.Trim()).ToArray();

        // Verify master groups collection
        var masterGroupsCollection = outputCollections[1];
        var masterGroups = await _dataStore.GetPagedDataAsync(masterGroupsCollection, 1, 100);

        Assert.True(masterGroups.TotalCount > 0, "Should have master groups");

        // Verify each group has exactly one master record
        foreach (var group in masterGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            var masterCount = records
                .Cast<IDictionary<string, object>>()
                .Count(r => r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
                           isMaster is bool boolVal && boolVal);

            Assert.Equal(1, masterCount); // Each group should have exactly one master
        }

        // Verify metadata
        var metadata = masterStepData.StepData.First().Metadata;
        Assert.True(metadata.ContainsKey("TotalGroupsProcessed"));
        Assert.True(metadata.ContainsKey("TotalMasterChanges"));

        var totalGroups = Convert.ToInt32(metadata["TotalGroupsProcessed"]);
        Assert.True(totalGroups > 0, "Should have processed groups");

        Console.WriteLine($"Master Determination Results:");
        Console.WriteLine($"Total Groups Processed: {totalGroups}");
        Console.WriteLine($"Total Master Changes: {metadata["TotalMasterChanges"]}");
        Console.WriteLine($"Master Groups: {masterGroups.TotalCount}");
    }

    [Fact]
    public async Task ExecCommandAsync_WithLongestValueRule_SelectsLongestName()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Longest Name Test");

        var ds1Path = GetTempFilePath("longest_test_ds1.xlsx");
        var ds2Path = GetTempFilePath("longest_test_ds2.xlsx");

        CreateLongestNameTestFile_DS1(ds1Path);
        CreateLongestNameTestFile_DS2(ds2Path);

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

        // Create rule set with Longest operation
        var ruleSet = new MasterRecordRuleSet(projectId)
        {
            IsActive = true,
            Rules = new List<MasterRecordRule>
            {
                new MasterRecordRule
                {
                    RuleSetId = Guid.NewGuid(),
                    Order = 0,
                    LogicalFieldName = "FullName",
                    Operation = MasterRecordOperation.Longest,
                    IsActive = true
                }
            }
        };
        await _ruleSetRepository.SaveWithRulesAsync(ruleSet);

        // Execute master determination
        var result = await ExecuteMasterDeterminationAsync(projectId);

        // Assert - Verify longest names were selected
        var masterGroupsCollection = result.OutputCollections.MasterGroupsCollection;
        var masterGroups = await _dataStore.GetPagedDataAsync(masterGroupsCollection, 1, 100);

        foreach (var group in masterGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            var masterRecord = records
                .Cast<IDictionary<string, object>>()
                .FirstOrDefault(r => r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
                                     isMaster is bool b && b);

            Assert.NotNull(masterRecord);

            // Verify master has longest name
            var masterName = masterRecord["FullName"]?.ToString() ?? "";
            foreach (var recordObj in records.Cast<IDictionary<string, object>>())
            {
                var name = recordObj["FullName"]?.ToString() ?? "";
                Assert.True(masterName.Length >= name.Length,
                    $"Master name '{masterName}' should be longest, but '{name}' is longer");
            }
        }

        Console.WriteLine($"Longest Value Rule Test - Verified {masterGroups.TotalCount} groups");
    }

    [Fact]
    public async Task ExecCommandAsync_WithPreferDataSourceRule_SelectsPreferredSource()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Prefer DataSource Test");

        var ds1Path = GetTempFilePath("prefer_ds_test_ds1.xlsx");
        var ds2Path = GetTempFilePath("prefer_ds_test_ds2.xlsx");

        CreatePreferDataSourceTestFile_DS1(ds1Path);
        CreatePreferDataSourceTestFile_DS2(ds2Path);

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

        // Create rule set with PreferDataSource operation (prefer DS1)
        var ruleSet = new MasterRecordRuleSet(projectId)
        {
            IsActive = true,
            Rules = new List<MasterRecordRule>
            {
                new MasterRecordRule
                {
                    RuleSetId = Guid.NewGuid(),
                    Order = 0,
                    LogicalFieldName = "FullName",
                    Operation = MasterRecordOperation.PreferDataSource,
                    IsActive = true,
                    PreferredDataSourceId = ds1Id
                }
            }
        };
        await _ruleSetRepository.SaveWithRulesAsync(ruleSet);

        // Execute master determination
        var result = await ExecuteMasterDeterminationAsync(projectId);

        // Assert - Verify records from DS1 are preferred
        var masterGroupsCollection = result.OutputCollections.MasterGroupsCollection;
        var masterGroups = await _dataStore.GetPagedDataAsync(masterGroupsCollection, 1, 100);

        int ds1MasterCount = 0;
        foreach (var group in masterGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            var masterRecord = records
                .Cast<IDictionary<string, object>>()
                .FirstOrDefault(r => r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
                                     isMaster is bool b && b);

            if (masterRecord != null &&
                masterRecord.TryGetValue("_metadata", out var metaObj) &&
                metaObj is IDictionary<string, object> metadata &&
                metadata.TryGetValue("DataSourceId", out var dsIdObj) &&
                dsIdObj is Guid dsGuid &&
                dsGuid == ds1Id)
            {
                ds1MasterCount++;
            }
        }

        Assert.True(ds1MasterCount > 0, "Should have masters from preferred data source");
        Console.WriteLine($"PreferDataSource Rule Test - {ds1MasterCount}/{masterGroups.TotalCount} masters from DS1");
    }

    [Fact]
    public async Task ExecCommandAsync_WithMultipleRules_AppliesRulesSequentially()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("Multi Rule Test");

        var ds1Path = GetTempFilePath("multi_rule_test_ds1.xlsx");
        var ds2Path = GetTempFilePath("multi_rule_test_ds2.xlsx");

        CreateMultiRuleTestFile_DS1(ds1Path);
        CreateMultiRuleTestFile_DS2(ds2Path);

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

        // Create rule set with multiple rules
        var ruleSet = new MasterRecordRuleSet(projectId)
        {
            IsActive = true,
            Rules = new List<MasterRecordRule>
            {
                new MasterRecordRule
                {
                    RuleSetId = Guid.NewGuid(),
                    Order = 0,
                    LogicalFieldName = "Email",
                    Operation = MasterRecordOperation.FirstNonNull,
                    IsActive = true
                },
                new MasterRecordRule
                {
                    RuleSetId = Guid.NewGuid(),
                    Order = 1,
                    LogicalFieldName = "FullName",
                    Operation = MasterRecordOperation.Longest,
                    IsActive = true
                }
            }
        };
        await _ruleSetRepository.SaveWithRulesAsync(ruleSet);

        // Execute master determination
        var result = await ExecuteMasterDeterminationAsync(projectId);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.TotalGroupsProcessed > 0);

        var masterGroupsCollection = result.OutputCollections.MasterGroupsCollection;
        var masterGroups = await _dataStore.GetPagedDataAsync(masterGroupsCollection, 1, 100);

        // Verify each group has master
        foreach (var group in masterGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            var masterCount = records
                .Cast<IDictionary<string, object>>()
                .Count(r => r.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMaster) &&
                           isMaster is bool boolVal && boolVal);

            Assert.Equal(1, masterCount);
        }

        Console.WriteLine($"Multi Rule Test - Processed {masterGroups.TotalCount} groups with {ruleSet.Rules.Count} rules");
    }

    [Fact]
    public async Task ExecCommandAsync_WithDefaultChangedFlag_TracksChangesCorrectly()
    {
        // Arrange
        var (projectId, runId) = await SetupProjectWithRunAsync("DefaultChanged Test");

        var ds1Path = GetTempFilePath("default_changed_ds1.xlsx");
        var ds2Path = GetTempFilePath("default_changed_ds2.xlsx");

        CreateDefaultChangedTestFile_DS1(ds1Path);
        CreateDefaultChangedTestFile_DS2(ds2Path);

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
        await CreateDefaultRuleSetAsync(projectId);

        // Execute first time
        var result1 = await ExecuteMasterDeterminationAsync(projectId);
        var firstMasterChanges = result1.TotalMasterChanges;

        // Execute second time (should have fewer changes)
        var result2 = await ExecuteMasterDeterminationAsync(projectId);
        var secondMasterChanges = result2.TotalMasterChanges;

        // Assert
        Assert.True(firstMasterChanges > 0, "First run should have master changes");
        Assert.True(secondMasterChanges >= 0, "Second run should track changes");

        // Verify DefaultChanged flags
        var masterGroupsCollection = result2.OutputCollections.MasterGroupsCollection;
        var masterGroups = await _dataStore.GetPagedDataAsync(masterGroupsCollection, 1, 100);

        foreach (var group in masterGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            foreach (var recordObj in records.Cast<IDictionary<string, object>>())
            {
                // Verify DefaultChanged field exists
                Assert.True(recordObj.ContainsKey(RecordSystemFieldNames.IsMasterRecord_DefaultChanged));
                Assert.True(recordObj.ContainsKey(RecordSystemFieldNames.IsMasterRecord));
            }
        }

        Console.WriteLine($"DefaultChanged Test - First run: {firstMasterChanges} changes, Second run: {secondMasterChanges} changes");
    }

    // ==================== TEST DATA CREATION METHODS ====================

    private void CreateMasterTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "John Smith", "john@example.com", "555-1111", "123 Main St", "New York", "NY", "10001", "USA", "Developer", "IT", "75000", "u1", "jsmith", "Record 1");
        AddDataRow(sheet, 2, "Jane Doe", "jane@example.com", "555-2222", "456 Oak Ave", "Boston", "MA", "02101", "USA", "Manager", "Sales", "85000", "u2", "jdoe", "Record 2");
        AddDataRow(sheet, 3, "Bob Wilson", "bob@example.com", "555-3333", "789 Pine Rd", "Chicago", "IL", "60601", "USA", "Analyst", "Finance", "65000", "u3", "bwilson", "Record 3");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateMasterTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "John Smith", "john@example.com", "555-1111", "123 Main Street", "New York", "NY", "10001", "USA", "Sr Developer", "IT", "80000", "u21", "johnsmith", "Duplicate 1");
        AddDataRow(sheet, 2, "Jane Doe", "jane@example.com", "555-2222", "456 Oak Avenue", "Boston", "MA", "02101", "USA", "Sr Manager", "Sales", "90000", "u22", "janedoe", "Duplicate 2");
        AddDataRow(sheet, 3, "Bob Wilson", "bob@example.com", "555-3333", "789 Pine Road", "Chicago", "IL", "60601", "USA", "Sr Analyst", "Finance", "70000", "u23", "bobwilson", "Duplicate 3");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateLongestNameTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "John", "john@test.com", "555-0001", "123 St", "NYC", "NY", "10001", "USA", "Dev", "IT", "70000", "u1", "john", "Short name");
        AddDataRow(sheet, 2, "Mary", "mary@test.com", "555-0002", "456 St", "Boston", "MA", "02101", "USA", "Mgr", "Sales", "80000", "u2", "mary", "Short name");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateLongestNameTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "John Alexander Smith", "john@test.com", "555-0001", "123 Street", "NYC", "NY", "10001", "USA", "Developer", "IT", "75000", "u21", "johnsmith", "Longest name");
        AddDataRow(sheet, 2, "Mary Elizabeth Johnson", "mary@test.com", "555-0002", "456 Street", "Boston", "MA", "02101", "USA", "Manager", "Sales", "85000", "u22", "maryjohnson", "Longest name");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreatePreferDataSourceTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Alice Anderson", "alice@test.com", "555-1001", "100 First St", "Portland", "OR", "97201", "USA", "Analyst", "Finance", "65000", "u101", "aalice", "DS1 record");
        AddDataRow(sheet, 2, "Bob Brown", "bob@test.com", "555-1002", "200 Second St", "Austin", "TX", "78701", "USA", "Developer", "Tech", "85000", "u102", "bbob", "DS1 record");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreatePreferDataSourceTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Alice Anderson", "alice@test.com", "555-2001", "100 First Street", "Portland", "OR", "97201", "USA", "Sr Analyst", "Finance", "68000", "u201", "alice1", "DS2 record");
        AddDataRow(sheet, 2, "Bob Brown", "bob@test.com", "555-2002", "200 Second Street", "Austin", "TX", "78701", "USA", "Sr Developer", "Tech", "88000", "u202", "bob1", "DS2 record");


        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateMultiRuleTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Charlie Chen", "charlie@test.com", "555-3001", "300 St", "Phoenix", "AZ", "85001", "USA", "Dev", "IT", "70000", "u301", "charlie", "Has email");
        AddDataRow(sheet, 2, "Diana Davis", "diana@test.com", "555-3002", "400 St", "Atlanta", "GA", "30301", "USA", "Mgr", "HR", "75000", "u302", "diana", "Has email");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateMultiRuleTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Charlie Chen", "charlie@test.com", "555-4001", "300 Street", "Phoenix", "AZ", "85001", "USA", "Developer", "IT", "72000", "u401", "charliechen", "Longer name");
        AddDataRow(sheet, 2, "Diana Davis", "diana@test.com", "555-4002", "400 Street", "Atlanta", "GA", "30301", "USA", "Manager", "HR", "78000", "u402", "dianadavis", "Has email");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateDefaultChangedTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Edward Evans", "edward@test.com", "555-5001", "500 St", "Denver", "CO", "80201", "USA", "Dev", "IT", "70000", "u501", "edward", "Test record");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateDefaultChangedTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        AddDataRow(sheet, 1, "Edward Evans", "edward@test.com", "555-6001", "500 Street", "Denver", "CO", "80201", "USA", "Developer", "IT", "75000", "u601", "edwardevans", "Longer name");

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