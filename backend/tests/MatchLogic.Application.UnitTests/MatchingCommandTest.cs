using Moq;
using MatchLogic.Application.Common;
using MatchLogic.Application.EventHandlers;
using MatchLogic.Application.Features.DataMatching.Orchestration;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Project.Commands;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPOI.POIFS.NIO;
using NPOI.SS.UserModel;
using NPOI.SS.UserModel.Charts;
using NPOI.XSSF.UserModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataSource = MatchLogic.Domain.Project.DataSource;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Application.UnitTests;

public class MatchingCommandTest : IDisposable
{
    private readonly string _dbPath;
    private readonly List<string> _tempFiles = new List<string>();
    private ILogger<DataCleansingCommand> _logger;
    private ILogger<DataImportCommand> _Dlogger;
    private ILogger<MatchingCommand> _mlogger;
    private readonly IServiceProvider _serviceProvider;

    private readonly IDataStore _dataStore;
    private readonly IRecordLinkageOrchestrator _recordLinkageOrchestrator;
    private readonly IProjectService _projectService;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepRespository;
    ////private readonly IGenericRepository<EnhancedCleaningRules, Guid> _cleaningRulesRepository;
    private readonly IGenericRepository<DomainDataSource, Guid> _domainDataSourceRepository;
    private readonly IDataSourceService _dataSourceService;
    private readonly IGenericRepository<DataSource, Guid> _genericRepository;
    private readonly IGenericRepository<MatchSettings, Guid> _matchSetting;
    private readonly IGenericRepository<MatchingDataSourcePairs, Guid> _mockPairRepo;

    private readonly IRecordHasher _recordHasher;
    private readonly ISecureParameterHandler _secureParameterHandler;
    private readonly IColumnFilter _columnFilter;
    private readonly IGenericRepository<Project, Guid> _projectRepository;
    private readonly IMatchConfigurationService _matchDefinitionService;
    private readonly IConnectionBuilder _connectionBuilder;


    public MatchingCommandTest()
    {
        _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();
        _logger = new NullLogger<DataCleansingCommand>();

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



        services.AddMediatR(cfg => {
            cfg.RegisterServicesFromAssembly(typeof(DatabaseUpdateEventHandler).Assembly);
        });
        _serviceProvider = services.BuildServiceProvider();

        _dataStore = _serviceProvider.GetService<IDataStore>();

        //_cleaningRulesRepository = _serviceProvider.GetService<IGenericRepository<EnhancedCleaningRules, Guid>>();

        _jobEventPublisher = _serviceProvider.GetService<IJobEventPublisher>();

        _projectRunRepository = _serviceProvider.GetService<IGenericRepository<ProjectRun, Guid>>();
        _matchSetting = _serviceProvider.GetService<IGenericRepository<MatchSettings, Guid>>();
        _stepRespository = _serviceProvider.GetService<IGenericRepository<StepJob, Guid>>();
        _recordLinkageOrchestrator = _serviceProvider.GetService<IRecordLinkageOrchestrator>();
        _projectService = _serviceProvider.GetService<IProjectService>();
        _domainDataSourceRepository = _serviceProvider.GetService<IGenericRepository<DomainDataSource, Guid>>();
        _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        _genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        _mockPairRepo = _serviceProvider.GetService<IGenericRepository<MatchingDataSourcePairs, Guid>>();
        _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();

        _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        _Dlogger = new NullLogger<DataImportCommand>();
        _mlogger = new NullLogger<MatchingCommand>();
        _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();
        _matchDefinitionService = _serviceProvider.GetService<IMatchConfigurationService>(); // Add this service

    }

    private string GetTempFilePath(string fileName)
    {
        var filePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}_{fileName}");
        _tempFiles.Add(filePath);
        return filePath;
    }

    public void Dispose()
    {
        // Clean up all tracked files
        foreach (var file in _tempFiles)
        {
            try
            {
                if (File.Exists(file))
                {
                    File.Delete(file);
                }
            }
            catch
            {
                // Ignore cleanup errors - we tried our best
            }
        }
    }
    [Fact]
    public async Task ExecCommandAsync_WithCleansingAndMatching_TestsFullPipeline()
    {
        // Arrange - Setup file paths and services
        var filePath1 = GetTempFilePath("testMatch.xlsx");
        var filePath2 = GetTempFilePath("testMatch2.xlsx");
        var filePath3 = GetTempFilePath("testMatch3.xlsx");



        // Initialize commands
        DataImportCommand dataImportCommand = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _genericRepository,
            _recordHasher,
            _Dlogger,
            _dataStore,
            _projectRunRepository,
            _stepRespository,
            _connectionBuilder,
            _columnFilter,
            _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory()
        );

        // Setup IDs
        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();


        // Create data source IDs for 3 sources
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid(),
            ["DS3"] = Guid.NewGuid()
        };

        // Create project
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project with Matching",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        await _matchSetting.InsertAsync(new MatchSettings() { MergeOverlappingGroups = true, ProjectId = projectId }, Constants.Collections.MatchSettings);
        // Create project run
        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        // ===== STEP 1: Create and Import Data Sources =====

        // Create test Excel files with similar structure but slightly different data
        CreateComprehensiveTestExcelFile(filePath1);
        CreateComprehensiveTestExcelFileVariation2(filePath2);
        CreateComprehensiveTestExcelFileVariation3(filePath3);

        // Create and insert data sources
        var dataSource1 = CreateComprehensiveExcelDataSource(dataSources["DS1"], filePath1);
        dataSource1.Name = "DS1";
        dataSource1.ProjectId = projectId;

        await _genericRepository.InsertAsync(dataSource1, Constants.Collections.DataSources);

        var ds1 = await _genericRepository.QueryAsync(x => x.ProjectId == projectId, Constants.Collections.DataSources);

        var dataSource2 = CreateComprehensiveExcelDataSource(dataSources["DS2"], filePath2);
        dataSource2.Name = "DS2";
        dataSource2.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource2, Constants.Collections.DataSources);

        var dataSource3 = CreateComprehensiveExcelDataSource(dataSources["DS3"], filePath3);
        dataSource3.Name = "DS3";
        dataSource3.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource3, Constants.Collections.DataSources);

        // Import each data source
        foreach (var ds in dataSources)
        {
            var importStepId = Guid.NewGuid();
            var importStep = new StepJob
            {
                Id = importStepId,
                Type = StepType.Import,
                RunId = runId,
                DataSourceId = ds.Value,
                Configuration = new Dictionary<string, object>
            {
                { "DataSourceId", ds.Value }
            }
            };
            await _stepRespository.InsertAsync(importStep, Constants.Collections.StepJobs);

            var importContext = new CommandContext(runId, projectId, importStepId, _projectRunRepository, _stepRespository);
            await dataImportCommand.ExecuteAsync(importContext, importStep);
        }

        // ===== STEP 2: Setup Match Definitions =====

        // Create data source pairs collection
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add all pairs
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        pairsCollection.Add(dataSources["DS1"], dataSources["DS3"]);
        pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Create match definition with mapped rows
        var matchJobId = Guid.NewGuid();
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = matchJobId,
            Name = "Comprehensive Match Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
        {
            // First definition - Email and Name matching
            new MatchDefinitionMappedRowDto
            {
                ProjectRunId = runId,
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    // Exact match on Email (highest weight)
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 0.7, // High weight for email
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "Email"
                                },
                                ["DS2"] = new FieldDto {
                                    DataSourceId = dataSources["DS2"],
                                    DataSourceName = "DS2",
                                    Name = "Email"
                                },
                                ["DS3"] = new FieldDto {
                                    DataSourceId = dataSources["DS3"],
                                    DataSourceName = "DS3",
                                    Name = "Email"
                                }
                            }
                        }
                    },
                    // Fuzzy match on FullName
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Fuzzy,
                        DataType = CriteriaDataType.Text,
                        Weight = 0.3,
                        Arguments = new Dictionary<ArgsValue, string> {[ArgsValue.FastLevel]="0.3", [ArgsValue.Level] = "0.7" }, // 80% similarity
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "FullName"
                                },
                                ["DS2"] = new FieldDto {
                                    DataSourceId = dataSources["DS2"],
                                    DataSourceName = "DS2",
                                    Name = "FullName"
                                },
                                ["DS3"] = new FieldDto {
                                    DataSourceId = dataSources["DS3"],
                                    DataSourceName = "DS3",
                                    Name = "FullName"
                                }
                            }
                        }
                    }
                }
            },
            // Second definition - Address and ZipCode matching
            new MatchDefinitionMappedRowDto
            {
                ProjectRunId = runId,
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    // Fuzzy match on Address
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Fuzzy,
                        DataType = CriteriaDataType.Text,
                        Weight = 0.5,
                        Arguments = new Dictionary<ArgsValue, string> {[ArgsValue.FastLevel]="0.3", [ArgsValue.Level] = "0.7" }, // Lower threshold
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "Address"
                                },
                                ["DS2"] = new FieldDto {
                                    DataSourceId = dataSources["DS2"],
                                    DataSourceName = "DS2",
                                    Name = "Address"
                                },
                                ["DS3"] = new FieldDto {
                                    DataSourceId = dataSources["DS3"],
                                    DataSourceName = "DS3",
                                    Name = "Address"
                                }
                            }
                        }
                    },
                    // Exact match on ZipCode
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 0.5,
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "ZipCode"
                                },
                                ["DS2"] = new FieldDto {
                                    DataSourceId = dataSources["DS2"],
                                    DataSourceName = "DS2",
                                    Name = "ZipCode"
                                },
                                ["DS3"] = new FieldDto {
                                    DataSourceId = dataSources["DS3"],
                                    DataSourceName = "DS3",
                                    Name = "ZipCode"
                                }
                            }
                        }
                    }
                }
            }
        }
        };

        // Save match configuration
        var savedMatchDefId = await _matchDefinitionService.SaveMappedRowConfigurationAsync(mappedRowDto);
        Assert.NotEqual(Guid.Empty, savedMatchDefId);

        OrchestrationOptions options = OrchestrationOptions.Default();
        options.RequireTransitiveGroups = false;
        // ===== STEP 4: Execute Matching Command =====

        var matchingCommand = new MatchingCommand(
            _recordLinkageOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRespository,
            _genericRepository,
            _mlogger,
            options
        );

        var matchStepId = Guid.NewGuid();
        var matchRunId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = matchRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);


        var matchStep = new StepJob
        {
            Id = matchStepId,
            Type = StepType.Match,
            DataSourceId = savedMatchDefId, // Using the saved match definition ID
            RunId = matchRunId,
            Configuration = new Dictionary<string, object>
        {
            { "ProjectId", projectId }
        }
        };

        await _stepRespository.InsertAsync(matchStep, Constants.Collections.StepJobs);

        var matchContext = new CommandContext(matchRunId, projectId, matchStepId, _projectRunRepository, _stepRespository);

        // Act - Execute matching
        await matchingCommand.ExecuteAsync(matchContext, matchStep);

        // ===== STEP 5: Assertions =====

        // Assert match command executed successfully
        //Assert.NotNull(matchResult);

        // Verify match results exist
        var matchStepData = await _stepRespository.GetByIdAsync(matchStepId, Constants.Collections.StepJobs);
        Assert.NotNull(matchStepData);
        Assert.NotNull(matchStepData.StepData);

        // Parse output collections
        var outputCollections = matchStepData.StepData.First().CollectionName.Split('|').Select(s => s.Trim()).ToArray();
        Assert.Equal(3, outputCollections.Length); // Should have pairs, groups, and matches collections

        // Verify Pairs
        var pairsCollectionName = outputCollections[0];
        var matchedPairs = await _dataStore.GetPagedDataAsync(pairsCollectionName, 1, 100);
        Assert.Equal(18, matchedPairs.TotalCount); // 18 pairs

        // Analyze match distribution
        var scoreDistribution = new Dictionary<string, int>
        {
            ["High (>=0.7)"] = 0,
            ["Medium (0.5-0.7)"] = 0,
            ["Low (<0.5)"] = 0
        };

        int pairNum = 1;
        foreach (var pair in matchedPairs.Data)
        {
            var score = Convert.ToDouble(pair["MaxScore"]);
            if (score >= 0.7) scoreDistribution["High (>=0.7)"]++;
            else if (score >= 0.5) scoreDistribution["Medium (0.5-0.7)"]++;
            else scoreDistribution["Low (<0.5)"]++;

            Console.WriteLine($"Pair {pairNum}:");

            // Data Source Information
            Console.WriteLine($"  DataSources: DS1={pair["DataSource1Id"]} (Row {pair["Row1Number"]}) <-> DS2={pair["DataSource2Id"]} (Row {pair["Row2Number"]})");

            // Record 1
            var record1 = pair["Record1"] as IDictionary<string, object>;
            if (record1 != null)
            {
                var r1Fields = string.Join(" | ", record1.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                Console.WriteLine($"  R1: {r1Fields}");
            }

            // Record 2
            var record2 = pair["Record2"] as IDictionary<string, object>;
            if (record2 != null)
            {
                var r2Fields = string.Join(" | ", record2.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                Console.WriteLine($"  R2: {r2Fields}");
            }

            Console.WriteLine($"  Score: {pair["MaxScore"]}");

            //// Field Scores if available
            //if (pair.ContainsKey("FieldScores") && pair["FieldScores"] is IList<object> fieldScores)
            //{
            //    var scores = fieldScores.Cast<IDictionary<string, object>>()
            //        .Select(fs => $"{fs["FieldName"]}={fs["Score"]}")
            //        .ToList();
            //    if (scores.Any())
            //    {
            //        Console.WriteLine($"  Field Scores: {string.Join(", ", scores)}");
            //    }
            //}

            // Scores by Definition if available
            if (pair.ContainsKey("ScoresByDefinition") && pair["ScoresByDefinition"] is IDictionary<string, object> scoresByDef)
            {
                // Handle if it's a Dictionary<string, object>
                if (scoresByDef is IDictionary<string, object> defDict)
                {
                    Console.WriteLine($"  Definition Scores:");
                    foreach (var defKvp in defDict)
                    {
                        Console.WriteLine($"    Definition {defKvp.Key}");

                        // If the value itself contains field scores
                        if (defKvp.Value is IDictionary<string, object> fieldScoresDict)
                        {
                            if (fieldScoresDict.ContainsKey("FieldScores"))
                            {
                                var fieldScores = fieldScoresDict["FieldScores"];
                                if (fieldScores is IDictionary<string, object> fieldList)
                                {
                                    foreach (var field in fieldList)
                                    {

                                        Console.WriteLine($"      Field: {field.Key}, Score: {field.Value}");

                                    }
                                }
                            }
                        }
                    }
                }
            }

            Console.WriteLine();
            pairNum++;
        }

        // Should have a mix of confidence levels
        //Assert.True(scoreDistribution["High (>=0.7)"] > 0, "Should have high confidence matches");


        // Verify Groups
        var groupsCollectionName = outputCollections[1];
        var matchGroups = await _dataStore.GetPagedDataAsync(groupsCollectionName, 1, 100);
        Assert.Equal(12, matchGroups.TotalCount); // 12 groups

        // All groups should have exactly 2 records (isolated pairs)
        //foreach (var group in matchGroups.Data)
        //{
        //    var records = group["Records"] as IList<object>;
        //    Assert.Equal(2, records.Count); // Each group is an isolated pair
        //}

        // Verify Graph
        var graphCollectionName = outputCollections[2];
        var graphData = await _dataStore.GetPagedDataAsync(graphCollectionName, 1, 10);
        var graph = graphData.Data.First();
        Assert.Equal(27, Convert.ToInt32(graph["TotalNodes"]));
        Assert.Equal(18, Convert.ToInt32(graph["TotalEdges"]));

    }

    [Fact]
    public async Task ExecCommandAsync()
    {
        // Arrange - Setup file paths and services
        var filePath1 = Path.GetFullPath("TestData\\Companies2MExcel.xlsx");
        //var filePath2 = GetTempFilePath("testMatch2.xlsx");
        //var filePath3 = GetTempFilePath("testMatch3.xlsx");



        // Initialize commands
        DataImportCommand dataImportCommand = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _genericRepository,
            _recordHasher,
            _Dlogger,
            _dataStore,
            _projectRunRepository,
            _stepRespository,
            _connectionBuilder,
            _columnFilter,
            _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory()
        );

        // Setup IDs
        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();


        // Create data source IDs for 3 sources
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
        };

        // Create project
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project with Matching",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        await _matchSetting.InsertAsync(new MatchSettings() { MergeOverlappingGroups = true, ProjectId = projectId }, Constants.Collections.MatchSettings);
        // Create project run
        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        // ===== STEP 1: Create and Import Data Sources =====

        // Create test Excel files with similar structure but slightly different data
        //CreateComprehensiveTestExcelFile(filePath1);
        //CreateComprehensiveTestExcelFileVariation2(filePath2);
        //CreateComprehensiveTestExcelFileVariation3(filePath3);

        // Create and insert data sources
        var dataSource1 = CreateComprehensiveExcel(dataSources["DS1"], filePath1);
        dataSource1.Name = "DS1";
        dataSource1.ProjectId = projectId;
        //var encryptedParameters = await _secureParameterHandler.EncryptSensitiveParametersAsync(dataSource1.ConnectionDetails.Parameters, dataSources["DS1"]);
        //dataSource1.ConnectionDetails.Parameters = encryptedParameters;
        await _genericRepository.InsertAsync(dataSource1, Constants.Collections.DataSources);

        var ds1 = await _genericRepository.QueryAsync(x => x.ProjectId == projectId, Constants.Collections.DataSources);

        //var dataSource2 = CreateComprehensiveExcelDataSource(dataSources["DS2"], filePath2);
        //dataSource2.Name = "DS2";
        //dataSource2.ProjectId = projectId;
        //await _genericRepository.InsertAsync(dataSource2, Constants.Collections.DataSources);

        //var dataSource3 = CreateComprehensiveExcelDataSource(dataSources["DS3"], filePath3);
        //dataSource3.Name = "DS3";
        //dataSource3.ProjectId = projectId;
        //await _genericRepository.InsertAsync(dataSource3, Constants.Collections.DataSources);

        // Import each data source
        foreach (var ds in dataSources)
        {
            var importStepId = Guid.NewGuid();
            var importStep = new StepJob
            {
                Id = importStepId,
                Type = StepType.Import,
                RunId = runId,
                DataSourceId = ds.Value,
                Configuration = new Dictionary<string, object>
            {
                { "DataSourceId", ds.Value }
            }
            };
            await _stepRespository.InsertAsync(importStep, Constants.Collections.StepJobs);

            var importContext = new CommandContext(runId, projectId, importStepId, _projectRunRepository, _stepRespository);
            await dataImportCommand.ExecuteAsync(importContext, importStep);
        }

        // ===== STEP 2: Setup Match Definitions =====

        // Create data source pairs collection
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add all pairs
        pairsCollection.Add(dataSources["DS1"], dataSources["DS1"]);
        //pairsCollection.Add(dataSources["DS1"], dataSources["DS3"]);
        //pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Create match definition with mapped rows
        var matchJobId = Guid.NewGuid();
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = matchJobId,
            Name = "Comprehensive Match Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
        {
            // First definition - Email and Name matching
            new MatchDefinitionMappedRowDto
            {
                ProjectRunId = runId,
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    // Exact match on Email (highest weight)
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 1, // High weight for email
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "City"
                                },

                            }
                        }
                    },
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 1, // High weight for email
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "State"
                                },

                            }
                        }
                    },
                    // Fuzzy match on FullName
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Fuzzy,
                        DataType = CriteriaDataType.Text,
                        Weight = 1,
                        Arguments = new Dictionary<ArgsValue, string> {[ArgsValue.FastLevel]="0.3", [ArgsValue.Level] = "0.95" }, // 80% similarity
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "CompanyName"
                                },
                            }
                        }
                    },
                    //new MatchCriterionMappedRowDto
                    //{
                    //    MatchingType = MatchingType.Fuzzy,
                    //    DataType = CriteriaDataType.Text,
                    //    Weight = 1,
                    //    Arguments = new Dictionary<ArgsValue, string> {[ArgsValue.FastLevel]="0.8", [ArgsValue.Level] = "0.9" }, // 80% similarity
                    //    MappedRow = new MappedFieldRowDto
                    //    {
                    //        FieldsByDataSource = new Dictionary<string, FieldDto>
                    //        {
                    //            ["DS1"] = new FieldDto {
                    //                DataSourceId = dataSources["DS1"],
                    //                DataSourceName = "DS1",
                    //                Name = "Contact Name"
                    //            },
                    //        }
                    //    }
                    //}
                }
            },

        }
        };

        // Save match configuration
        var savedMatchDefId = await _matchDefinitionService.SaveMappedRowConfigurationAsync(mappedRowDto);
        Assert.NotEqual(Guid.Empty, savedMatchDefId);

        OrchestrationOptions options = OrchestrationOptions.Default();
        options.RequireTransitiveGroups = false;
        // ===== STEP 4: Execute Matching Command =====

        var matchingCommand = new MatchingCommand(
            _recordLinkageOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRespository,
            _genericRepository,
            _mlogger,
            options
        );

        var matchStepId = Guid.NewGuid();
        var matchRunId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = matchRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);


        var matchStep = new StepJob
        {
            Id = matchStepId,
            Type = StepType.Match,
            DataSourceId = savedMatchDefId, // Using the saved match definition ID
            RunId = matchRunId,
            Configuration = new Dictionary<string, object>
        {
            { "ProjectId", projectId }
        }
        };

        await _stepRespository.InsertAsync(matchStep, Constants.Collections.StepJobs);

        var matchContext = new CommandContext(matchRunId, projectId, matchStepId, _projectRunRepository, _stepRespository);
        Console.WriteLine("Start Matchjob {0}", DateTime.Now);
        // Act - Execute matching
        await matchingCommand.ExecuteAsync(matchContext, matchStep);

        // ===== STEP 5: Assertions =====

        // Assert match command executed successfully
        //Assert.NotNull(matchResult);
        Console.WriteLine("End MatchJob {0}", DateTime.Now);
        // Verify match results exist
        var matchStepData = await _stepRespository.GetByIdAsync(matchStepId, Constants.Collections.StepJobs);
        Assert.NotNull(matchStepData);
        Assert.NotNull(matchStepData.StepData);

        // Parse output collections
        var outputCollections = matchStepData.StepData.First().CollectionName.Split('|').Select(s => s.Trim()).ToArray();
        Assert.Equal(3, outputCollections.Length); // Should have pairs, groups, and matches collections

        // Verify Pairs
        var pairsCollectionName = outputCollections[0];
        var matchedPairs = await _dataStore.GetPagedDataAsync(pairsCollectionName, 1, 100);
        Assert.Equal(18, matchedPairs.TotalCount); // 18 pairs

        // Analyze match distribution
        var scoreDistribution = new Dictionary<string, int>
        {
            ["High (>=0.7)"] = 0,
            ["Medium (0.5-0.7)"] = 0,
            ["Low (<0.5)"] = 0
        };

        int pairNum = 1;
        foreach (var pair in matchedPairs.Data)
        {
            var score = Convert.ToDouble(pair["MaxScore"]);
            if (score >= 0.7) scoreDistribution["High (>=0.7)"]++;
            else if (score >= 0.5) scoreDistribution["Medium (0.5-0.7)"]++;
            else scoreDistribution["Low (<0.5)"]++;

            Console.WriteLine($"Pair {pairNum}:");

            // Data Source Information
            Console.WriteLine($"  DataSources: DS1={pair["DataSource1Id"]} (Row {pair["Row1Number"]}) <-> DS2={pair["DataSource2Id"]} (Row {pair["Row2Number"]})");

            // Record 1
            var record1 = pair["Record1"] as IDictionary<string, object>;
            if (record1 != null)
            {
                var r1Fields = string.Join(" | ", record1.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                Console.WriteLine($"  R1: {r1Fields}");
            }

            // Record 2
            var record2 = pair["Record2"] as IDictionary<string, object>;
            if (record2 != null)
            {
                var r2Fields = string.Join(" | ", record2.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                Console.WriteLine($"  R2: {r2Fields}");
            }

            Console.WriteLine($"  Score: {pair["MaxScore"]}");

            //// Field Scores if available
            //if (pair.ContainsKey("FieldScores") && pair["FieldScores"] is IList<object> fieldScores)
            //{
            //    var scores = fieldScores.Cast<IDictionary<string, object>>()
            //        .Select(fs => $"{fs["FieldName"]}={fs["Score"]}")
            //        .ToList();
            //    if (scores.Any())
            //    {
            //        Console.WriteLine($"  Field Scores: {string.Join(", ", scores)}");
            //    }
            //}

            // Scores by Definition if available
            if (pair.ContainsKey("ScoresByDefinition") && pair["ScoresByDefinition"] is IDictionary<string, object> scoresByDef)
            {
                // Handle if it's a Dictionary<string, object>
                if (scoresByDef is IDictionary<string, object> defDict)
                {
                    Console.WriteLine($"  Definition Scores:");
                    foreach (var defKvp in defDict)
                    {
                        Console.WriteLine($"    Definition {defKvp.Key}");

                        // If the value itself contains field scores
                        if (defKvp.Value is IDictionary<string, object> fieldScoresDict)
                        {
                            if (fieldScoresDict.ContainsKey("FieldScores"))
                            {
                                var fieldScores = fieldScoresDict["FieldScores"];
                                if (fieldScores is IDictionary<string, object> fieldList)
                                {
                                    foreach (var field in fieldList)
                                    {

                                        Console.WriteLine($"      Field: {field.Key}, Score: {field.Value}");

                                    }
                                }
                            }
                        }
                    }
                }
            }

            Console.WriteLine();
            pairNum++;
        }

        // Should have a mix of confidence levels
        //Assert.True(scoreDistribution["High (>=0.7)"] > 0, "Should have high confidence matches");


        // Verify Groups
        var groupsCollectionName = outputCollections[1];
        var matchGroups = await _dataStore.GetPagedDataAsync(groupsCollectionName, 1, 100);
        Assert.Equal(12, matchGroups.TotalCount); // 12 groups

        // All groups should have exactly 2 records (isolated pairs)
        //foreach (var group in matchGroups.Data)
        //{
        //    var records = group["Records"] as IList<object>;
        //    Assert.Equal(2, records.Count); // Each group is an isolated pair
        //}

        // Verify Graph
        var graphCollectionName = outputCollections[2];
        var graphData = await _dataStore.GetPagedDataAsync(graphCollectionName, 1, 10);
        var graph = graphData.Data.First();
        Assert.Equal(27, Convert.ToInt32(graph["TotalNodes"]));
        Assert.Equal(18, Convert.ToInt32(graph["TotalEdges"]));

    }



    [Fact]
    public async Task ExecCommandWithGraphSavingAndAnalysisAsync()
    {
        // Arrange - Setup file paths and services
        var filePath1 = Path.GetFullPath("TestData\\Companies2MExcel.xlsx");
        //var filePath2 = GetTempFilePath("testMatch2.xlsx");
        //var filePath3 = GetTempFilePath("testMatch3.xlsx");



        // Initialize commands
        DataImportCommand dataImportCommand = new DataImportCommand(
            _dataSourceService,
            _projectService,
            _jobEventPublisher,
            _genericRepository,
            _recordHasher,
            _Dlogger,
            _dataStore,
            _projectRunRepository,
            _stepRespository,
            _connectionBuilder,
            _columnFilter,
            _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory()
        );

        // Setup IDs
        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();


        // Create data source IDs for 3 sources
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
        };

        // Create project
        var project = new Project
        {
            Id = projectId,
            Name = "Test Project with Matching",
            Description = "Test Description"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        await _matchSetting.InsertAsync(new MatchSettings() { MergeOverlappingGroups = true, ProjectId = projectId }, Constants.Collections.MatchSettings);
        // Create project run
        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        // ===== STEP 1: Create and Import Data Sources =====

        // Create test Excel files with similar structure but slightly different data
        //CreateComprehensiveTestExcelFile(filePath1);
        //CreateComprehensiveTestExcelFileVariation2(filePath2);
        //CreateComprehensiveTestExcelFileVariation3(filePath3);

        // Create and insert data sources
        var dataSource1 = CreateComprehensiveExcel(dataSources["DS1"], filePath1);
        dataSource1.Name = "DS1";
        dataSource1.ProjectId = projectId;
        //var encryptedParameters = await _secureParameterHandler.EncryptSensitiveParametersAsync(dataSource1.ConnectionDetails.Parameters, dataSources["DS1"]);
        //dataSource1.ConnectionDetails.Parameters = encryptedParameters;
        await _genericRepository.InsertAsync(dataSource1, Constants.Collections.DataSources);

        var ds1 = await _genericRepository.QueryAsync(x => x.ProjectId == projectId, Constants.Collections.DataSources);

        //var dataSource2 = CreateComprehensiveExcelDataSource(dataSources["DS2"], filePath2);
        //dataSource2.Name = "DS2";
        //dataSource2.ProjectId = projectId;
        //await _genericRepository.InsertAsync(dataSource2, Constants.Collections.DataSources);

        //var dataSource3 = CreateComprehensiveExcelDataSource(dataSources["DS3"], filePath3);
        //dataSource3.Name = "DS3";
        //dataSource3.ProjectId = projectId;
        //await _genericRepository.InsertAsync(dataSource3, Constants.Collections.DataSources);

        // Import each data source
        foreach (var ds in dataSources)
        {
            var importStepId = Guid.NewGuid();
            var importStep = new StepJob
            {
                Id = importStepId,
                Type = StepType.Import,
                RunId = runId,
                DataSourceId = ds.Value,
                Configuration = new Dictionary<string, object>
            {
                { "DataSourceId", ds.Value }
            }
            };
            await _stepRespository.InsertAsync(importStep, Constants.Collections.StepJobs);

            var importContext = new CommandContext(runId, projectId, importStepId, _projectRunRepository, _stepRespository);
            await dataImportCommand.ExecuteAsync(importContext, importStep);
        }

        // ===== STEP 2: Setup Match Definitions =====

        // Create data source pairs collection
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add all pairs
        pairsCollection.Add(dataSources["DS1"], dataSources["DS1"]);
        //pairsCollection.Add(dataSources["DS1"], dataSources["DS3"]);
        //pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Create match definition with mapped rows
        var matchJobId = Guid.NewGuid();
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = matchJobId,
            Name = "Comprehensive Match Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
        {
            // First definition - Email and Name matching
            new MatchDefinitionMappedRowDto
            {
                ProjectRunId = runId,
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    // Exact match on Email (highest weight)
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 1, // High weight for email
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "City"
                                },

                            }
                        }
                    },
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 1, // High weight for email
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "State"
                                },

                            }
                        }
                    },
                    // Fuzzy match on FullName
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Fuzzy,
                        DataType = CriteriaDataType.Text,
                        Weight = 1,
                        Arguments = new Dictionary<ArgsValue, string> {[ArgsValue.FastLevel]="0.3", [ArgsValue.Level] = "0.95" }, // 80% similarity
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "CompanyName"
                                },
                            }
                        }
                    },
                    //new MatchCriterionMappedRowDto
                    //{
                    //    MatchingType = MatchingType.Fuzzy,
                    //    DataType = CriteriaDataType.Text,
                    //    Weight = 1,
                    //    Arguments = new Dictionary<ArgsValue, string> {[ArgsValue.FastLevel]="0.8", [ArgsValue.Level] = "0.9" }, // 80% similarity
                    //    MappedRow = new MappedFieldRowDto
                    //    {
                    //        FieldsByDataSource = new Dictionary<string, FieldDto>
                    //        {
                    //            ["DS1"] = new FieldDto {
                    //                DataSourceId = dataSources["DS1"],
                    //                DataSourceName = "DS1",
                    //                Name = "Contact Name"
                    //            },
                    //        }
                    //    }
                    //}
                }
            },

        }
        };

        // Save match configuration
        var savedMatchDefId = await _matchDefinitionService.SaveMappedRowConfigurationAsync(mappedRowDto);
        Assert.NotEqual(Guid.Empty, savedMatchDefId);

        OrchestrationOptions options = OrchestrationOptions.Default();
        options.RequireTransitiveGroups = false;
        options.EnableMatchQualityAnalysis = true;
        options.SaveMatchGraph = true;
        // ===== STEP 4: Execute Matching Command =====

        var matchingCommand = new MatchingCommand(
            _recordLinkageOrchestrator,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRespository,
            _genericRepository,
            _mlogger,
            options
        );

        var matchStepId = Guid.NewGuid();
        var matchRunId = Guid.NewGuid();

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = matchRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);


        var matchStep = new StepJob
        {
            Id = matchStepId,
            Type = StepType.Match,
            DataSourceId = savedMatchDefId, // Using the saved match definition ID
            RunId = matchRunId,
            Configuration = new Dictionary<string, object>
        {
            { "ProjectId", projectId }
        }
        };

        await _stepRespository.InsertAsync(matchStep, Constants.Collections.StepJobs);

        var matchContext = new CommandContext(matchRunId, projectId, matchStepId, _projectRunRepository, _stepRespository);
        Console.WriteLine("Start Matchjob {0}", DateTime.Now);
        // Act - Execute matching
        await matchingCommand.ExecuteAsync(matchContext, matchStep);

        // ===== STEP 5: Assertions =====

        // Assert match command executed successfully
        //Assert.NotNull(matchResult);
        Console.WriteLine("End MatchJob {0}", DateTime.Now);
        // Verify match results exist
        var matchStepData = await _stepRespository.GetByIdAsync(matchStepId, Constants.Collections.StepJobs);
        Assert.NotNull(matchStepData);
        Assert.NotNull(matchStepData.StepData);

        // Parse output collections
        var outputCollections = matchStepData.StepData.First().CollectionName.Split('|').Select(s => s.Trim()).ToArray();
        Assert.Equal(3, outputCollections.Length); // Should have pairs, groups, and matches collections

        // Verify Pairs
        var pairsCollectionName = outputCollections[0];
        var matchedPairs = await _dataStore.GetPagedDataAsync(pairsCollectionName, 1, 100);
        Assert.Equal(88966, matchedPairs.TotalCount);

        // Analyze match distribution
        var scoreDistribution = new Dictionary<string, int>
        {
            ["High (>=0.7)"] = 0,
            ["Medium (0.5-0.7)"] = 0,
            ["Low (<0.5)"] = 0
        };

        int pairNum = 1;
        foreach (var pair in matchedPairs.Data)
        {
            var score = Convert.ToDouble(pair["MaxScore"]);
            if (score >= 0.7) scoreDistribution["High (>=0.7)"]++;
            else if (score >= 0.5) scoreDistribution["Medium (0.5-0.7)"]++;
            else scoreDistribution["Low (<0.5)"]++;

            Console.WriteLine($"Pair {pairNum}:");

            // Data Source Information
            Console.WriteLine($"  DataSources: DS1={pair["DataSource1Id"]} (Row {pair["Row1Number"]}) <-> DS2={pair["DataSource2Id"]} (Row {pair["Row2Number"]})");

            // Record 1
            var record1 = pair["Record1"] as IDictionary<string, object>;
            if (record1 != null)
            {
                var r1Fields = string.Join(" | ", record1.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                Console.WriteLine($"  R1: {r1Fields}");
            }

            // Record 2
            var record2 = pair["Record2"] as IDictionary<string, object>;
            if (record2 != null)
            {
                var r2Fields = string.Join(" | ", record2.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                Console.WriteLine($"  R2: {r2Fields}");
            }

            Console.WriteLine($"  Score: {pair["MaxScore"]}");

            //// Field Scores if available
            //if (pair.ContainsKey("FieldScores") && pair["FieldScores"] is IList<object> fieldScores)
            //{
            //    var scores = fieldScores.Cast<IDictionary<string, object>>()
            //        .Select(fs => $"{fs["FieldName"]}={fs["Score"]}")
            //        .ToList();
            //    if (scores.Any())
            //    {
            //        Console.WriteLine($"  Field Scores: {string.Join(", ", scores)}");
            //    }
            //}

            // Scores by Definition if available
            if (pair.ContainsKey("ScoresByDefinition") && pair["ScoresByDefinition"] is IDictionary<string, object> scoresByDef)
            {
                // Handle if it's a Dictionary<string, object>
                if (scoresByDef is IDictionary<string, object> defDict)
                {
                    Console.WriteLine($"  Definition Scores:");
                    foreach (var defKvp in defDict)
                    {
                        Console.WriteLine($"    Definition {defKvp.Key}");

                        // If the value itself contains field scores
                        if (defKvp.Value is IDictionary<string, object> fieldScoresDict)
                        {
                            if (fieldScoresDict.ContainsKey("FieldScores"))
                            {
                                var fieldScores = fieldScoresDict["FieldScores"];
                                if (fieldScores is IDictionary<string, object> fieldList)
                                {
                                    foreach (var field in fieldList)
                                    {

                                        Console.WriteLine($"      Field: {field.Key}, Score: {field.Value}");

                                    }
                                }
                            }
                        }
                    }
                }
            }

            Console.WriteLine();
            pairNum++;
        }

        // Should have a mix of confidence levels
        //Assert.True(scoreDistribution["High (>=0.7)"] > 0, "Should have high confidence matches");


        // Verify Groups
        var groupsCollectionName = outputCollections[1];
        var matchGroups = await _dataStore.GetPagedDataAsync(groupsCollectionName, 1, 100);
        Assert.Equal(52147, matchGroups.TotalCount);

        // All groups should have exactly 2 records (isolated pairs)
        //foreach (var group in matchGroups.Data)
        //{
        //    var records = group["Records"] as IList<object>;
        //    Assert.Equal(2, records.Count); // Each group is an isolated pair
        //}

        // Verify Graph
        var graphCollectionName = outputCollections[2];
        var graphData = await _dataStore.GetPagedDataAsync(graphCollectionName, 1, 10);
        var graph = graphData.Data.First();
        Assert.Equal(113935, Convert.ToInt32(graph["TotalNodes"]));
        Assert.Equal(89295, Convert.ToInt32(graph["TotalEdges"]));

        var analysisCollectionname = graphCollectionName.Replace("matchgraph_", "analytics_");
        var analysisData = await _dataStore.GetPagedDataAsync(analysisCollectionname, 1, 10);
        var analysis = analysisData.Data.First();

    }
    [Fact]
    public async Task ExecCommandAsync_SimpleExactEmailMatching_CreatesOneGroupWithThreeRecords()
    {
        // This test verifies the transitive grouping with exact email matches
        // Expected: 3 pairs forming 1 group with 3 records

        // Arrange - Setup file paths
        var filePath1 = GetTempFilePath("testSimple1.xlsx");
        var filePath2 = GetTempFilePath("testSimple2.xlsx");
        var filePath3 = GetTempFilePath("testSimple3.xlsx");

        // Initialize command
        DataImportCommand dataImportCommand = new DataImportCommand(
            _dataSourceService, _projectService, _jobEventPublisher, _genericRepository,
            _recordHasher, _Dlogger,
            _dataStore, _projectRunRepository, _stepRespository, _connectionBuilder, _columnFilter, _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory()
        );

        // Setup IDs
        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();

        // Create data source IDs
        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid(),
            ["DS3"] = Guid.NewGuid()
        };

        // Create project
        var project = new Project
        {
            Id = projectId,
            Name = "Simple Email Match Test",
            Description = "Test exact email matching creating transitive groups"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        // Create project run
        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        await _matchSetting.InsertAsync(new MatchSettings() { ProjectId = projectId }, Constants.Collections.MatchSettings);

        // Create simple test files with shared email
        CreateSimpleTestFile_DS1(filePath1);
        CreateSimpleTestFile_DS2(filePath2);
        CreateSimpleTestFile_DS3(filePath3);

        // Create and insert data sources
        var dataSource1 = CreateComprehensiveExcelDataSource(dataSources["DS1"], filePath1);
        dataSource1.Name = "DS1";
        dataSource1.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource1, Constants.Collections.DataSources);

        var dataSource2 = CreateComprehensiveExcelDataSource(dataSources["DS2"], filePath2);
        dataSource2.Name = "DS2";
        dataSource2.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource2, Constants.Collections.DataSources);

        var dataSource3 = CreateComprehensiveExcelDataSource(dataSources["DS3"], filePath3);
        dataSource3.Name = "DS3";
        dataSource3.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource3, Constants.Collections.DataSources);

        // Import each data source
        foreach (var ds in dataSources)
        {
            var importStepId = Guid.NewGuid();
            var importStep = new StepJob
            {
                Id = importStepId,
                Type = StepType.Import,
                RunId = runId,
                DataSourceId = ds.Value,
                Configuration = new Dictionary<string, object> { { "DataSourceId", ds.Value } }
            };
            await _stepRespository.InsertAsync(importStep, Constants.Collections.StepJobs);
            var importContext = new CommandContext(runId, projectId, importStepId, _projectRunRepository, _stepRespository);
            await dataImportCommand.ExecuteAsync(importContext, importStep);
        }

        // Setup Match Definitions - Only exact email matching
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };

        // Add all three pairs for triangular matching
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        pairsCollection.Add(dataSources["DS1"], dataSources["DS3"]);
        pairsCollection.Add(dataSources["DS2"], dataSources["DS3"]);

        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Create simple match definition - ONLY email exact matching
        var matchJobId = Guid.NewGuid();
        var matchRunId = Guid.NewGuid();
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = matchJobId,
            Name = "Simple Email Match",
            Definitions = new List<MatchDefinitionMappedRowDto>
        {
            new MatchDefinitionMappedRowDto
            {
                ProjectRunId = matchRunId,
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 1.0, // Full weight on email
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto { DataSourceId = dataSources["DS1"], DataSourceName = "DS1", Name = "Email" },
                                ["DS2"] = new FieldDto { DataSourceId = dataSources["DS2"], DataSourceName = "DS2", Name = "Email" },
                                ["DS3"] = new FieldDto { DataSourceId = dataSources["DS3"], DataSourceName = "DS3", Name = "Email" }
                            }
                        }
                    }
                }
            }
        }
        };

        var savedMatchDefId = await _matchDefinitionService.SaveMappedRowConfigurationAsync(mappedRowDto);

        // Execute Matching Command
        var matchingCommand = new MatchingCommand(
            _recordLinkageOrchestrator, _projectService, _jobEventPublisher,
            _projectRunRepository, _stepRespository, _genericRepository, _mlogger
        );

        var matchStepId = Guid.NewGuid();
        var matchStep = new StepJob
        {
            Id = matchStepId,
            Type = StepType.Match,
            DataSourceId = savedMatchDefId,
            RunId = matchRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = matchRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(matchStep, Constants.Collections.StepJobs);

        var matchContext = new CommandContext(matchRunId, projectId, matchStepId, _projectRunRepository, _stepRespository);

        // Act
        await matchingCommand.ExecuteAsync(matchContext, matchStep);

        // Assert - Specific assertions for 3 pairs, 1 group scenario
        var matchStepData = await _stepRespository.GetByIdAsync(matchStepId, Constants.Collections.StepJobs);
        Assert.NotNull(matchStepData);
        Assert.NotNull(matchStepData.StepData);

        var outputCollections = matchStepData.StepData.First().CollectionName.Split('|').Select(s => s.Trim()).ToArray();

        // Verify Pairs
        var pairsCollectionName = outputCollections[0];
        var matchedPairs = await _dataStore.GetPagedDataAsync(pairsCollectionName, 1, 100);
        Assert.Equal(3, matchedPairs.TotalCount); // Exactly 3 pairs

        // Verify all pairs have john.smith@example.com
        foreach (var pair in matchedPairs.Data)
        {
            var record1 = pair["Record1"] as IDictionary<string, object>;
            var record2 = pair["Record2"] as IDictionary<string, object>;
            Assert.Equal("john.smith@example.com", record1["Email"]?.ToString());
            Assert.Equal("john.smith@example.com", record2["Email"]?.ToString());

            // Score should be 1.0 for exact matches
            Assert.Equal(1.0, Convert.ToDouble(pair["MaxScore"]));
        }

        // Verify Groups
        var groupsCollectionName = outputCollections[1];
        var matchGroups = await _dataStore.GetPagedDataAsync(groupsCollectionName, 1, 100);
        Assert.Equal(1, matchGroups.TotalCount); // Exactly 1 group

        var group = matchGroups.Data.First();
        var records = group["Records"] as IList<object>;
        Assert.Equal(3, records.Count); // Group contains exactly 3 records

        // Verify Graph
        var graphCollectionName = outputCollections[2];
        var graphData = await _dataStore.GetPagedDataAsync(graphCollectionName, 1, 10);
        var graph = graphData.Data.First();
        Assert.Equal(3, Convert.ToInt32(graph["TotalNodes"])); // 3 nodes (one per data source)
        Assert.Equal(3, Convert.ToInt32(graph["TotalEdges"])); // 3 edges forming a triangle

    }
    // Helper method to create variation 2 of test data

    private DataSource CreateComprehensiveExcel(Guid dataSourceId, string filePath)
    {
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
        {
            { "FilePath", filePath },
            { "HasHeaders", "true" }
        }
        };
        connectionInfo.Parameters = Task.Run(async () => await _secureParameterHandler.EncryptSensitiveParametersAsync(connectionInfo.Parameters, dataSourceId)).Result;
        var columnMappings = new Dictionary<string, ColumnMapping>();

        // Add mapping for each column
        string[] columns = new[] {
        "FullName", "Email", "PhoneNumber", "Address", "City", "State", "ZipCode",
        "Country", "JobTitle", "Department", "Salary", "UserId", "Username", "Notes"
    };

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
                //ColumnMappings = columnMappings
            }
        };
    }
    private DataSource CreateComprehensiveExcelDataSource(Guid dataSourceId, string filePath)
    {
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
        {
            { "FilePath", filePath },
            { "HasHeaders", "true" }
        }
        };
        connectionInfo.Parameters = Task.Run(async () => await _secureParameterHandler.EncryptSensitiveParametersAsync(connectionInfo.Parameters, dataSourceId)).Result;
        var columnMappings = new Dictionary<string, ColumnMapping>();

        // Add mapping for each column
        string[] columns = new[] {
        "FullName", "Email", "PhoneNumber", "Address", "City", "State", "ZipCode",
        "Country", "JobTitle", "Department", "Salary", "UserId", "Username", "Notes"
    };

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
            }
        };
    }
    private void CreateComprehensiveTestExcelFile(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
        {
            headerRow.CreateCell(i).SetCellValue(headers[i]);
        }

        // DS1 Data - Original records
        AddTestDataRow(sheet, 1, "John Smith", "john.smith@example.com", "555-123-4567", "123 Main St", "New York", "NY", "10001", "United States", "Senior Analyst", "Accounting", "$75,000", "user123", "john_smith", "Original record");
        AddTestDataRow(sheet, 2, "Mary Johnson", "mary.johnson@example.com", "555-234-5678", "456 Oak Ave", "Los Angeles", "CA", "90210", "United States", "Marketing Coordinator", "Marketing", "$65,000", "user456", "mary.johnson", "Original record");
        AddTestDataRow(sheet, 3, "Robert Williams", "robert.williams@example.com", "555-345-6789", "789 Pine Rd", "Chicago", "IL", "60601", "United States", "Software Engineer", "Engineering", "$95,000", "user789", "robert_williams", "Original record");
        AddTestDataRow(sheet, 4, "Jennifer Brown", "jennifer.brown@example.com", "555-456-7890", "321 Elm St", "Boston", "MA", "02108", "United States", "Product Manager", "Product", "$85,000", "user101", "jennifer_brown", "Original record");
        AddTestDataRow(sheet, 5, "Michael Davis", "michael.davis@example.com", "555-567-8901", "654 Maple Ave", "San Francisco", "CA", "94105", "United States", "Sales Representative", "Sales", "$70,000", "user202", "michael_davis", "Original record");
        AddTestDataRow(sheet, 6, "Sarah Miller", "sarah.miller@example.com", "555-678-9012", "987 Cedar Blvd", "Seattle", "WA", "98101", "United States", "HR Manager", "HR", "$80,000", "user303", "sarah_miller", "Original record");
        AddTestDataRow(sheet, 7, "David Wilson", "david.wilson@example.com", "555-789-0123", "159 Birch St", "Portland", "OR", "97201", "United States", "Customer Service Rep", "Customer Service", "$60,000", "user404", "david_wilson", "Original record");
        AddTestDataRow(sheet, 8, "Jessica Garcia", "jessica.garcia@example.com", "555-890-1234", "753 Walnut Dr", "Denver", "CO", "80202", "United States", "Research Scientist", "Research", "$90,000", "user505", "jessica_garcia", "Original record");
        AddTestDataRow(sheet, 9, "James Rodriguez", "james.rodriguez@example.com", "555-901-2345", "852 Spruce Way", "Austin", "TX", "78701", "United States", "Operations Director", "Operations", "$100,000", "user606", "james_rodriguez", "Original record");
        AddTestDataRow(sheet, 10, "Patricia Martinez", "patricia.martinez@example.com", "555-012-3456", "426 Pine Lane", "Miami", "FL", "33101", "United States", "Data Analyst", "Analytics", "$72,000", "user707", "patricia_martinez", "Original record");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    // Modified CreateComprehensiveTestExcelFileVariation2 - DS2
    private void CreateComprehensiveTestExcelFileVariation2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
        {
            headerRow.CreateCell(i).SetCellValue(headers[i]);
        }

        // DS2 Data - Mix of exact matches, fuzzy matches, and new records
        // Exact email matches with DS1
        AddTestDataRow(sheet, 1, "John W Smith", "john.smith@example.com", "555-123-4567", "123 Main Street", "New York", "NY", "10001", "United States", "Sr Analyst", "Accounting", "$75000", "usr123", "jsmith", "Exact email match");
        AddTestDataRow(sheet, 2, "Mary K Johnson", "mary.johnson@example.com", "555-234-5678", "456 Oak Avenue", "Los Angeles", "CA", "90210", "United States", "Marketing Coord", "Marketing", "$65000", "usr456", "mkjohnson", "Exact email match");
        AddTestDataRow(sheet, 3, "Bob Williams", "robert.williams@example.com", "555-345-6789", "789 Pine Road", "Chicago", "IL", "60601", "United States", "Software Eng", "Engineering", "$95000", "usr789", "bwilliams", "Exact email match");

        // Fuzzy name matches with different emails
        AddTestDataRow(sheet, 4, "Jennifer A Brown", "j.brown@example.com", "555-456-7891", "321 Elm Street", "Boston", "MA", "02108", "United States", "Product Mgr", "Product", "$85500", "usr101", "jbrown", "Similar name and address");
        AddTestDataRow(sheet, 5, "Mike Davis", "m.davis@example.com", "555-567-8902", "654 Maple Avenue", "San Francisco", "CA", "94105", "United States", "Sales Rep", "Sales", "$70000", "usr202", "mdavis", "Similar address");

        // New unique records for DS2
        AddTestDataRow(sheet, 6, "Emily Chen", "emily.chen@example.com", "555-111-2222", "111 First St", "San Jose", "CA", "95110", "United States", "Engineer", "Tech", "$85000", "usr808", "echen", "Unique to DS2");
        AddTestDataRow(sheet, 7, "Carlos Lopez", "carlos.lopez@example.com", "555-222-3333", "222 Second Ave", "Phoenix", "AZ", "85001", "United States", "Manager", "Operations", "$75000", "usr909", "clopez", "Unique to DS2");

        // More exact matches
        AddTestDataRow(sheet, 8, "Jessica M Garcia", "jessica.garcia@example.com", "555-890-1234", "753 Walnut Drive", "Denver", "CO", "80202", "United States", "Research Sci", "Research", "$90000", "usr505", "jgarcia", "Exact email match");
        AddTestDataRow(sheet, 9, "James Rodriguez Jr", "james.rodriguez@example.com", "555-901-2345", "852 Spruce Way", "Austin", "TX", "78701", "United States", "Ops Director", "Operations", "$100000", "usr606", "jrodriguez", "Exact email match");
        AddTestDataRow(sheet, 10, "Pat Martinez", "patricia.martinez@example.com", "555-012-3456", "426 Pine Lane", "Miami", "FL", "33101", "United States", "Data Analyst", "Analytics", "$72000", "usr707", "pmartinez", "Exact email match");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    // Modified CreateComprehensiveTestExcelFileVariation3 - DS3
    private void CreateComprehensiveTestExcelFileVariation3(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
        {
            headerRow.CreateCell(i).SetCellValue(headers[i]);
        }

        // DS3 Data - Another mix creating triangular matches
        // These will match with both DS1 and DS2
        AddTestDataRow(sheet, 1, "John Smith III", "john.smith@example.com", "555-123-4567", "123 Main St Apt 1", "New York", "NY", "10001", "United States", "Analyst", "Finance", "$75K", "u123", "johns", "Matches DS1 & DS2");
        AddTestDataRow(sheet, 2, "Mary Johnson-Smith", "mary.johnson@example.com", "555-234-5678", "456 Oak Ave Unit B", "Los Angeles", "CA", "90210", "United States", "Mktg Coordinator", "Marketing", "$65K", "u456", "maryj", "Matches DS1 & DS2");

        // These match only with DS1
        AddTestDataRow(sheet, 3, "Robert C Williams", "robert.williams@example.com", "555-345-6789", "789 Pine Rd Suite 100", "Chicago", "IL", "60601", "United States", "SW Engineer", "Eng", "$95K", "u789", "robw", "Matches DS1 & DS2");
        AddTestDataRow(sheet, 4, "Sarah Ann Miller", "sarah.miller@example.com", "555-678-9012", "987 Cedar Boulevard", "Seattle", "WA", "98101", "United States", "HR Mgr", "Human Resources", "$80K", "u303", "sarahm", "Matches DS1 only");
        AddTestDataRow(sheet, 5, "David M Wilson", "david.wilson@example.com", "555-789-0123", "159 Birch Street", "Portland", "OR", "97201", "United States", "Service Rep", "Support", "$60K", "u404", "dwilson", "Matches DS1 only");

        // These match only with DS2
        AddTestDataRow(sheet, 6, "Emily S Chen", "emily.chen@example.com", "555-111-2222", "111 First Street", "San Jose", "CA", "95110", "United States", "Sr Engineer", "Technology", "$85K", "u808", "emchen", "Matches DS2 only");
        AddTestDataRow(sheet, 7, "Carlos A Lopez", "carlos.lopez@example.com", "555-222-3333", "222 Second Avenue", "Phoenix", "AZ", "85001", "United States", "Sr Manager", "Ops", "$75K", "u909", "calopez", "Matches DS2 only");

        // Unique to DS3
        AddTestDataRow(sheet, 8, "Lisa Thompson", "lisa.thompson@example.com", "555-333-4444", "333 Third Blvd", "Dallas", "TX", "75201", "United States", "Architect", "Design", "$88K", "u010", "lthompson", "Unique to DS3");
        AddTestDataRow(sheet, 9, "Kevin Park", "kevin.park@example.com", "555-444-5555", "444 Fourth Ave", "Houston", "TX", "77001", "United States", "Developer", "Tech", "$92K", "u011", "kpark", "Unique to DS3");
        AddTestDataRow(sheet, 10, "Amanda White", "amanda.white@example.com", "555-555-6666", "555 Fifth St", "Atlanta", "GA", "30301", "United States", "Director", "Sales", "$105K", "u012", "awhite", "Unique to DS3");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    // Helper method shared by all file creation methods
    private void AddTestDataRow(ISheet sheet, int rowNum, params string[] values)
    {
        var row = sheet.CreateRow(rowNum);
        for (int i = 0; i < values.Length; i++)
        {
            row.CreateCell(i).SetCellValue(values[i]);
        }
    }

    // Helper methods for simple test
    private void CreateSimpleTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // One shared record and some unique ones
        AddTestDataRow(sheet, 1, "John Smith", "john.smith@example.com", "555-123-4567", "123 Main St");
        AddTestDataRow(sheet, 2, "Alice Brown", "alice.brown@example.com", "555-111-1111", "456 Oak Ave");
        AddTestDataRow(sheet, 3, "Bob Wilson", "bob.wilson@example.com", "555-222-2222", "789 Pine Rd");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateSimpleTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Shared John Smith, plus unique records
        AddTestDataRow(sheet, 1, "John W Smith", "john.smith@example.com", "555-123-4567", "123 Main Street");
        AddTestDataRow(sheet, 2, "Carol Davis", "carol.davis@example.com", "555-333-3333", "111 Elm St");
        AddTestDataRow(sheet, 3, "David Lee", "david.lee@example.com", "555-444-4444", "222 Maple Ave");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateSimpleTestFile_DS3(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Shared John Smith, plus unique records
        AddTestDataRow(sheet, 1, "John Smith III", "john.smith@example.com", "(555) 123-4567", "123 Main St Apt 1");
        AddTestDataRow(sheet, 2, "Emma Jones", "emma.jones@example.com", "555-555-5555", "333 Cedar Blvd");
        AddTestDataRow(sheet, 3, "Frank Miller", "frank.miller@example.com", "555-666-6666", "444 Birch Dr");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    [Fact]
    public async Task ExecCommandAsync_TwoDataSources_CreatesTransitiveGroups()
    {
        // This test creates transitive groups where records chain together
        // Example: R1-R2 match, R2-R3 match, so R1-R2-R3 form one group

        // Arrange
        var filePath1 = GetTempFilePath("testTransitive1.xlsx");
        var filePath2 = GetTempFilePath("testTransitive2.xlsx");

        DataImportCommand dataImportCommand = new DataImportCommand(
            _dataSourceService, _projectService, _jobEventPublisher, _genericRepository,
            _recordHasher, _Dlogger,
            _dataStore, _projectRunRepository, _stepRespository, _connectionBuilder, _columnFilter, _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory()
        );

        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();

        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid()
        };

        // Create project
        var project = new Project
        {
            Id = projectId,
            Name = "Two DS Transitive Test",
            Description = "Test transitive grouping with two data sources"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        await _matchSetting.InsertAsync(new MatchSettings() { MergeOverlappingGroups = true, ProjectId = projectId }, Constants.Collections.MatchSettings);

        // Create test files with transitive matches
        CreateTransitiveTestFile_DS1(filePath1);
        CreateTransitiveTestFile_DS2(filePath2);

        // Create and insert data sources
        var dataSource1 = CreateComprehensiveExcelDataSource(dataSources["DS1"], filePath1);
        dataSource1.Name = "DS1";
        dataSource1.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource1, Constants.Collections.DataSources);

        var dataSource2 = CreateComprehensiveExcelDataSource(dataSources["DS2"], filePath2);
        dataSource2.Name = "DS2";
        dataSource2.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource2, Constants.Collections.DataSources);

        // Import data sources
        foreach (var ds in dataSources)
        {
            var importStepId = Guid.NewGuid();
            var importStep = new StepJob
            {
                Id = importStepId,
                Type = StepType.Import,
                RunId = runId,
                DataSourceId = ds.Value,
                Configuration = new Dictionary<string, object> { { "DataSourceId", ds.Value } }
            };
            await _stepRespository.InsertAsync(importStep, Constants.Collections.StepJobs);
            var importContext = new CommandContext(runId, projectId, importStepId, _projectRunRepository, _stepRespository);
            await dataImportCommand.ExecuteAsync(importContext, importStep);
        }

        // Setup Match Definitions
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Create match definition with Email exact match
        var matchJobId = Guid.NewGuid();
        var matchRunId = Guid.NewGuid();
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = matchJobId,
            Name = "Transitive Match Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
        {
            new MatchDefinitionMappedRowDto
            {
                ProjectRunId = matchRunId,
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 1.0,
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "Email"
                                },
                                ["DS2"] = new FieldDto {
                                    DataSourceId = dataSources["DS2"],
                                    DataSourceName = "DS2",
                                    Name = "Email"
                                }
                            }
                        }
                    }
                }
            }
        }
        };

        var savedMatchDefId = await _matchDefinitionService.SaveMappedRowConfigurationAsync(mappedRowDto);

        var options = OrchestrationOptions.Default();
        options.RequireTransitiveGroups = false;
        // Execute Matching
        var matchingCommand = new MatchingCommand(
            _recordLinkageOrchestrator, _projectService, _jobEventPublisher,
            _projectRunRepository, _stepRespository, _genericRepository, _mlogger,
             options
        );

        var matchStepId = Guid.NewGuid();
        var matchStep = new StepJob
        {
            Id = matchStepId,
            Type = StepType.Match,
            DataSourceId = savedMatchDefId,
            RunId = matchRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = matchRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(matchStep, Constants.Collections.StepJobs);
        var matchContext = new CommandContext(matchRunId, projectId, matchStepId, _projectRunRepository, _stepRespository);

        // Act
        await matchingCommand.ExecuteAsync(matchContext, matchStep);

        // Assert
        var matchStepData = await _stepRespository.GetByIdAsync(matchStepId, Constants.Collections.StepJobs);
        var outputCollections = matchStepData.StepData.First().CollectionName.Split('|').Select(s => s.Trim()).ToArray();

        // Verify Pairs
        var pairsCollectionName = outputCollections[0];
        var matchedPairs = await _dataStore.GetPagedDataAsync(pairsCollectionName, 1, 100);

        // Should have 4 pairs: (John Smith - John Smith), (John Smith 2 - John Smith2), (John Smith 2 - John Smith 3) forming transitive chain
        Assert.Equal(4, matchedPairs.TotalCount);

        // Verify Groups
        var groupsCollectionName = outputCollections[1];
        var matchGroups = await _dataStore.GetPagedDataAsync(groupsCollectionName, 1, 100);

        // Should have 2 groups:
        // Group 1: John Smith 2, John Smith 2, John Smith 3 (transitive)
        // Group 2: Jane Doe, Jane Doe 2 (direct match)
        // Group 2: John Smith, John Smith (direct match)
        Assert.Equal(3, matchGroups.TotalCount);

        // Verify transitive group has 3 records
        var transitiveGroup = matchGroups.Data
            .FirstOrDefault(g => ((IList<object>)g["Records"]).Count == 3);
        Assert.NotNull(transitiveGroup);

        // Print results for debugging
        Console.WriteLine($"Transitive Test Results:");
        Console.WriteLine($"Pairs: {matchedPairs.TotalCount}");
        Console.WriteLine($"Groups: {matchGroups.TotalCount}");
        foreach (var group in matchGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            Console.WriteLine($"  Group with {records.Count} records");
        }
    }

    [Fact]
    public async Task ExecCommandAsync_TwoDataSources_CreatesNonTransitiveGroups()
    {
        // This test creates non-transitive (isolated) groups
        // Each pair forms its own group without connections

        // Arrange
        var filePath1 = GetTempFilePath("testNonTransitive1.xlsx");
        var filePath2 = GetTempFilePath("testNonTransitive2.xlsx");

        DataImportCommand dataImportCommand = new DataImportCommand(
            _dataSourceService, _projectService, _jobEventPublisher, _genericRepository,
            _recordHasher, _Dlogger,
            _dataStore, _projectRunRepository, _stepRespository, _connectionBuilder, _columnFilter, _secureParameterHandler,
            Mock.Of<IOAuthTokenService>(),
            new RemoteFileConnectorFactory()
        );

        var projectId = Guid.NewGuid();
        var runId = Guid.NewGuid();

        var dataSources = new Dictionary<string, Guid>
        {
            ["DS1"] = Guid.NewGuid(),
            ["DS2"] = Guid.NewGuid()
        };

        // Create project
        var project = new Project
        {
            Id = projectId,
            Name = "Two DS Non-Transitive Test",
            Description = "Test non-transitive grouping with two data sources"
        };
        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        await _matchSetting.InsertAsync(new MatchSettings() { ProjectId = projectId }, Constants.Collections.MatchSettings);

        // Create test files with non-transitive matches
        CreateNonTransitiveTestFile_DS1(filePath1);
        CreateNonTransitiveTestFile_DS2(filePath2);

        // Create and insert data sources
        var dataSource1 = CreateComprehensiveExcelDataSource(dataSources["DS1"], filePath1);
        dataSource1.Name = "DS1";
        dataSource1.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource1, Constants.Collections.DataSources);

        var dataSource2 = CreateComprehensiveExcelDataSource(dataSources["DS2"], filePath2);
        dataSource2.Name = "DS2";
        dataSource2.ProjectId = projectId;
        await _genericRepository.InsertAsync(dataSource2, Constants.Collections.DataSources);

        // Import data sources
        foreach (var ds in dataSources)
        {
            var importStepId = Guid.NewGuid();
            var importStep = new StepJob
            {
                Id = importStepId,
                Type = StepType.Import,
                RunId = runId,
                DataSourceId = ds.Value,
                Configuration = new Dictionary<string, object> { { "DataSourceId", ds.Value } }
            };
            await _stepRespository.InsertAsync(importStep, Constants.Collections.StepJobs);
            var importContext = new CommandContext(runId, projectId, importStepId, _projectRunRepository, _stepRespository);
            await dataImportCommand.ExecuteAsync(importContext, importStep);
        }

        // Setup Match Definitions  
        var pairsCollection = new MatchingDataSourcePairs(new List<MatchingDataSourcePair>())
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId
        };
        pairsCollection.Add(dataSources["DS1"], dataSources["DS2"]);
        await _mockPairRepo.InsertAsync(pairsCollection, Constants.Collections.MatchDataSourcePairs);

        // Create match definition with multiple criteria
        var matchJobId = Guid.NewGuid();
        var matchRunId = Guid.NewGuid();
        var mappedRowDto = new MatchDefinitionCollectionMappedRowDto
        {
            ProjectId = projectId,
            JobId = matchJobId,
            Name = "Non-Transitive Match Test",
            Definitions = new List<MatchDefinitionMappedRowDto>
        {
            new MatchDefinitionMappedRowDto
            {
                ProjectRunId = matchRunId,
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    // Exact email match
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Weight = 1,
                        Arguments = new Dictionary<ArgsValue, string>(),
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "Email"
                                },
                                ["DS2"] = new FieldDto {
                                    DataSourceId = dataSources["DS2"],
                                    DataSourceName = "DS2",
                                    Name = "Email"
                                }
                            }
                        }
                    },
                    // Fuzzy name match
                    new MatchCriterionMappedRowDto
                    {
                        MatchingType = MatchingType.Fuzzy,
                        DataType = CriteriaDataType.Text,
                        Weight = 1,
                        Arguments = new Dictionary<ArgsValue, string> {
                            [ArgsValue.FastLevel] = "0.3",
                            [ArgsValue.Level] = "0.75"
                        },
                        MappedRow = new MappedFieldRowDto
                        {
                            FieldsByDataSource = new Dictionary<string, FieldDto>
                            {
                                ["DS1"] = new FieldDto {
                                    DataSourceId = dataSources["DS1"],
                                    DataSourceName = "DS1",
                                    Name = "FullName"
                                },
                                ["DS2"] = new FieldDto {
                                    DataSourceId = dataSources["DS2"],
                                    DataSourceName = "DS2",
                                    Name = "FullName"
                                }
                            }
                        }
                    }
                }
            }
        }
        };

        var savedMatchDefId = await _matchDefinitionService.SaveMappedRowConfigurationAsync(mappedRowDto);

        // Execute Matching
        var matchingCommand = new MatchingCommand(
            _recordLinkageOrchestrator, _projectService, _jobEventPublisher,
            _projectRunRepository, _stepRespository, _genericRepository, _mlogger
        );

        var matchStepId = Guid.NewGuid();
        var matchStep = new StepJob
        {
            Id = matchStepId,
            Type = StepType.Match,
            DataSourceId = savedMatchDefId,
            RunId = matchRunId,
            Configuration = new Dictionary<string, object> { { "ProjectId", projectId } }
        };

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = matchRunId,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(matchStep, Constants.Collections.StepJobs);
        var matchContext = new CommandContext(matchRunId, projectId, matchStepId, _projectRunRepository, _stepRespository);

        // Act
        await matchingCommand.ExecuteAsync(matchContext, matchStep);

        // Assert
        var matchStepData = await _stepRespository.GetByIdAsync(matchStepId, Constants.Collections.StepJobs);
        var outputCollections = matchStepData.StepData.First().CollectionName.Split('|').Select(s => s.Trim()).ToArray();

        // Verify Pairs
        var pairsCollectionName = outputCollections[0];
        var matchedPairs = await _dataStore.GetPagedDataAsync(pairsCollectionName, 1, 100);

        // Should have 4 isolated pairs
        Assert.Equal(4, matchedPairs.TotalCount);

        // Verify Groups
        var groupsCollectionName = outputCollections[1];
        var matchGroups = await _dataStore.GetPagedDataAsync(groupsCollectionName, 1, 100);

        // Should have 4 groups (each pair forms its own group)
        Assert.Equal(4, matchGroups.TotalCount);

        // All groups should have exactly 2 records (isolated pairs)
        foreach (var group in matchGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            Assert.Equal(2, records.Count);
        }

        // Print results for debugging
        Console.WriteLine($"Non-Transitive Test Results:");
        Console.WriteLine($"Pairs: {matchedPairs.TotalCount}");
        Console.WriteLine($"Groups: {matchGroups.TotalCount}");
        foreach (var group in matchGroups.Data)
        {
            var records = group["Records"] as IList<object>;
            Console.WriteLine($"  Isolated group with {records.Count} records");
        }
    }
    private void CreateTransitiveTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Records that will create transitive chains
        AddTestDataRow(sheet, 1, "John Smith", "john.smith@example.com", "555-111-1111", "123 Main St", "New York", "NY", "10001", "USA", "Analyst", "Finance", "75000", "u1", "jsmith", "Original");
        AddTestDataRow(sheet, 2, "John Smith 2", "john.smith2@example.com", "555-222-2222", "456 Oak Ave", "Boston", "MA", "02101", "USA", "Manager", "Sales", "85000", "u2", "jsmith2", "Links to John Smith 3");
        AddTestDataRow(sheet, 3, "Jane Doe", "jane.doe@example.com", "555-333-3333", "789 Pine Rd", "Chicago", "IL", "60601", "USA", "Engineer", "Tech", "95000", "u3", "jdoe", "Direct match");
        AddTestDataRow(sheet, 4, "Bob Wilson", "bob.wilson@example.com", "555-444-4444", "321 Elm St", "Seattle", "WA", "98101", "USA", "Director", "HR", "100000", "u4", "bwilson", "No match");
        AddTestDataRow(sheet, 5, "Alice Brown", "alice.brown@example.com", "555-555-5555", "654 Maple Ave", "Miami", "FL", "33101", "USA", "Analyst", "Finance", "70000", "u5", "abrown", "No match");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateTransitiveTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Records that create transitive connections
        AddTestDataRow(sheet, 1, "John Smith", "john.smith@example.com", "555-111-1111", "123 Main Street", "New York", "NY", "10001", "USA", "Sr Analyst", "Finance", "78000", "u21", "johnsmith", "Matches DS1 John");
        AddTestDataRow(sheet, 2, "John Smith 2", "john.smith2@example.com", "555-222-2222", "456 Oak Avenue", "Boston", "MA", "02101", "USA", "Sr Manager", "Sales", "88000", "u22", "johnsmith2", "Bridge record");
        AddTestDataRow(sheet, 3, "John Smith 3", "john.smith2@example.com", "555-666-6666", "999 Bridge St", "Hartford", "CT", "06101", "USA", "VP", "Exec", "120000", "u23", "jsmith3", "Same email as John2");
        AddTestDataRow(sheet, 4, "Jane Doe 2", "jane.doe@example.com", "555-777-7777", "789 Pine Road", "Chicago", "IL", "60601", "USA", "Sr Engineer", "Tech", "98000", "u24", "janedoe", "Matches DS1 Jane");
        AddTestDataRow(sheet, 5, "Carol Davis", "carol.davis@example.com", "555-888-8888", "111 First Ave", "Denver", "CO", "80201", "USA", "Manager", "Ops", "82000", "u25", "cdavis", "No match");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    // Helper methods for non-transitive test data
    private void CreateNonTransitiveTestFile_DS1(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Records that will NOT create transitive chains - each match is isolated
        AddTestDataRow(sheet, 1, "Alice Anderson", "alice.a@example.com", "555-100-1000", "100 First St", "Portland", "OR", "97201", "USA", "Analyst", "Finance", "65000", "u101", "aalice", "Isolated match 1");
        AddTestDataRow(sheet, 2, "Bob Brown", "bob.b@example.com", "555-200-2000", "200 Second St", "Austin", "TX", "78701", "USA", "Developer", "Tech", "85000", "u102", "bbob", "Isolated match 2");
        AddTestDataRow(sheet, 3, "Charlie Chen", "charlie.c@example.com", "555-300-3000", "300 Third St", "Phoenix", "AZ", "85001", "USA", "Manager", "Sales", "90000", "u103", "cchen", "Isolated match 3");
        AddTestDataRow(sheet, 4, "Diana Davis", "diana.d@example.com", "555-400-4000", "400 Fourth St", "Atlanta", "GA", "30301", "USA", "Director", "HR", "100000", "u104", "ddiana", "Isolated match 4");
        AddTestDataRow(sheet, 5, "Edward Evans", "edward.e@example.com", "555-500-5000", "500 Fifth St", "Dallas", "TX", "75201", "USA", "VP", "Exec", "120000", "u105", "eevans", "No match");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void CreateNonTransitiveTestFile_DS2(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var headerRow = sheet.CreateRow(0);
        string[] headers = { "FullName", "Email", "PhoneNumber", "Address", "City",
                     "State", "ZipCode", "Country", "JobTitle", "Department",
                     "Salary", "UserId", "Username", "Notes" };
        for (int i = 0; i < headers.Length; i++)
            headerRow.CreateCell(i).SetCellValue(headers[i]);

        // Each record matches only one from DS1 - no chains
        AddTestDataRow(sheet, 1, "Alice A Anderson", "alice.a@example.com", "555-100-1001", "100 First Street", "Portland", "OR", "97201", "USA", "Sr Analyst", "Finance", "68000", "u201", "alice1", "Matches only DS1 Alice");
        AddTestDataRow(sheet, 2, "Robert Brown", "bob.b@example.com", "555-200-2001", "200 Second Street", "Austin", "TX", "78701", "USA", "Sr Developer", "Tech", "88000", "u202", "robert1", "Matches only DS1 Bob");
        AddTestDataRow(sheet, 3, "Charles Chen", "charlie.c@example.com", "555-300-3001", "300 Third Street", "Phoenix", "AZ", "85001", "USA", "Sr Manager", "Sales", "93000", "u203", "charles1", "Matches only DS1 Charlie");
        AddTestDataRow(sheet, 4, "Diana D Davis", "diana.d@example.com", "555-400-4001", "400 Fourth Street", "Atlanta", "GA", "30301", "USA", "Sr Director", "HR", "105000", "u204", "diana1", "Matches only DS1 Diana");
        AddTestDataRow(sheet, 5, "Frank Foster", "frank.f@example.com", "555-600-6000", "600 Sixth St", "Houston", "TX", "77001", "USA", "Manager", "Ops", "75000", "u205", "ffrank", "No match");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }
}
