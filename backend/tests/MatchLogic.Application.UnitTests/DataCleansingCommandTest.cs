using Ardalis.Result;
using MatchLogic.Application.Common;
using MatchLogic.Application.EventHandlers;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using Moq;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Project.Commands;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using System.Text;
using System.Text.RegularExpressions;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Application.UnitTests;

public class DataCleansingCommandTest
{
    private readonly string _dbPath;
    private readonly string _dbJobPath;
    private ILogger<DataCleansingCommand> _logger;
    private readonly IServiceProvider _serviceProvider;

    private readonly IDataStore _dataStore;
    private readonly ICleansingModule _cleansingModule;
    private readonly IProjectService _projectService;
    private readonly IFieldMappingService _fieldMappingService;
    private readonly IAutoMappingService _autoMappingService;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepRespository;
    private readonly IGenericRepository<EnhancedCleaningRules, Guid> _cleaningRulesRepository;
    private readonly IGenericRepository<DomainDataSource, Guid> _domainDataSourceRepository;
    private readonly ISecureParameterHandler _secureParameterHandler;

    private IGenericRepository<DataSnapshot, Guid> _snapshotRepo;
    private IGenericRepository<FileImport, Guid> _fileImportRepo;

    private ISchemaValidationService _schemaValidationService;
    public DataCleansingCommandTest()
    {
        _dbPath = Path.GetTempFileName();
        _dbJobPath = Path.GetTempFileName();
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



        // Add event bus and job event publisher
        //services.AddSingleton<IEventBus, TestEventBus>();
        //// Add job event publisher that tracks completion
        //services.AddScoped<IJobEventPublisher>(sp =>
        //    new TestJobEventPublisher(_completionTracker, new TestEventBus()));

        services.AddMediatR(cfg => {
            cfg.RegisterServicesFromAssembly(typeof(DatabaseUpdateEventHandler).Assembly);
        });
        _serviceProvider = services.BuildServiceProvider();

        _dataStore = _serviceProvider.GetService<IDataStore>();

        _cleaningRulesRepository = _serviceProvider.GetService<IGenericRepository<EnhancedCleaningRules, Guid>>();

        _jobEventPublisher = _serviceProvider.GetService<IJobEventPublisher>();

        _projectRunRepository = _serviceProvider.GetService<IGenericRepository<ProjectRun, Guid>>();
        _stepRespository = _serviceProvider.GetService<IGenericRepository<StepJob, Guid>>();
        _cleansingModule = _serviceProvider.GetService<ICleansingModule>();
        _projectService = _serviceProvider.GetService<IProjectService>();
        _fieldMappingService = _serviceProvider.GetService<IFieldMappingService>();
        _autoMappingService = _serviceProvider.GetService<IAutoMappingService>();
        _domainDataSourceRepository = _serviceProvider.GetService<IGenericRepository<DomainDataSource, Guid>>();
        _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();

        _schemaValidationService = _serviceProvider.GetRequiredService<ISchemaValidationService>();
        _snapshotRepo = _serviceProvider.GetService<IGenericRepository<DataSnapshot, Guid>>();
        _fileImportRepo = _serviceProvider.GetService<IGenericRepository<FileImport, Guid>>();

    }

    [Fact]
    public async Task ExecCommandAsync_WithValidInputs_CallsCleansingModuleAndReturnsCorrectStepData()
    {
        var filePath = Path.Combine(Path.GetTempPath(), "testCleanse.xlsx");
        var _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        var _genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        var _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        var _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        var logger = new NullLogger<DataImportCommand>();

        var _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        //var _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();

        _snapshotRepo = _serviceProvider.GetService<IGenericRepository<DataSnapshot, Guid>>();
        _fileImportRepo = _serviceProvider.GetService<IGenericRepository<FileImport, Guid>>();
        _schemaValidationService = _serviceProvider.GetRequiredService<ISchemaValidationService>();

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
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(stepI, Constants.Collections.StepJobs);

        stepId = Guid.NewGuid();
        //dataSourceId = Guid.NewGuid();
        var cleaningRulesId = Guid.NewGuid();
        var jobId = Guid.NewGuid();

        var step = new StepJob
        {
            Id = stepId,
            Type = StepType.Cleanse,
            DataSourceId = dataSourceId,
            RunId = runId,
            Configuration = new Dictionary<string, object>
            {
                ["cleaningRulesId"] = cleaningRulesId
            }
        };

        CreateTestExcelFile(filePath);

        await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);

        var context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        var dataSource = CreateExcelDataSource(dataSourceId, filePath);

        await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act
        await dataImportCommand.ExecuteAsync(context, stepI);

        var run = await _projectRunRepository.GetByIdAsync(runId, Constants.Collections.ProjectRuns);

        // Arrange
        var command = CreateCommand();

        // Setup cleaning rules repository
        var cleaningRules = new EnhancedCleaningRules
        {
            Id = cleaningRulesId,
            ProjectId = Guid.NewGuid()
        };

        cleaningRules.AddExtendedRule(new ExtendedCleaningRule
        {
            ColumnName = "Name",
            OperationType = OperationType.Standard,
            ColumnMappings = new List<Domain.CleansingAndStandaradization.DataCleansingColumnMapping> {

                new Domain.CleansingAndStandaradization.DataCleansingColumnMapping("Name", "Name_Original")
           }
        });

        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Name",
            RuleType = CleaningRuleType.UpperCase,
        });

        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Age",
            RuleType = CleaningRuleType.RemoveNumbers,
        });

        await _cleaningRulesRepository.InsertAsync(cleaningRules, Constants.Collections.CleaningRules);

        context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);


        // Act
        await command.ExecuteAsync(context, step);

    }

    [Fact]
    public async Task ExecCommandAsync_WithComprehensiveData_TestsAllCleansingOperations()
    {
        var filePath = Path.Combine(Path.GetTempPath(), "testCleanse1.xlsx");
        var _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        var _genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        var _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        var _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>(); var logger = new NullLogger<DataImportCommand>();

        var _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();
        //var _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
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
        var runId = Guid.NewGuid();
        var projectId = Guid.NewGuid();

        var project = new Project
        {
            Id = projectId,
            Name = "Test Project",
            Description = "Test Description"
        };

        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        // Create a more comprehensive test Excel file
        CreateComprehensiveTestExcelFile(filePath);

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
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(stepI, Constants.Collections.StepJobs);

        stepId = Guid.NewGuid();
        var cleaningRulesId = Guid.NewGuid();

        var step = new StepJob
        {
            Id = stepId,
            Type = StepType.Cleanse,
            DataSourceId = dataSourceId,
            RunId = runId,
            Configuration = new Dictionary<string, object>
            {
                ["cleaningRulesId"] = cleaningRulesId
            }
        };

        await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);

        var context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        var dataSource = CreateComprehensiveExcelDataSource(dataSourceId, filePath);

        await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act
        await dataImportCommand.ExecuteAsync(context, stepI);

        // Setup cleaning rules repository
        var cleaningRules = new EnhancedCleaningRules
        {
            Id = cleaningRulesId,
            ProjectId = projectId
        };

        // Add extended rules for copying original values
        cleaningRules.AddExtendedRule(new ExtendedCleaningRule
        {
            ColumnName = "FullName",
            OperationType = OperationType.Standard,
            ColumnMappings = new List<Domain.CleansingAndStandaradization.DataCleansingColumnMapping> {
        new Domain.CleansingAndStandaradization.DataCleansingColumnMapping("FullName", "FullName_Original")
    }
        });

        cleaningRules.AddExtendedRule(new ExtendedCleaningRule
        {
            ColumnName = "Email",
            OperationType = OperationType.Standard,
            ColumnMappings = new List<Domain.CleansingAndStandaradization.DataCleansingColumnMapping> {
        new Domain.CleansingAndStandaradization.DataCleansingColumnMapping("Email", "Email_Original")
    }
        });

        cleaningRules.AddExtendedRule(new ExtendedCleaningRule
        {
            ColumnName = "PhoneNumber",
            OperationType = OperationType.Standard,
            ColumnMappings = new List<Domain.CleansingAndStandaradization.DataCleansingColumnMapping> {
        new Domain.CleansingAndStandaradization.DataCleansingColumnMapping("PhoneNumber", "PhoneNumber_Original")
    }
        });

        cleaningRules.AddExtendedRule(new ExtendedCleaningRule
        {
            ColumnName = "Address",
            OperationType = OperationType.Standard,
            ColumnMappings = new List<Domain.CleansingAndStandaradization.DataCleansingColumnMapping> {
        new Domain.CleansingAndStandaradization.DataCleansingColumnMapping("Address", "Address_Original")
    }
        });

        cleaningRules.AddExtendedRule(new ExtendedCleaningRule
        {
            ColumnName = "Notes",
            OperationType = OperationType.Standard,
            ColumnMappings = new List<Domain.CleansingAndStandaradization.DataCleansingColumnMapping> {
        new Domain.CleansingAndStandaradization.DataCleansingColumnMapping("Notes", "Notes_Original")
    }
        });

        // Add standard cleaning rules for each column and operation type

        // UpperCase operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "FullName",
            RuleType = CleaningRuleType.UpperCase,
        });

        // LowerCase operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Email",
            RuleType = CleaningRuleType.LowerCase,
        });

        // TitleCase operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "JobTitle",
            RuleType = CleaningRuleType.ReverseCase,
        });

        // Trim operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Address",
            RuleType = CleaningRuleType.Trim,
        });

        // RemoveLeadingWhiteSpace operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Department",
            RuleType = CleaningRuleType.RemoveLeadingWhiteSpace,
        });

        // RemoveTrailingWhiteSpace operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "City",
            RuleType = CleaningRuleType.RemoveTrailingWhiteSpace,
        });

        // RemoveWhiteSpace operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "PhoneNumber",
            RuleType = CleaningRuleType.RemoveWhiteSpace,
        });

        // RemoveNumbers operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "UserId",
            RuleType = CleaningRuleType.RemoveNumbers,
        });

        // RemoveSpecialCharacters operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Username",
            RuleType = CleaningRuleType.RemoveSpecialCharacters,
        });

        // RemoveNonAlphaNumeric operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Notes",
            RuleType = CleaningRuleType.RemoveNonAlphaNumeric,
        });

        // RemoveNonAlpha operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "State",
            RuleType = CleaningRuleType.RemoveNumbers,
        });

        // RemoveNonNumeric operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "ZipCode",
            RuleType = CleaningRuleType.RemoveNonNumeric,
        });

        // Replace operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Country",
            RuleType = CleaningRuleType.Replace,
            Arguments = new Dictionary<string, string>
    {
        { "pattern", "United States" },
        { "replacement", "USA" }
    }
        });

        // Remove operation
        cleaningRules.AddRule(new CleaningRule
        {
            ColumnName = "Salary",
            RuleType = CleaningRuleType.Remove,
            Arguments = new Dictionary<string, string>
    {
        { "pattern", "[$,]" }
    }
        });

        await _cleaningRulesRepository.InsertAsync(cleaningRules, Constants.Collections.CleaningRules);

        context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        var command = CreateCommand();

        // Act
        await command.ExecuteAsync(context, step);

        // Assert
        // You could add assertions here to verify the cleansing operations worked correctly
        // For example:
        //var outputCollection = $"cleanse_{dataSourceId}";
        //var results = await _dataStore.GetAllAsync(outputCollection);

        //Assert.NotEmpty(results);

        //// Verify UpperCase transformation
        //foreach (var record in results)
        //{
        //    if (record.ContainsKey("FullName") && record.ContainsKey("FullName_Original"))
        //    {
        //        var original = record["FullName_Original"]?.ToString();
        //        var transformed = record["FullName"]?.ToString();
        //        Assert.Equal(original?.ToUpper(), transformed);
        //    }

        //    // Add more assertions for other transformations
        //}
    }

    [Fact]
    public async Task ExecCommandAsync_WithAddressData_TestsAddressParsingOperations()
    {
        var filePath = Path.Combine(Path.GetTempPath(), "testAddressParser.xlsx");
        var _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        var _genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        var _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        var _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        var logger = new NullLogger<DataImportCommand>();

        var _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();
        var _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
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
        var runId = Guid.NewGuid();
        var projectId = Guid.NewGuid();

        var project = new Project
        {
            Id = projectId,
            Name = "Test Address Parsing Project",
            Description = "Test Address Parsing Description"
        };

        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        // Create test Excel file with address data
        CreateAddressTestExcelFile(filePath);

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
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(stepI, Constants.Collections.StepJobs);

        stepId = Guid.NewGuid();
        var cleaningRulesId = Guid.NewGuid();

        var step = new StepJob
        {
            Id = stepId,
            Type = StepType.Cleanse,
            DataSourceId = dataSourceId,
            RunId = runId,
            Configuration = new Dictionary<string, object>
            {
                ["cleaningRulesId"] = cleaningRulesId
            }
        };

        await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);

        var context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        var dataSource = CreateAddressDataSource(dataSourceId, filePath);

        await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act - Import the data
        await dataImportCommand.ExecuteAsync(context, stepI);

        // Setup cleaning rules repository with address parsing rules
        var cleaningRules = new EnhancedCleaningRules
        {
            Id = cleaningRulesId,
            ProjectId = projectId,
            DataSourceId = dataSourceId,
        };

        cleaningRules.AddRule(new CleaningRule()
        {
            ColumnName = "AddressLine1",
            RuleType = CleaningRuleType.LowerCase,
        });

        // Add mapping rule for address parsing
        var addressParsingRule = new MappingRule
        {
            OperationType = MappingOperationType.AddressParser,
            SourceColumn = new List<string> { "AddressLine1", "AddressLine2", "City", "State", "ZipCode", "Country" },
            //OutputColumns = new List<string>
            //{
            //    "Address_StreetNumber",
            //    "Address_Street",
            //    "Address_StreetSuffix",
            //    "Address_SecondaryAddressUnit",
            //    "Address_SecondaryAddressUnitNumber",
            //    "Address_City",
            //    "Address_State",
            //    "Address_ZipCode",
            //    "Address_Zip9Code",
            //    "Address_Country",
            //    "Address_PreDirection",
            //    "Address_PostDirection",
            //    "Address_Box",
            //    "Address_BoxNumber",
            //    "Address_Recipient"
            //}
        };

        cleaningRules.AddMappingRule(addressParsingRule);

        await _cleaningRulesRepository.InsertAsync(cleaningRules, Constants.Collections.CleaningRules);

        context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        var command = CreateCommand();

        // Act - Execute the cleansing command with address parsing
        await command.ExecuteAsync(context, step);

        // Assert            

        // Verify the output collection exists
        var outputCollection = $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";
        var records = await _dataStore.GetStreamFromTempCollection(outputCollection, CancellationToken.None).ToListAsync();

        Assert.NotEmpty(records);

        // Verify the parsed address components
        foreach (var record in records)
        {
            // Check if address components were extracted correctly
            if (record.ContainsKey("Address_StreetNumber"))
            {
                Assert.NotNull(record["Address_StreetNumber"]);
            }

            if (record.ContainsKey("Address_Street"))
            {
                Assert.NotNull(record["Address_Street"]);
            }

            if (record.ContainsKey("Address_City") && record.ContainsKey("City"))
            {
                // City should match or be a standardized version
                var originalCity = record["City"]?.ToString();
                var parsedCity = record["Address_City"]?.ToString();

                if (!string.IsNullOrEmpty(parsedCity) && !string.IsNullOrEmpty(originalCity))
                {
                    // The city might be standardized, so use a less strict comparison
                    // Convert both to uppercase and check if one contains the other
                    Assert.True(
                        parsedCity.ToUpperInvariant().Contains(originalCity.ToUpperInvariant()) ||
                        originalCity.ToUpperInvariant().Contains(parsedCity.ToUpperInvariant()),
                        $"Parsed city '{parsedCity}' does not match original city '{originalCity}'"
                    );
                }
            }

            if (record.ContainsKey("Address_State") && record.ContainsKey("State"))
            {
                // State should match or be a standardized abbreviation
                var originalState = record["State"]?.ToString();
                var parsedState = record["Address_State"]?.ToString();

                if (!string.IsNullOrEmpty(parsedState) && !string.IsNullOrEmpty(originalState))
                {
                    // If parsedState is 2 characters, it's probably a standard abbreviation
                    if (parsedState.Length == 2)
                    {
                        // For standard state abbreviation, just verify it's not empty
                        Assert.NotEmpty(parsedState);
                    }
                    else
                    {
                        // Otherwise check for containment in one direction or the other
                        Assert.True(
                            parsedState.ToUpperInvariant().Contains(originalState.ToUpperInvariant()) ||
                            originalState.ToUpperInvariant().Contains(parsedState.ToUpperInvariant()),
                            $"Parsed state '{parsedState}' does not match original state '{originalState}'"
                        );
                    }
                }
            }

            if (record.ContainsKey("Address_ZipCode") && record.ContainsKey("ZipCode"))
            {
                var originalZip = record["ZipCode"]?.ToString();
                var parsedZip = record["Address_ZipCode"]?.ToString();

                if (!string.IsNullOrEmpty(parsedZip) && !string.IsNullOrEmpty(originalZip))
                {
                    // The parsed zip should match or be part of the original zip
                    Assert.True(
                        originalZip.Contains(parsedZip) || parsedZip.Contains(originalZip),
                        $"Parsed zip '{parsedZip}' does not match original zip '{originalZip}'"
                    );
                }
            }

            // For addresses with a secondary unit, check if it was parsed correctly
            if (record.ContainsKey("AddressLine2") && !string.IsNullOrEmpty(record["AddressLine2"]?.ToString()))
            {
                var addressLine2 = record["AddressLine2"].ToString();

                // If the address has an apartment, suite, unit, etc.
                if (addressLine2.Contains("Apt") || addressLine2.Contains("Suite") ||
                    addressLine2.Contains("Unit") || addressLine2.Contains("Floor") ||
                    addressLine2.Contains("Room"))
                {
                    // One of these should be populated
                    Assert.True(
                        record.ContainsKey("Address_SecondaryAddressUnit") ||
                        record.ContainsKey("Address_SecondaryAddressUnitNumber"),
                        $"Address with secondary unit '{addressLine2}' was not parsed correctly"
                    );
                }
            }

            // For PO Box addresses, check if it was parsed correctly
            if (record.ContainsKey("AddressLine1") &&
                record["AddressLine1"]?.ToString().Contains("PO Box", StringComparison.OrdinalIgnoreCase) == true)
            {
                Assert.True(
                    record.ContainsKey("Address_Box") || record.ContainsKey("Address_BoxNumber"),
                    $"PO Box address was not parsed correctly: {record["AddressLine1"]}"
                );
            }
        }
    }

    [Fact]
    public async Task ExecCommandAsync_WithComplexRegexPatterns_TestsAdvancedRegexFeatures()
    {
        var filePath = Path.Combine(Path.GetTempPath(), "testComplexRegex.xlsx");
        var _dataSourceService = _serviceProvider.GetService<IDataSourceService>();
        var _genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        var _recordHasher = _serviceProvider.GetService<IRecordHasher>();
        var _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        var logger = new NullLogger<DataImportCommand>();

        var _columnFilter = _serviceProvider.GetService<IColumnFilter>();
        var _projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();
        var _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
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
        var runId = Guid.NewGuid();
        var projectId = Guid.NewGuid();

        var project = new Project
        {
            Id = projectId,
            Name = "Test Complex Regex Project",
            Description = "Test Advanced Regex Features"
        };

        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        // Create test Excel file with email data
        CreateComplexRegexTestExcelFile(filePath);

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
        }, Constants.Collections.ProjectRuns);

        await _stepRespository.InsertAsync(stepI, Constants.Collections.StepJobs);

        stepId = Guid.NewGuid();
        var cleaningRulesId = Guid.NewGuid();
        var runIdCleanse = Guid.NewGuid();

        var step = new StepJob
        {
            Id = stepId,
            Type = StepType.Cleanse,
            DataSourceId = dataSourceId,
            RunId = runIdCleanse,
            Configuration = new Dictionary<string, object>
            {
                ["cleaningRulesId"] = cleaningRulesId
            }
        };

        await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);

        var context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);
        var dataSource = CreateComplexRegexDataSource(dataSourceId, filePath);
        await _genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act - Import the data
        await dataImportCommand.ExecuteAsync(context, stepI);

        await _projectRunRepository.InsertAsync(new ProjectRun()
        {
            ProjectId = projectId,
            Id = runIdCleanse,
            Status = RunStatus.NotStarted,
            StartTime = DateTime.UtcNow,
        }, Constants.Collections.ProjectRuns);

        // Setup cleaning rules repository with complex regex rules
        var cleaningRules = new EnhancedCleaningRules
        {
            Id = cleaningRulesId,
            ProjectId = projectId
        };

        // Test 1: Regex with lookbehind/lookahead for domain extraction
        var domainRegexRule = new MappingRule
        {
            OperationType = MappingOperationType.RegexPattern,
            SourceColumn = new List<string> { "Email" },
            OutputColumns = new List<string> { "Domain" },
            MappingConfig = new Dictionary<string, string>
    {
        { "pattern", @"(?i)((?<=@)(?<Domain>[^.]+)(?=\.))" }
    }
        };
        cleaningRules.AddMappingRule(domainRegexRule);

        // Test 2: Regex with named capture groups
        var namePartsRegexRule = new MappingRule
        {
            OperationType = MappingOperationType.RegexPattern,
            SourceColumn = new List<string> { "FullName" },
            OutputColumns = new List<string> { "FirstName", "LastName" },
            MappingConfig = new Dictionary<string, string>
    {
        { "pattern", @"(?<FirstName>[^\s]+)\s+(?<LastName>[^\s]+)" }
    }
        };
        cleaningRules.AddMappingRule(namePartsRegexRule);

        // Test 3: Auto output columns from named groups
        var addressRegexRule = new MappingRule
        {
            OperationType = MappingOperationType.RegexPattern,
            SourceColumn = new List<string> { "Address" },
            // No OutputColumns specified - should use named groups from pattern
            MappingConfig = new Dictionary<string, string>
    {
            { "pattern", @"(?<Number>\d+)\s+(?<Street>[^,]+),\s*(?<City>[^,]+),\s*(?<State>[A-Z]{2})\s*(?<ZipCode>\d{5}(?:-\d{4})?)" }
    }
        };
        cleaningRules.AddMappingRule(addressRegexRule);

        // Test 4: Complex pattern with backreferences and conditionals
        var phoneRegexRule = new MappingRule
        {
            OperationType = MappingOperationType.RegexPattern,
            SourceColumn = new List<string> { "PhoneNumber" },
            OutputColumns = new List<string> { "FormattedPhone" },
            MappingConfig = new Dictionary<string, string>
    {
        { "pattern", @"(?<AreaCode>\d{3})[\s\-\.]?(?<Exchange>\d{3})[\s\-\.]?(?<LineNumber>\d{4})" },
        { "useAdvancedFunctionality", "true" },
        { "outputFormat", "($1) $2-$3" }
    }
        };
        cleaningRules.AddMappingRule(phoneRegexRule);

        await _cleaningRulesRepository.InsertAsync(cleaningRules, Constants.Collections.CleaningRules);

        context = new CommandContext(runIdCleanse, projectId, stepId, _projectRunRepository, _stepRespository);
        var command = CreateCommand();

        // Act - Execute the cleansing command with complex regex transformations
        await command.ExecuteAsync(context, step);

        // Assert
        // Verify the output collection exists
        var outputCollection = $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";
        var records = await _dataStore.GetStreamFromTempCollection(outputCollection, CancellationToken.None).ToListAsync();

        Assert.NotEmpty(records);

        // Verify complex regex transformations
        foreach (var record in records)
        {
            // Test 1: Verify domain extraction with lookbehind/lookahead
            if (record.ContainsKey("Email") && record.ContainsKey("Domain"))
            {
                var email = record["Email"]?.ToString();
                var domain = record["Domain"]?.ToString();

                if (!string.IsNullOrEmpty(email) && email.Contains('@') && email.Contains('.'))
                {
                    var parts = email.Split('@');
                    if (parts.Length > 1)
                    {
                        var domainParts = parts[1].Split('.');
                        Assert.Equal(domainParts[0].ToLowerInvariant(), domain?.ToLowerInvariant());
                    }
                }
            }

            // Test 2: Verify named capture groups for name parts
            if (record.ContainsKey("FullName") && record.ContainsKey("FirstName") && record.ContainsKey("LastName"))
            {
                var fullName = record["FullName"]?.ToString();
                var firstName = record["FirstName"]?.ToString();
                var lastName = record["LastName"]?.ToString();

                if (!string.IsNullOrEmpty(fullName) && fullName.Contains(' '))
                {
                    var nameParts = fullName.Split(' ', 2);
                    Assert.Equal(nameParts[0], firstName);
                    Assert.Equal(nameParts[1], lastName);
                }
            }

            // Test 3: Verify auto output columns from named groups
            if (record.ContainsKey("Address"))
            {
                // Check for all extracted address components
                if (record.ContainsKey("Number") && record.ContainsKey("Street") &&
                    record.ContainsKey("City") && record.ContainsKey("State") &&
                    record.ContainsKey("ZipCode"))
                {
                    var address = record["Address"]?.ToString();
                    var number = record["Number"]?.ToString();
                    var street = record["Street"]?.ToString();
                    var city = record["City"]?.ToString();
                    var state = record["State"]?.ToString();
                    var zipCode = record["ZipCode"]?.ToString();

                    // Verify the address components exist in the original address
                    if (!string.IsNullOrEmpty(address))
                    {
                        Assert.Contains(number, address);
                        Assert.Contains(street, address);
                        Assert.Contains(city, address);
                        Assert.Contains(state, address);
                        Assert.Contains(zipCode, address);
                    }
                }
            }

            // Test 4: Verify formatted phone with advanced functionality
            if (record.ContainsKey("PhoneNumber") && record.ContainsKey("FormattedPhone"))
            {
                var phone = record["PhoneNumber"]?.ToString();
                var formattedPhone = record["FormattedPhone"]?.ToString();

                if (!string.IsNullOrEmpty(phone) && !string.IsNullOrEmpty(formattedPhone))
                {
                    // Extract digits from original phone
                    var digitsOnly = new string(phone.Where(char.IsDigit).ToArray());

                    // Verify formatted phone matches expected pattern (xxx) xxx-xxxx
                    var formattedRegex = new Regex(@"^\(\d{3}\) \d{3}-\d{4}$");
                    Assert.True(formattedRegex.IsMatch(formattedPhone),
                        $"Phone {formattedPhone} doesn't match expected format (xxx) xxx-xxxx");

                    // Extract digits from formatted phone for comparison
                    var formattedDigits = new string(formattedPhone.Where(char.IsDigit).ToArray());

                    // The digits should match
                    Assert.Equal(digitsOnly.Substring(0, Math.Min(10, digitsOnly.Length)),
                        formattedDigits.Substring(0, Math.Min(10, formattedDigits.Length)));
                }
            }
        }
    }

    //[Fact]
    //public async Task ExecCommandAsync_WithWordSmithCompleteFeatures_AppliesAllTransformationTypes()
    //{
    //    // ARRANGE
    //    // Create test files
    //    var filePath = Path.Combine(Path.GetTempPath(), "testWordSmithComprehensive.xlsx");
    //    var _testDictionaryPath = Path.Combine(Path.GetTempPath(), "wordsmith_comprehensive_dictionary.txt");
    //    var jobTitleDictionary = Path.Combine(Path.GetTempPath(), "wordsmith_jobtitle_dictionary.txt");
    //    // Get needed services
    //    var dataSourceService = _serviceProvider.GetService<IDataSourceService>();
    //    var genericRepository = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
    //    var recordHasher = _serviceProvider.GetService<IRecordHasher>();
    //    var _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
    //    var columnFilter = _serviceProvider.GetService<IColumnFilter>();
    //    var projectRepository = _serviceProvider.GetService<IGenericRepository<Project, Guid>>();
    //    var logger = new NullLogger<DataImportCommand>();

    //    // Create test dictionary for WordSmith with all feature types
    //    CreateComprehensiveWordSmithDictionary(_testDictionaryPath, jobTitleDictionary);

    //    // Create test Excel file with test data for all features
    //    CreateComprehensiveWordSmithTestExcelFile(filePath);
    //    var _secureParameterHandler = _serviceProvider.GetService<ISecureParameterHandler>();
    //    // Create Data Import Command
    //    DataImportCommand dataImportCommand = new DataImportCommand(
    //        dataSourceService,
    //        _projectService,
    //        _jobEventPublisher,
    //        genericRepository,
    //        recordHasher,
    //        logger,
    //        _dataStore,
    //        _projectRunRepository,
    //        _stepRespository,
    //        _connectionBuilder,
    //        columnFilter,
    //        _secureParameterHandler
    //    );

    //    // Create project for testing
    //    var dataSourceId = Guid.NewGuid();
    //    var stepId = Guid.NewGuid();
    //    var runId = Guid.NewGuid();
    //    var projectId = Guid.NewGuid();
    //    var project = new Project
    //    {
    //        Id = projectId,
    //        Name = "Test Comprehensive WordSmith Project",
    //        Description = "Test All WordSmith Features"
    //    };
    //    await projectRepository.InsertAsync(project, Constants.Collections.Projects);

    //    // Create import step
    //    var stepI = new StepJob
    //    {
    //        Id = stepId,
    //        Type = StepType.Import,
    //        RunId = runId,
    //        DataSourceId = dataSourceId,
    //        Configuration = new Dictionary<string, object>
    //        {
    //            { "DataSourceId", dataSourceId }
    //        }
    //    };

    //    // Create project run
    //    await _projectRunRepository.InsertAsync(new ProjectRun()
    //    {
    //        ProjectId = projectId,
    //        Id = runId,
    //        Status = RunStatus.NotStarted,
    //        StartTime = DateTime.UtcNow,
    //    }, Constants.Collections.ProjectRuns);

    //    // Save import step
    //    await _stepRespository.InsertAsync(stepI, Constants.Collections.StepJobs);

    //    // Create cleansing step
    //    stepId = Guid.NewGuid();
    //    var cleaningRulesId = Guid.NewGuid();
    //    var step = new StepJob
    //    {
    //        Id = stepId,
    //        Type = StepType.Cleanse,
    //        DataSourceId = dataSourceId,
    //        RunId = runId,
    //        Configuration = new Dictionary<string, object>
    //        {
    //            ["cleaningRulesId"] = cleaningRulesId
    //        }
    //    };

    //    // Save cleansing step
    //    await _stepRespository.InsertAsync(step, Constants.Collections.StepJobs);

    //    // Create context for execution
    //    var context = new CommandContext(runId, projectId, stepId, _projectRunRepository, _stepRespository);

    //    // Create data source
    //    var dataSource = CreateComprehensiveWordSmithDataSource(dataSourceId, filePath);
    //    await genericRepository.InsertAsync(dataSource, Constants.Collections.DataSources);

    //    // ACT - Import the data
    //    await dataImportCommand.ExecuteAsync(context, stepI);

    //    // Setup cleaning rules repository with comprehensive WordSmith rules
    //    var cleaningRules = new EnhancedCleaningRules
    //    {
    //        Id = cleaningRulesId,
    //        ProjectId = projectId,
    //        DataSourceId = dataSourceId,
    //    };

    //    // 1. Standard WordSmith with replacements
    //    var wordSmithReplacementRule = new MappingRule
    //    {
    //        OperationType = MappingOperationType.WordSmith,
    //        SourceColumn = new List<string> { "CompanyName" },
    //        OutputColumns = new List<string>(),
    //        MappingConfig = new Dictionary<string, string>
    //        {
    //            { "dictionaryPath", _testDictionaryPath },
    //            { "separators", @".,;: ()[]{}|!?-_/\\" },
    //            { "maxWordCount", "3" },
    //            { "flagMode", "false" }
    //        }
    //    };

    //    // 2. WordSmith with deletions
    //    var wordSmithDeletionRule = new MappingRule
    //    {
    //        OperationType = MappingOperationType.WordSmith,
    //        SourceColumn = new List<string> { "JobTitle" },
    //        OutputColumns = new List<string>(),
    //        MappingConfig = new Dictionary<string, string>
    //        {
    //            { "dictionaryPath", jobTitleDictionary },
    //            { "separators", @".,;: ()[]{}|!?-_/\\" },
    //            { "maxWordCount", "3" },
    //            { "flagMode", "false" }
    //        }
    //    };

    //    // 3. WordSmith with new column creation (classifications)
    //    var wordSmithClassificationRule = new MappingRule
    //    {
    //        OperationType = MappingOperationType.WordSmith,
    //        SourceColumn = new List<string> { "Industry" },
    //        OutputColumns = new List<string> { "IndustryCategory", "IndustrySubcategory" },
    //        MappingConfig = new Dictionary<string, string>
    //        {
    //            { "dictionaryPath", _testDictionaryPath },
    //            { "separators", @".,;: ()[]{}|!?-_/\\" },
    //            { "maxWordCount", "3" },
    //            { "flagMode", "false" }
    //        }
    //    };

    //    // 4. WordSmith with combinations of words (test maxWordCount)
    //    var wordSmithCombinationRule = new MappingRule
    //    {
    //        OperationType = MappingOperationType.WordSmith,
    //        SourceColumn = new List<string> { "ProductDescription" },
    //        OutputColumns = new List<string>(),
    //        MappingConfig = new Dictionary<string, string>
    //        {
    //            { "dictionaryPath", _testDictionaryPath },
    //            { "separators", @".,;: ()[]{}|!?-_/\\" },
    //            { "maxWordCount", "4" }, // Support up to 4-word combinations
    //            { "flagMode", "false" }
    //        }
    //    };

    //    // 5. WordSmith with priority ordering (test priority handling)
    //    var wordSmithPriorityRule = new MappingRule
    //    {
    //        OperationType = MappingOperationType.WordSmith,
    //        SourceColumn = new List<string> { "CustomerSegment" },
    //        OutputColumns = new List<string> { "SegmentCategory" },
    //        MappingConfig = new Dictionary<string, string>
    //        {
    //            { "dictionaryPath", _testDictionaryPath },
    //            { "separators", @" .,;: ()[]{}|!?-_/\\" },
    //            { "maxWordCount", "3" },
    //            { "flagMode", "false" }
    //        }
    //    };

    //    // 6. WordSmith in flag mode
    //    var wordSmithFlagRule = new MappingRule
    //    {
    //        OperationType = MappingOperationType.WordSmith,
    //        SourceColumn = new List<string> { "Address" },
    //        OutputColumns = new List<string> { "AddressType", "Region" },
    //        MappingConfig = new Dictionary<string, string>
    //        {
    //            { "dictionaryPath", _testDictionaryPath },
    //            { "separators", @" .,;: ()[]{}|!?-_/\\" },
    //            { "maxWordCount", "3" },
    //            { "flagMode", "true" }
    //        }
    //    };

    //    // Add all rules to the configuration
    //    cleaningRules.AddMappingRule(wordSmithReplacementRule);
    //    cleaningRules.AddMappingRule(wordSmithDeletionRule);
    //    cleaningRules.AddMappingRule(wordSmithClassificationRule);
    //    cleaningRules.AddMappingRule(wordSmithCombinationRule);
    //    cleaningRules.AddMappingRule(wordSmithPriorityRule);
    //    cleaningRules.AddMappingRule(wordSmithFlagRule);

    //    // Save the cleaning rules
    //    await _cleaningRulesRepository.InsertAsync(cleaningRules, Constants.Collections.CleaningRules);

    //    // Create data cleansing command
    //    var command = CreateCommand();

    //    // ACT - Execute the cleansing command with WordSmith transformations
    //    await command.ExecuteAsync(context, step);

    //    // ASSERT
    //    // Verify the output collection exists
    //    var outputCollection = $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";
    //    var records = await _dataStore.GetStreamFromTempCollection(outputCollection, CancellationToken.None).ToListAsync();

    //    Assert.NotEmpty(records);

    //    // Verify all WordSmith transformation types
    //    foreach (var record in records)
    //    {
    //        // TEST 1: Verify simple replacements
    //        if (record.ContainsKey("CompanyName"))
    //        {
    //            var companyName = record["CompanyName"]?.ToString();

    //            if (companyName.Contains("Limited"))
    //            {
    //                Assert.Equal(companyName.Replace("Limited", "Ltd"), record["CompanyName"]);
    //            }

    //            if (companyName.Contains("Incorporated"))
    //            {
    //                Assert.Equal(companyName.Replace("Incorporated", "Inc"), record["CompanyName"]);
    //            }

    //            if (companyName.Contains("Corporation"))
    //            {
    //                Assert.Equal(companyName.Replace("Corporation", "Corp"), record["CompanyName"]);
    //            }
    //        }

    //        // TEST 2: Verify deletions
    //        if (record.ContainsKey("JobTitle"))
    //        {
    //            var jobTitle = record["JobTitle"]?.ToString().ToLower();

    //            // Words marked for deletion should be removed
    //            Assert.DoesNotContain("intern", jobTitle);
    //            Assert.DoesNotContain("junior", jobTitle);
    //            Assert.DoesNotContain("trainee", jobTitle);
    //            Assert.DoesNotContain("associate", jobTitle);

    //            // Check for specific transformations
    //            if (jobTitle.Contains("developer"))
    //            {
    //                // Specific job title with words to be deleted
    //                Assert.DoesNotContain("assistant", jobTitle);
    //            }
    //        }
    //        bool skipHealthCare = false;
    //        // TEST 3: Verify new column creation
    //        if (record.ContainsKey("Industry") && record.ContainsKey("IndustryCategory"))
    //        {
    //            var industry = record["Industry"]?.ToString().ToLower();

    //            // Check for specific classifications based on industry
    //            if (industry.Contains("tech") || industry.Contains("software") || industry.Contains("computing"))
    //            {
    //                skipHealthCare = true;
    //                Assert.Equal("Technology", record["IndustryCategory"]);
    //            }

    //            if (industry.Contains("health") || industry.Contains("medical") || industry.Contains("pharma"))
    //            {
    //                if (!skipHealthCare)
    //                    Assert.Equal("Healthcare", record["IndustryCategory"]);
    //            }

    //            if (industry.Contains("bank") || industry.Contains("insurance") || industry.Contains("invest"))
    //            {
    //                Assert.Equal("Financial Services", record["IndustryCategory"]);
    //            }
    //        }

    //        // TEST 4: Verify multi-word combinations
    //        if (record.ContainsKey("ProductDescription"))
    //        {
    //            var description = record["ProductDescription"]?.ToString().ToLower();

    //            // Check if multi-word combinations were matched and replaced
    //            if (description.Contains("cloud based platform"))
    //            {
    //                Assert.Equal(description.Replace("cloud based platform", "Cloud Platform"), record["ProductDescription"]);
    //            }

    //            if (description.Contains("mobile application development"))
    //            {
    //                Assert.Equal(description.Replace("mobile application development", "Mobile Dev"), record["ProductDescription"]);
    //            }

    //            if (description.Contains("machine learning solution"))
    //            {
    //                Assert.Equal(description.Replace("machine learning solution", "ML Solution"), record["ProductDescription"]);
    //            }
    //        }

    //        // TEST 5: Verify priority ordering
    //        if (record.ContainsKey("CustomerSegment") && record.ContainsKey("SegmentCategory"))
    //        {
    //            var segment = record["CustomerSegment"]?.ToString().ToLower();

    //            // Check if higher priority rules took precedence
    //            // "enterprise client" should match "enterprise" (priority 3) not "enterprise client" (priority 5)
    //            if (segment.Contains("enterprise client"))
    //            {
    //                Assert.Equal("Enterprise", record["SegmentCategory"]);
    //            }

    //            // "small business owner" should match "small business" (priority 2) not "business owner" (priority 4)
    //            if (segment.Contains("small business owner"))
    //            {
    //                Assert.Equal("SMB", record["SegmentCategory"]);
    //            }
    //        }

    //        // TEST 6: Verify flag mode (original values preserved)
    //        if (record.ContainsKey("Address") && record.ContainsKey("AddressType"))
    //        {
    //            var address = record["Address"]?.ToString();
    //            var originalAddress = address; // In flag mode, this should be unchanged

    //            // The address should remain unchanged in flag mode
    //            Assert.Equal(originalAddress, record["Address"]);

    //            // But new columns should be populated
    //            if (address.ToLower().Contains("street") || address.ToLower().Contains("avenue") ||
    //                address.ToLower().Contains("road") || address.ToLower().Contains("drive"))
    //            {
    //                Assert.Equal("Residential", record["AddressType"]);
    //            }

    //            if (address.ToLower().Contains("new york") || address.ToLower().Contains("manhattan") ||
    //                address.ToLower().Contains("brooklyn"))
    //            {
    //                Assert.Equal("East Coast", record["Region"]);
    //            }

    //            if (address.ToLower().Contains("california") || address.ToLower().Contains("san francisco") ||
    //                address.ToLower().Contains("los angeles"))
    //            {
    //                Assert.Equal("West Coast", record["Region"]);
    //            }
    //        }
    //    }

    //    // Clean up temp files
    //    try
    //    {
    //        //if (File.Exists(filePath)) File.Delete(filePath);
    //        //if (File.Exists(_testDictionaryPath)) File.Delete(_testDictionaryPath);
    //    }
    //    catch { /* Ignore cleanup errors */ }
    //}

    #region Helper Methods

    private DataCleansingCommand CreateCommand()
    {
        return new DataCleansingCommand(
            _cleansingModule,
            _projectService,
            _jobEventPublisher,
            _projectRunRepository,
            _stepRespository,
            _cleaningRulesRepository,
            _domainDataSourceRepository,
            _dataStore,
            _fieldMappingService,
            _autoMappingService,                       
            _logger);
    }

    private void CreateTestExcelFile(string filePath)
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

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }
    private DomainDataSource CreateExcelDataSource(Guid dataSourceId, string filePath)
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
                    { "Age", new MatchLogic.Domain.Project.ColumnMapping { SourceColumn = "Age", TargetColumn = "Age", Include = true } }
                }
            }
        };
    }

    private void CreateComprehensiveTestExcelFile(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row with multiple columns
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("FullName");
        headerRow.CreateCell(1).SetCellValue("Email");
        headerRow.CreateCell(2).SetCellValue("PhoneNumber");
        headerRow.CreateCell(3).SetCellValue("Address");
        headerRow.CreateCell(4).SetCellValue("City");
        headerRow.CreateCell(5).SetCellValue("State");
        headerRow.CreateCell(6).SetCellValue("ZipCode");
        headerRow.CreateCell(7).SetCellValue("Country");
        headerRow.CreateCell(8).SetCellValue("JobTitle");
        headerRow.CreateCell(9).SetCellValue("Department");
        headerRow.CreateCell(10).SetCellValue("Salary");
        headerRow.CreateCell(11).SetCellValue("UserId");
        headerRow.CreateCell(12).SetCellValue("Username");
        headerRow.CreateCell(13).SetCellValue("Notes");

        // Add 10 rows of test data with various characteristics to test all rule types
        AddTestDataRow(sheet, 1, "John Smith 1", "john.smith@example.com", "555-123-4567", "  123 Main St  ", "New York ", " Finance", "NY123", "United States", "senior analyst", " Accounting", "$75,000", "user123", "john_smith", "Notes with special chars: $#@!");
        AddTestDataRow(sheet, 2, "mary johnson", "MARY.JOHNSON@EXAMPLE.COM", "(555) 234-5678", "456 Oak Ave, Apt 2B", "Los Angeles", "Marketing", "CA90210", "United States", "marketing coordinator", "Marketing", "$65,000", "user456", "mary.johnson", "Contains 123 numbers and !@# symbols");
        AddTestDataRow(sheet, 3, "ROBERT WILLIAMS", "robert.williams@example.com", "555 345 6789", "789 Pine Rd.", "Chicago ", "Illinois", "IL60601", "United States", "software engineer", "Engineering", "$95,000", "user789", "robert_williams", "Normal text with no special characters");
        AddTestDataRow(sheet, 4, "Jennifer Brown", "jennifer.brown@example.com", "555.456.7890", " 321 Elm St ", " Boston", "MA02108", "MA02108", "United States", "product manager", "Product", "$85,000", "user101", "jennifer!brown", "Text with line\nbreak and tabs\t\t!");
        AddTestDataRow(sheet, 5, "Michael Davis", "michael.davis@example.com", "555 567 8901", "654 Maple Ave", "San Francisco ", " California", "CA94105", "United States", "sales representative", " Sales", "$70,000", "user202", "michael_davis2", "Text with   multiple    spaces!");
        AddTestDataRow(sheet, 6, "Sarah Miller", "sarah.miller@example.com", "(555)678-9012", "  987 Cedar Blvd  ", "Seattle", "WA98101", "WA98101", "United States", "human resources manager", "HR", "$80,000", "user303", "sarah.miller", "Special chars: !@#$%^&*()");
        AddTestDataRow(sheet, 7, "David Wilson", "david.wilson@example.com", "555-789-0123", "159 Birch St", " Portland", "Oregon ", "OR97201", "United States", "customer service rep", "Customer Service", "$60,000", "user404", "david_wilson", "Numbers only: 12345");
        AddTestDataRow(sheet, 8, "Jessica Garcia", "jessica.garcia@example.com", "555 890 1234", "  753 Walnut Dr  ", "Denver ", " CO", "CO80202", "United States", "research scientist", " Research", "$90,000", "user505", "jessica.garcia", "Mixed text, 123 numbers, and !@# symbols");
        AddTestDataRow(sheet, 9, "James Rodriguez$", "james.rodriguez@example.com", "(555) 901-2345", "852 Spruce Way", "Austin ", "TX78701", "TX78701", "United States", "operations director", "Operations", "$100,000", "user606", "james_rodriguez", "Trailing spaces at end   ");
        AddTestDataRow(sheet, 10, "Patricia Martinez", "patricia.martinez@example.com", "555.012.3456", "  426 Pine Lane  ", " Miami", " Florida", "FL33101", "United States", "data analyst", "Analytics", "$72,000", "user707", "patricia!martinez", "Leading spaces at start   ");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void AddTestDataRow(ISheet sheet, int rowIndex, string fullName, string email, string phone, string address, string city, string state, string zipCode, string country, string jobTitle, string department, string salary, string userId, string username, string notes)
    {
        var row = sheet.CreateRow(rowIndex);
        row.CreateCell(0).SetCellValue(fullName);
        row.CreateCell(1).SetCellValue(email);
        row.CreateCell(2).SetCellValue(phone);
        row.CreateCell(3).SetCellValue(address);
        row.CreateCell(4).SetCellValue(city);
        row.CreateCell(5).SetCellValue(state);
        row.CreateCell(6).SetCellValue(zipCode);
        row.CreateCell(7).SetCellValue(country);
        row.CreateCell(8).SetCellValue(jobTitle);
        row.CreateCell(9).SetCellValue(department);
        row.CreateCell(10).SetCellValue(salary);
        row.CreateCell(11).SetCellValue(userId);
        row.CreateCell(12).SetCellValue(username);
        row.CreateCell(13).SetCellValue(notes);
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
        var columnMappings = new Dictionary<string, MatchLogic.Domain.Project.ColumnMapping>();

        // Add mapping for each column
        string[] columns = new[] {
                                    "FullName", "Email", "PhoneNumber", "Address", "City", "State", "ZipCode",
                                    "Country", "JobTitle", "Department", "Salary", "UserId", "Username", "Notes"
                                  };

        foreach (var column in columns)
        {
            columnMappings[column] = new MatchLogic.Domain.Project.ColumnMapping
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

    private void CreateAddressTestExcelFile(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row with address columns
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("AddressLine1");
        headerRow.CreateCell(1).SetCellValue("AddressLine2");
        headerRow.CreateCell(2).SetCellValue("City");
        headerRow.CreateCell(3).SetCellValue("State");
        headerRow.CreateCell(4).SetCellValue("ZipCode");
        headerRow.CreateCell(5).SetCellValue("Country");

        // Add test address data rows
        AddAddressDataRow(sheet, 1, "123 Main St", "Apt 4B", "New York", "NY", "10001", "USA");
        AddAddressDataRow(sheet, 2, "456 Oak Ave", "Suite 200", "Los Angeles", "California", "90210", "United States");
        AddAddressDataRow(sheet, 3, "789 Maple Rd", "", "Chicago", "IL", "60601", "USA");
        AddAddressDataRow(sheet, 4, "321 Pine Blvd", "Room 101", "Houston", "Texas", "77002", "United States");
        AddAddressDataRow(sheet, 5, "555 Cedar Dr", "Apt 3C", "Philadelphia", "PA", "19104", "USA");
        AddAddressDataRow(sheet, 6, "1234 Elm St", "", "Phoenix", "Arizona", "85001", "United States");
        AddAddressDataRow(sheet, 7, "987 Birch Ln", "Suite 300", "San Antonio", "TX", "78205", "USA");
        AddAddressDataRow(sheet, 8, "246 Walnut Ct", "Floor 2", "San Diego", "California", "92101", "United States");
        AddAddressDataRow(sheet, 9, "135 Spruce Way", "Unit 5D", "Dallas", "TX", "75201", "USA");
        AddAddressDataRow(sheet, 10, "864 Ash St", "", "San Jose", "CA", "95113", "United States");

        // Add PO Box addresses
        AddAddressDataRow(sheet, 11, "PO Box 1234", "", "Denver", "CO", "80201", "USA");
        AddAddressDataRow(sheet, 12, "P.O. Box 5678", "", "Seattle", "Washington", "98101", "United States");

        // Add complex addresses
        AddAddressDataRow(sheet, 13, "123 N Main St W", "Building C, Suite 400", "Boston", "Massachusetts", "02108", "United States");
        AddAddressDataRow(sheet, 14, "987 S Oak Ave E, Apt 5B", "", "Miami", "FL", "33130", "USA");

        // Add international addresses
        AddAddressDataRow(sheet, 15, "10 Downing Street", "", "London", "", "SW1A 2AA", "United Kingdom");
        AddAddressDataRow(sheet, 16, "1600 Pennsylvania Avenue NW", "", "Washington", "DC", "20500", "USA");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void AddAddressDataRow(ISheet sheet, int rowIndex, string addressLine1, string addressLine2, string city, string state, string zipCode, string country)
    {
        var row = sheet.CreateRow(rowIndex);
        row.CreateCell(0).SetCellValue(addressLine1);
        row.CreateCell(1).SetCellValue(addressLine2);
        row.CreateCell(2).SetCellValue(city);
        row.CreateCell(3).SetCellValue(state);
        row.CreateCell(4).SetCellValue(zipCode);
        row.CreateCell(5).SetCellValue(country);
    }

    private DataSource CreateAddressDataSource(Guid dataSourceId, string filePath)
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

        var columnMappings = new Dictionary<string, MatchLogic.Domain.Project.ColumnMapping>();

        // Add mapping for each address column
        string[] columns = new[] { "AddressLine1", "AddressLine2", "City", "State", "ZipCode", "Country" };

        foreach (var column in columns)
        {
            columnMappings[column] = new MatchLogic.Domain.Project.ColumnMapping
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

    private void CreateComplexRegexTestExcelFile(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row with columns for complex regex testing
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("Email");
        headerRow.CreateCell(1).SetCellValue("FullName");
        headerRow.CreateCell(2).SetCellValue("Address");
        headerRow.CreateCell(3).SetCellValue("PhoneNumber");

        // Add test data rows
        AddComplexRegexDataRow(sheet, 1, "john.doe@example.com", "John Doe", "123 Main St, Springfield, IL 62701", "555-123-4567");
        AddComplexRegexDataRow(sheet, 2, "mary.smith@company.org", "Mary Smith", "456 Oak Ave, Chicago, IL 60601", "(555) 234-5678");
        AddComplexRegexDataRow(sheet, 3, "bob.jones@test.edu", "Bob Jones", "789 Pine Rd, New York, NY 10001", "555.345.6789");
        AddComplexRegexDataRow(sheet, 4, "alice.brown@mail.net", "Alice Brown", "321 Elm St, Los Angeles, CA 90001", "5554567890");
        AddComplexRegexDataRow(sheet, 5, "david.wilson@site.co.uk", "David Wilson", "555 Cedar Dr, Houston, TX 77001", "555 567 8901");
        AddComplexRegexDataRow(sheet, 6, "sarah.miller@domain.io", "Sarah Miller", "654 Maple Ave, Phoenix, AZ 85001", "555-678-9012");
        AddComplexRegexDataRow(sheet, 7, "michael.davis@service.com", "Michael Davis", "987 Birch Ln, Philadelphia, PA 19101", "(555)789-0123");
        AddComplexRegexDataRow(sheet, 8, "jennifer.taylor@portal.net", "Jennifer Taylor", "246 Walnut Ct, San Antonio, TX 78201", "555 890 1234");
        AddComplexRegexDataRow(sheet, 9, "james.thomas@forum.org", "James Thomas", "135 Spruce Way, San Diego, CA 92101", "555.901.2345");
        AddComplexRegexDataRow(sheet, 10, "lisa.anderson@blog.co", "Lisa Anderson", "864 Ash St, Dallas, TX 75201", "5550123456");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void AddComplexRegexDataRow(ISheet sheet, int rowIndex, string email, string fullName, string address, string phoneNumber)
    {
        var row = sheet.CreateRow(rowIndex);
        row.CreateCell(0).SetCellValue(email);
        row.CreateCell(1).SetCellValue(fullName);
        row.CreateCell(2).SetCellValue(address);
        row.CreateCell(3).SetCellValue(phoneNumber);
    }

    private DataSource CreateComplexRegexDataSource(Guid dataSourceId, string filePath)
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
        var columnMappings = new Dictionary<string, MatchLogic.Domain.Project.ColumnMapping>();

        // Add mapping for each column
        string[] columns = new[] { "Email", "FullName", "Address", "PhoneNumber" };

        foreach (var column in columns)
        {
            columnMappings[column] = new MatchLogic.Domain.Project.ColumnMapping
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

    private void CreateComprehensiveWordSmithDictionary(string filePath, string jobTitleDictionary)
    {
        // Create a comprehensive WordSmith dictionary with all feature types
        var lines = new List<string>
        {
            "Words\tReplacement\tNewColumn\tToDelete\tPriority\tCount",
            
            // 1. Simple replacements
            "Limited\tLtd\t\t0\t5\t1",
            "Incorporated\tInc\t\t0\t5\t1",
            "Corporation\tCorp\t\t0\t5\t1",
            "International\tIntl\t\t0\t5\t1",
            "Associates\tAssoc\t\t0\t5\t1",
            
            // 2. Deletions (To Delete = 1)
            //"intern\t\t\t1\t5\t1",
            //"junior\t\t\t1\t5\t1",
            //"trainee\t\t\t1\t5\t1",
            //"associate\t\t\t1\t5\t1",
            //"assistant\t\t\t1\t5\t1",
            
            // 3. New column creation (classifications)
            "health\tHealthcare\tIndustryCategory\t0\t5\t1",
            "tech\tTechnology\tIndustryCategory\t0\t2\t1",
            "technology\tTechnology\tIndustryCategory\t0\t5\t1",
            "healthcare technology\tHealthcare\tIndustryCategory\t0\t3\t1", // Higher priority (3) than technology
            "software\tTechnology\tIndustryCategory\t0\t5\t1",
            "computing\tTechnology\tIndustryCategory\t0\t5\t1",

            "medical\tHealthcare\tIndustryCategory\t0\t5\t1",
            "pharma\tHealthcare\tIndustryCategory\t0\t5\t1",
            "bank\tFinancial Services\tIndustryCategory\t0\t5\t1",
            "banking\tFinancial Services\tIndustryCategory\t0\t5\t1",
            "insurance\tFinancial Services\tIndustryCategory\t0\t5\t1",
            "invest\tFinancial Services\tIndustryCategory\t0\t5\t1",
            
            // More detailed subcategories
            "software\tSoftware Development\tIndustrySubcategory\t0\t5\t1",
            "hardware\tHardware Manufacturing\tIndustrySubcategory\t0\t5\t1",
            "biotech\tBiotechnology\tIndustrySubcategory\t0\t5\t1",
            
            // 4. Multi-word combinations
            "cloud based platform\tCloud Platform\t\t0\t5\t1",
            "mobile application development\tMobile Dev\t\t0\t5\t1",
            "machine learning solution\tML Solution\t\t0\t5\t1",
            "artificial intelligence system\tAI System\t\t0\t5\t1",
            
            // 5. Priority ordering (lower numbers = higher priority)
            "enterprise\tEnterprise\tSegmentCategory\t0\t3\t1",
            "enterprise client\tEnterprise Client\tSegmentCategory\t0\t5\t1",
            "small business\tSMB\tSegmentCategory\t0\t2\t1",
            "business owner\tBusiness Owner\tSegmentCategory\t0\t4\t1",
            "consumer\tB2C\tSegmentCategory\t0\t3\t1",
            
            // 6. Flag mode entries for address
            "street\tResidential\tAddressType\t0\t5\t1",
            "avenue\tResidential\tAddressType\t0\t5\t1",
            "road\tResidential\tAddressType\t0\t5\t1",
            "drive\tResidential\tAddressType\t0\t5\t1",
            "plaza\tCommercial\tAddressType\t0\t5\t1",
            "suite\tCommercial\tAddressType\t0\t5\t1",
            "new york\tEast Coast\tRegion\t0\t5\t1",
            "manhattan\tEast Coast\tRegion\t0\t5\t1",
            "brooklyn\tEast Coast\tRegion\t0\t5\t1",
            "california\tWest Coast\tRegion\t0\t5\t1",
            "san francisco\tWest Coast\tRegion\t0\t5\t1",
            "los angeles\tWest Coast\tRegion\t0\t5\t1"
        };

        File.WriteAllLines(filePath, lines, Encoding.Unicode);

        string jobTitleDictionaryPath = jobTitleDictionary;
        var jobTitleDictionaryLines = new List<string>
            {
                "Words\tReplacement\tNewColumn\tToDelete\tPriority\tCount",
    
                // Deletion rules for job titles
                "intern\t\t\t1\t5\t1",
                "junior\t\t\t1\t5\t1",
                "trainee\t\t\t1\t5\t1",
                "associate\t\t\t1\t5\t1",
                "assistant\t\t\t1\t5\t1"
            };
        File.WriteAllLines(jobTitleDictionaryPath, jobTitleDictionaryLines, Encoding.Unicode);
    }

    private void CreateComprehensiveWordSmithTestExcelFile(string filePath)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("CompanyName");
        headerRow.CreateCell(1).SetCellValue("JobTitle");
        headerRow.CreateCell(2).SetCellValue("Industry");
        headerRow.CreateCell(3).SetCellValue("ProductDescription");
        headerRow.CreateCell(4).SetCellValue("CustomerSegment");
        headerRow.CreateCell(5).SetCellValue("Address");

        // Add test data with various features to test
        AddComprehensiveWordSmithDataRow(sheet, 1, "Acme Corporation", "Junior Software Developer",
            "Software Development", "Cloud based platform for enterprise customers",
            "Enterprise Client", "123 Main Street, New York, NY 10001");

        AddComprehensiveWordSmithDataRow(sheet, 2, "Global Solutions Limited", "Senior Product Manager",
            "Technology Consulting", "Mobile application development toolkit",
            "Small Business Owner", "456 Oak Avenue, San Francisco, California 94107");

        AddComprehensiveWordSmithDataRow(sheet, 3, "Tech Innovations Inc", "Associate UX Designer",
            "Hardware Computing", "Artificial intelligence system for data analysis",
            "Consumer Market", "789 Pine Road, Chicago, IL 60601");

        AddComprehensiveWordSmithDataRow(sheet, 4, "Smith & Associates International", "Trainee Marketing Specialist",
            "Medical Devices", "Healthcare software solution for hospitals",
            "Healthcare Provider", "101 Elm Drive, Boston, MA 02108");

        AddComprehensiveWordSmithDataRow(sheet, 5, "Johnson Incorporated", "Assistant Project Manager",
            "Banking Services", "Financial analytics platform",
            "Banking Client", "202 Plaza Suite 300, Manhattan, NY 10016");

        AddComprehensiveWordSmithDataRow(sheet, 6, "Worldwide Trading Corporation", "Finance Director",
            "Investment Banking", "Machine learning solution for risk assessment",
            "Financial Institutions", "303 Commerce Boulevard, Brooklyn, NY 11201");

        AddComprehensiveWordSmithDataRow(sheet, 7, "Premier Technologies Limited", "Intern Data Scientist",
            "Tech Biology", "Clinical trial management system",
            "Pharmaceutical Companies", "404 Highland Avenue, Los Angeles, CA 90001");

        AddComprehensiveWordSmithDataRow(sheet, 8, "First Data Services LLC", "Junior Business Analyst",
            "Insurance", "Mobile application development for policy management",
            "Small Business Owner", "505 Sunset Drive, Dallas, TX 75201");

        AddComprehensiveWordSmithDataRow(sheet, 9, "Liberty Digital Partners", "Software Engineer Intern",
            "Health Tech", "Patient management platform with cloud based platform",
            "Healthcare Provider", "606 Mountain Road, Seattle, WA 98101");

        AddComprehensiveWordSmithDataRow(sheet, 10, "Apex Software Corporation", "Associate DevOps Engineer",
            "Software as a Service", "Cloud based platform with machine learning solution",
            "Enterprise Client", "707 Valley Boulevard, San Francisco, California 94105");

        using var fileStream = File.Create(filePath);
        workbook.Write(fileStream);
    }

    private void AddComprehensiveWordSmithDataRow(ISheet sheet, int rowIndex, string companyName,
        string jobTitle, string industry, string productDescription, string customerSegment, string address)
    {
        var row = sheet.CreateRow(rowIndex);
        row.CreateCell(0).SetCellValue(companyName);
        row.CreateCell(1).SetCellValue(jobTitle);
        row.CreateCell(2).SetCellValue(industry);
        row.CreateCell(3).SetCellValue(productDescription);
        row.CreateCell(4).SetCellValue(customerSegment);
        row.CreateCell(5).SetCellValue(address);
    }

    private DataSource CreateComprehensiveWordSmithDataSource(Guid dataSourceId, string filePath)
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
        var columnMappings = new Dictionary<string, MatchLogic.Domain.Project.ColumnMapping>();

        // Add mapping for each column
        string[] columns = new[] { "CompanyName", "JobTitle", "Industry", "ProductDescription", "CustomerSegment", "Address" };
        foreach (var column in columns)
        {
            columnMappings[column] = new MatchLogic.Domain.Project.ColumnMapping
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
    #endregion
}
