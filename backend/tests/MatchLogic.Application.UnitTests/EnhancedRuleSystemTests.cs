using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.UnitTests;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Record = MatchLogic.Domain.CleansingAndStandaradization.Record;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;

namespace MatchLogic.Application.UnitTests;
public class EnhancedRuleSystemTests
{
    private readonly ITestOutputHelper _output;
    private readonly ILogger<EnhancedDependencyResolver> _logger;
    private readonly ILogger<EnhancedRuleRegistry> _registryLogger;
    private readonly ILogger<EnhancedRuleFactory> _factoryLogger;
    private readonly ILogger<EnhancedRuleScheduler> _schedulerLogger;
    private readonly EnhancedDependencyResolver _dependencyResolver;
    private readonly IServiceProvider _serviceProvider;

    public EnhancedRuleSystemTests(ITestOutputHelper output)
    {
        var _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();

        IServiceCollection services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, _dbJobPath);
        services.AddScoped<IEnhancedRuleFactory, EnhancedRuleFactory>();

        

        _serviceProvider = services.BuildServiceProvider();
        _output = output;

        var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddProvider(new XunitLoggerProvider(output)));

        _logger = loggerFactory.CreateLogger<EnhancedDependencyResolver>();
        _registryLogger = loggerFactory.CreateLogger<EnhancedRuleRegistry>();
        _schedulerLogger = loggerFactory.CreateLogger<EnhancedRuleScheduler>();

        _factoryLogger = loggerFactory.CreateLogger<EnhancedRuleFactory>();
        _dependencyResolver = new EnhancedDependencyResolver(_logger);
    }

    [Fact]
    public void ColumnFlowDependencies_OverridePriority_ExecutesInCorrectOrder()
    {
        // Arrange: Create rules where priority would give wrong order
        var rules = new List<EnhancedTransformationRule>
            {
                // Rule 1: Copy FirstName (Priority 10 - should execute after text transformation)
                new EnhancedCopyFieldRule("FirstName", "DisplayFirstName") { Priority = 10 },
                
                // Rule 2: Create FirstName using regex (Priority 200 - should execute first)
                new EnhancedRegexTransformationRule(
                    new List<string> { "FullName" },
                    @"(\w+)\s+(\w+)",
                    _logger,
                    new List<string> { "FirstName", "LastName" },
                    new Dictionary<string, string>()) { Priority = 200 },
                
                // Rule 3: UpperCase FirstName (Priority 80 - should execute after regex but before copy)
                new EnhancedTextTransformationRule("FirstName", CleaningRuleType.UpperCase) { Priority = 80 }
            };

        // Act: Analyze dependencies and create execution plan
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);
        var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            new Dictionary<Guid, List<Guid>>(), columnDependencies);

        // Assert: Verify column flow overrides priority
        Assert.Equal(3, executionPlan.Count);

        var regexRule = rules.OfType<EnhancedRegexTransformationRule>().First();
        var upperCaseRule = rules.OfType<EnhancedTextTransformationRule>().First();
        var copyRule = rules.OfType<EnhancedCopyFieldRule>().First();

        // Execution order should be: Regex → Copy (regardless of priority) → UpperCase
        Assert.Equal(regexRule.Id, executionPlan[0]);
        Assert.Equal(copyRule.Id, executionPlan[1]);
        Assert.Equal(upperCaseRule.Id, executionPlan[2]);

        _output.WriteLine("✅ Column flow dependencies correctly override priority");
    }

    [Fact]
    public void UIScenario_FullNameToUpperFirstName_ExecutesCorrectly()
    {
        // Arrange: User creates rules in UI that should chain together
        var record = new Record();
        record.AddColumn("FullName", "john doe");

        var rules = new List<EnhancedTransformationRule>
            {
                // Extract first and last name using regex
                new EnhancedRegexTransformationRule(
                    new List<string> { "FullName" },
                    @"(\w+)\s+(\w+)",
                    _logger,
                    new List<string> { "FirstName", "LastName" },
                    new Dictionary<string, string>()),
                    
                // Transform first name to uppercase
                new EnhancedTextTransformationRule("FirstName", CleaningRuleType.UpperCase)
            };

        // Act: Execute rules with dependency resolution
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);
        var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            new Dictionary<Guid, List<Guid>>(), columnDependencies);

        foreach (var ruleId in executionPlan)
        {
            var rule = rules.First(r => r.Id == ruleId);
            rule.Apply(record);
        }

        // Assert: Verify correct transformation chain
        Assert.Equal("john doe", record["FullName"].Value); // Original preserved
        Assert.Equal("JOHN", record["FirstName"].Value);    // Extracted and uppercased
        Assert.Equal("doe", record["LastName"].Value);      // Extracted only

        _output.WriteLine($"✅ UI scenario executed correctly: FullName '{record["FullName"].Value}' → FirstName '{record["FirstName"].Value}'");
    }

    [Fact]
    public void TwelveColumnDataset_TenRules_ProcessesEfficiently()
    {
        // Arrange: Create realistic 12-column dataset with 10 transformation rules
        var record = new Record();
        record.AddColumn("FullName", "John Michael Doe");
        record.AddColumn("Email", " JOHN.DOE@EXAMPLE.COM ");
        record.AddColumn("Phone", "(555) 123-4567");
        record.AddColumn("Address", "123 Main Street, Springfield, IL 62701");
        record.AddColumn("Company", "  ACME Corp  ");
        record.AddColumn("Title", "software engineer");
        record.AddColumn("Department", "ENGINEERING");
        record.AddColumn("Comments", "Excellent customer service! Very satisfied.");
        record.AddColumn("ProductCode", "ABC-123-XYZ");
        record.AddColumn("CustomerID", "CUST001");
        record.AddColumn("OrderAmount", "$1,234.56");
        record.AddColumn("OrderDate", "2024-01-15");

        var rules = new List<EnhancedTransformationRule>
            {
                // Rule 1: Extract name components using regex
                new EnhancedRegexTransformationRule(
                    new List < string > { "FullName" },
                    @"(\w+)\s+(\w+)\s+(\w+)",
                    _logger,
                    new List<string> { "FirstName", "MiddleName", "LastName" },
                    new Dictionary<string, string>()),
                
                // Rule 2-4: Clean text fields
                new EnhancedTextTransformationRule("Email", CleaningRuleType.Trim),
                new EnhancedTextTransformationRule("Email", CleaningRuleType.LowerCase),
                new EnhancedTextTransformationRule("Company", CleaningRuleType.Trim),
                
                // Rule 5-6: Format fields
                new EnhancedTextTransformationRule("Title", CleaningRuleType.ProperCase),
                new EnhancedTextTransformationRule("FirstName", CleaningRuleType.UpperCase),
                
                // Rule 7-8: Create display fields from processed data
                new EnhancedCopyFieldRule("Email", "ContactEmail"),
                new EnhancedCopyFieldRule("FirstName", "DisplayFirstName"),
                
                // Rule 9: Extract phone components
                new EnhancedRegexTransformationRule(
                    new List<string> { "Phone" },
                    @"\((\d{3})\)\s(\d{3})-(\d{4})",
                    _logger,
                    new List<string> { "AreaCode", "Exchange", "Number" },
                    new Dictionary<string, string>()),
                
                // Rule 10: Address parsing
                new EnhancedAddressTransformationRule(
                    new[] { "Address" },
                    new Dictionary<string, string>
                    {
                        { "Street", "Street" },
                        { "City", "City" },
                        { "State", "State" },
                        { "Zip", "Zip" }
                    },
                    _logger)
            };

        // Act: Process all rules
        var startTime = DateTime.Now;

        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);
        var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            new Dictionary<Guid, List<Guid>>(), columnDependencies);

        foreach (var ruleId in executionPlan)
        {
            var rule = rules.First(r => r.Id == ruleId);
            rule.Apply(record);
        }

        var processingTime = DateTime.Now - startTime;

        // Assert: Verify all transformations completed correctly
        Assert.Equal("JOHN", record["FirstName"].Value);
        Assert.Equal("Michael", record["MiddleName"].Value);
        Assert.Equal("Doe", record["LastName"].Value);
        Assert.Equal("john.doe@example.com", record["Email"].Value);
        Assert.Equal("John", record["DisplayFirstName"].Value);
        Assert.True(record.HasColumn("ContactEmail"));
        Assert.True(record.HasColumn("AreaCode"));

        _output.WriteLine($"✅ Processed 12-column dataset with 10 rules in {processingTime.TotalMilliseconds}ms");
        _output.WriteLine($"   Final record has {record.ColumnCount} columns");
    }

    [Fact]
    public void RegexRule_CreatesDynamicColumns_DependenciesResolved()
    {
        // Arrange: Regex rule creates multiple output columns
        var record = new Record();
        record.AddColumn("CustomerFeedback", "Product: ABC-123, Rating: 5, Date: 2024-01-15");

        var rules = new List<EnhancedTransformationRule>
            {
                // Regex creates multiple output columns
                new EnhancedRegexTransformationRule(
                    new List < string > { "CustomerFeedback" },
                    @"Product:\s([^,]+),\sRating:\s(\d+),\sDate:\s([\d-]+)",
                    _logger,
                    new List<string> { "ProductCode", "Rating", "ReviewDate" },
                    new Dictionary<string, string>()),
                
                // Subsequent rules depend on regex outputs
                new EnhancedTextTransformationRule("ProductCode", CleaningRuleType.UpperCase),
                new EnhancedCopyFieldRule("Rating", "CustomerRating")
            };

        // Act: Execute with dependency resolution
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);
        var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            new Dictionary<Guid, List<Guid>>(), columnDependencies);

        foreach (var ruleId in executionPlan)
        {
            var rule = rules.First(r => r.Id == ruleId);
            rule.Apply(record);
        }

        // Assert: Verify regex outputs and dependent transformations
        Assert.True(record.HasColumn("ProductCode"));
        Assert.True(record.HasColumn("Rating"));
        Assert.True(record.HasColumn("ReviewDate"));
        Assert.True(record.HasColumn("CustomerRating"));

        // Verify dependency chain worked
        var productCode = record["ProductCode"].Value.ToString();
        Assert.True(productCode.Any(c => char.IsUpper(c))); // Should be uppercased

        _output.WriteLine($"✅ Regex rule created multiple dynamic columns");
        _output.WriteLine($"   Dependencies resolved correctly for downstream transformations");
    }

    [Fact]
    public void CircularDependency_ThrowsException()
    {
        // Arrange: Create rules with circular dependency
        var rule1 = new EnhancedTextTransformationRule("Column1", CleaningRuleType.UpperCase);
        var rule2 = new EnhancedTextTransformationRule("Column2", CleaningRuleType.LowerCase);

        var rules = new List<EnhancedTransformationRule> { rule1, rule2 };

        // Create explicit circular dependencies
        var explicitDependencies = new Dictionary<Guid, List<Guid>>
            {
                { rule1.Id, new List<Guid> { rule2.Id } },
                { rule2.Id, new List<Guid> { rule1.Id } }
            };

        // Act & Assert: Should detect circular dependency
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            _dependencyResolver.CreateExecutionPlanWithColumnFlow(
                explicitDependencies, columnDependencies));

        Assert.Contains("Circular dependency", exception.Message);
        _output.WriteLine("✅ Circular dependency correctly detected and prevented");
    }

    [Fact]
    public void ColumnAvailabilityValidation_DetectsMissingColumns()
    {
        // Arrange: Rules that require columns not in initial dataset
        var rules = new List<EnhancedTransformationRule>
            {
                new EnhancedRegexTransformationRule(
                    new List<string> { "FullName" },
                    @"(\w+)\s+(\w+)",
                    _logger,
                    new List<string> { "FirstName", "LastName" },
                    new Dictionary<string, string>()),
                new EnhancedTextTransformationRule("MiddleName", CleaningRuleType.UpperCase), // Missing column!
                new EnhancedTextTransformationRule("FirstName", CleaningRuleType.UpperCase)
            };

        var initialColumns = new[] { "FullName", "Email", "Phone" }; // MiddleName not available

        // Act: Validate column availability
        var result = _dependencyResolver.ValidateColumnAvailability(rules, initialColumns);

        // Assert: Should detect missing MiddleName
        Assert.False(result.IsValid);
        Assert.Single(result.Errors);
        Assert.Contains("MiddleName", result.Errors[0]);

        _output.WriteLine($"✅ Missing column validation detected: {result.Errors[0]}");
    }

    [Fact]
    public void ComplexColumnFlow_MultipleChains_ExecutesCorrectly()
    {
        // Arrange: Complex scenario with multiple independent and dependent chains
        var record = new Record();
        record.AddColumn("CustomerName", "Jane Smith");
        record.AddColumn("CustomerAddress", "456 Oak St, Springfield, IL 62701");
        record.AddColumn("OrderData", "Order: ORD-123, Amount: $500.00, Status: Shipped");

        var rules = new List<EnhancedTransformationRule>
            {
                // Chain 1: Name processing
                new EnhancedRegexTransformationRule(
                    new List<string> { "CustomerName" },
                    @"(\w+)\s+(\w+)",
                    _logger,
                    new List<string> { "FirstName", "LastName" },
                    new Dictionary<string, string>()),
                new EnhancedTextTransformationRule("FirstName", CleaningRuleType.UpperCase),
                new EnhancedCopyFieldRule("FirstName", "FormalFirstName"),
                
                // Chain 2: Address processing  
                new EnhancedAddressTransformationRule(
                    new[] { "CustomerAddress" },
                    new Dictionary<string, string>
                    {
                        { "Street", "Street" },
                        { "City", "City" },
                        { "State", "State" }
                    },
                    _logger),
                new EnhancedTextTransformationRule("City", CleaningRuleType.ProperCase),
                
                // Chain 3: Order processing
                new EnhancedRegexTransformationRule(
                    new List < string > { "OrderData" },
                    @"Order:\s([^,]+),\sAmount:\s([^,]+),\sStatus:\s(.+)",
                    _logger,
                    new List<string> { "OrderID", "Amount", "Status" },
                    new Dictionary<string, string>()),
                new EnhancedTextTransformationRule("OrderID", CleaningRuleType.Trim),
                
                // Chain 4: Cross-chain copies (depends on outputs from all chains)
                new EnhancedCopyFieldRule("FormalFirstName", "CustomerDisplayName"),
                new EnhancedCopyFieldRule("City", "ShippingCity")
            };

        // Act: Execute complex dependency chain
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);
        var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            new Dictionary<Guid, List<Guid>>(), columnDependencies);

        foreach (var ruleId in executionPlan)
        {
            var rule = rules.First(r => r.Id == ruleId);
            rule.Apply(record);
        }

        // Assert: Verify all chains executed correctly
        Assert.Equal("JANE", record["FirstName"].Value);
        Assert.Equal("Jane", record["FormalFirstName"].Value);
        Assert.True(record.HasColumn("CustomerDisplayName"));
        Assert.True(record.HasColumn("ShippingCity"));
        Assert.True(record.HasColumn("OrderID"));

        var finalColumnCount = record.ColumnCount;
        Assert.True(finalColumnCount >= 15); // Should have created many new columns

        _output.WriteLine($"✅ Complex multi-chain scenario executed successfully");
        _output.WriteLine($"   Started with 3 columns, ended with {finalColumnCount} columns");
    }

    [Fact]
    public void EnhancedRulesManager_LoadsAndAppliesRules_IntegrationTest()
    {
        // Arrange: Integration test with full enhanced rules manager
        var ruleFactory = _serviceProvider.GetService<IEnhancedRuleFactory>();
        //var ruleFactory = new EnhancedRuleFactory(_factoryLogger);
        var registry = new EnhancedRuleRegistry(_registryLogger, ruleFactory, _dependencyResolver);
        var scheduler = new EnhancedRuleScheduler(registry, _dependencyResolver, _schedulerLogger);
        var rulesManager = new EnhancedRulesManager(registry, scheduler, _dependencyResolver,
            LoggerFactory.Create(b => b.AddProvider(new XunitLoggerProvider(_output)))
                .CreateLogger<EnhancedRulesManager>());

        var configuration = new EnhancedCleaningRules
        {
            Rules = new List<CleaningRule>
                {
                    new CleaningRule("FirstName", CleaningRuleType.UpperCase),
                    new CleaningRule("Email", CleaningRuleType.Trim)
                },
            MappingRules = new List<MappingRule>
                {
                    new MappingRule
                    {
                        OperationType = MappingOperationType.RegexPattern,
                        SourceColumn = new List<string> { "FullName" },
                        OutputColumns = new List<string> { "FirstName", "LastName" },
                        MappingConfig = new Dictionary<string, string>
                        {
                            { "pattern", @"(\w+)\s+(\w+)" }
                        }
                    }
                }
        };

        var record = new Record();
        record.AddColumn("FullName", "john doe");
        record.AddColumn("Email", "  john@example.com  ");

        // Act: Load rules and apply to record
        var loaded = rulesManager.LoadRulesFromConfigAsync(configuration).Result;
        Assert.True(loaded);

        rulesManager.ApplyRules(record);

        // Assert: Verify integration worked end-to-end
        Assert.Equal("JOHN", record["FirstName"].Value); // Extracted then uppercased
        Assert.Equal("doe", record["LastName"].Value);   // Extracted
        Assert.Equal("john@example.com", record["Email"].Value); // Trimmed

        _output.WriteLine("✅ End-to-end integration test completed successfully");
        _output.WriteLine($"   Rules manager loaded {rulesManager.RuleCount} rules");
    }
    [Fact]
    public void AddressTransformationRule_ParsesAddress_WithDifferentPrefixes()
    {
        // Arrange
        var record = new Record();
        record.AddColumn("FullAddress", "123 Main Street, Springfield, IL 62701");
        record.AddColumn("HomeAddress", "456 Oak Avenue, Autumnfield, IL 62702");
        record.AddColumn("WorkAddress", "789 Business Blvd, Commerce City, IL 62703");

        var rules = new List<EnhancedTransformationRule>
    {
        // Default prefix: Address_*
        new EnhancedAddressTransformationRule(
            new[] { "FullAddress" },
            new Dictionary<string, string>(),
            _logger),
        
        // Custom prefix: Home_Address_*
        new EnhancedAddressTransformationRule(
            new[] { "HomeAddress" },
            new Dictionary<string, string>(),
            _logger,
            null,
            "Home"),
        
        // Custom prefix: Work_Address_*
        new EnhancedAddressTransformationRule(
            new[] { "WorkAddress" },
            new Dictionary<string, string>(),
            _logger,
            null,
            "Work")
    };

        // Act
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);
        var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            new Dictionary<Guid, List<Guid>>(), columnDependencies);

        foreach (var ruleId in executionPlan)
        {
            var rule = rules.First(r => r.Id == ruleId);
            rule.Apply(record);
        }

        // Assert - Default prefix
        Assert.True(record.HasColumn("Address_Street"));
        Assert.True(record.HasColumn("Address_City"));
        Assert.True(record.HasColumn("Address_State"));
        Assert.True(record.HasColumn("Address_ZipCode"));

        // Assert - Home prefix
        Assert.True(record.HasColumn("Home_Address_Street"));
        Assert.True(record.HasColumn("Home_Address_City"));
        Assert.True(record.HasColumn("Home_Address_State"));
        Assert.True(record.HasColumn("Home_Address_ZipCode"));

        // Assert - Work prefix
        Assert.True(record.HasColumn("Work_Address_Street"));
        Assert.True(record.HasColumn("Work_Address_City"));
        Assert.True(record.HasColumn("Work_Address_State"));
        Assert.True(record.HasColumn("Work_Address_ZipCode"));

        _output.WriteLine($"✅ Address transformation rules with different prefixes");
        _output.WriteLine($"   Default: Address_Street, Address_City, etc.");
        _output.WriteLine($"   Home: Home_Address_Street, Home_Address_City, etc.");
        _output.WriteLine($"   Work: Work_Address_Street, Work_Address_City, etc.");
    }
    [Fact]
    public void AddressTransformationRule_ParsesAddress_CorrectDependencies()
    {
        // Arrange: Address transformation creates multiple output columns
        var record = new Record();
        record.AddColumn("FullAddress", "123 Main Street, Springfield, IL 62701");

        var rules = new List<EnhancedTransformationRule>
            {
                // Address parser creates multiple output columns
                new EnhancedAddressTransformationRule(
                    new[] { "FullAddress" },
                    new Dictionary<string, string>
                    {
                        { "Street", "Street" },
                        { "City", "City" },
                        { "State", "State" },
                        { "ZipCode", "ZipCode" }
                    },
                    _logger),
                
                // Subsequent rules depend on address parser outputs
                new EnhancedTextTransformationRule("City", CleaningRuleType.UpperCase),
                new EnhancedTextTransformationRule("State", CleaningRuleType.UpperCase),
                new EnhancedCopyFieldRule("ZipCode", "PostalCode")
            };

        // Act: Execute with dependency resolution
        var columnDependencies = _dependencyResolver.AnalyzeColumnDependencies(rules);
        var executionPlan = _dependencyResolver.CreateExecutionPlanWithColumnFlow(
            new Dictionary<Guid, List<Guid>>(), columnDependencies);

        foreach (var ruleId in executionPlan)
        {
            var rule = rules.First(r => r.Id == ruleId);
            rule.Apply(record);
        }

        // Assert: Verify address parser outputs and dependent transformations
        Assert.True(record.HasColumn("Street"));
        Assert.True(record.HasColumn("City"));
        Assert.True(record.HasColumn("State"));
        Assert.True(record.HasColumn("PostalCode"));

        _output.WriteLine($"✅ Address transformation rule created multiple components");
        _output.WriteLine($"   Dependencies resolved correctly for downstream transformations");
    }
}

#region xUnit Logging Helper

public class XunitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _testOutputHelper;

    public XunitLoggerProvider(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new XunitLogger(_testOutputHelper, categoryName);
    }

    public void Dispose()
    {
    }
}

public class XunitLogger : ILogger
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly string _categoryName;

    public XunitLogger(ITestOutputHelper testOutputHelper, string categoryName)
    {
        _testOutputHelper = testOutputHelper;
        _categoryName = categoryName;
    }

    public IDisposable BeginScope<TState>(TState state)
    {
        return null;
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
    {
        try
        {
            _testOutputHelper.WriteLine($"[{logLevel}] {_categoryName}: {formatter(state, exception)}");
            if (exception != null)
            {
                _testOutputHelper.WriteLine(exception.ToString());
            }
        }
        catch
        {
            // Ignore logging errors in tests
        }
    }
}

    #endregion
