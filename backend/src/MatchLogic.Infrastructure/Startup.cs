using MatchLogic.Application.Common;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Phonetics;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.BackgroundJob;
using MatchLogic.Infrastructure.Common;
using MatchLogic.Infrastructure.Comparator;
using MatchLogic.Infrastructure.Core.Telemetry;
using MatchLogic.Infrastructure.Events.Providers;
using MatchLogic.Infrastructure.Import;
using MatchLogic.Infrastructure.Persistence;
using MatchLogic.Infrastructure.Phonetics;
using MatchLogic.Infrastructure.Repository;
using Mapster;
using MassTransit.NewIdProviders;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Features.DataProfiling;
using MatchLogic.Infrastructure.Project.DataProfiling;
using MatchLogic.Application.Interfaces.Regex;
using MatchLogic.Application.Features.Regex;
using MatchLogic.Application.Interfaces.Dictionary;
using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Core;
using MatchLogic.Domain.Regex;
using MatchLogic.Domain.Dictionary;
using MatchLogic.Infrastructure.Project.DataSource;
using MatchLogic.Infrastructure.Project.Commands;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Infrastructure.CleansingAndStandardization;
using MatchLogic.Application.Features.Import;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;
using MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Features.MatchDefinition;
using MatchLogic.Infrastructure.Dictionary;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;
using MatchLogic.Application.Features.DataMatching.Grouping;
using MatchLogic.Application.Features.DataMatching.Orchestration;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Infrastructure.Security;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.Transformation.Parsers;
using System.Threading.Tasks;
using MatchLogic.Data.QualityII;
using MatchLogic.Parsers;
using MatchLogic.Infrastructure.Persistence.MongoDB;
using Microsoft.Extensions.Configuration;
using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Features.DataMatching.Storage;
using MatchLogic.Application.Features.FinalExport;
using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Features.Transform;
using MatchLogic.Infrastructure.Export;
using MatchLogic.Infrastructure.FinalExport;
using MatchLogic.Application.Services;
using MatchLogic.Application.Features.DataMatching.Analytics;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Infrastructure.Configuration;
using MatchLogic.Infrastructure.Scheduling;
using Hangfire;
using Hangfire.Mongo;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
// Licensing layer removed during saas-extract — SaaS uses quota-based limits, not licenses.

namespace MatchLogic.Infrastructure;

public static class ApplicationSetup
{
    public static IServiceCollection AddApplicationSetup(
        this IServiceCollection services,
        string paramDbPath = "",
        string paramJobDbPath = "",
        IConfiguration configuration = null)
    {

        // ================================================================
        // GET OPERATION MODE FROM CONFIGURATION
        // ================================================================
        var operationConfig = configuration?.GetSection("Application:Operation")
            .Get<ApplicationOperationConfig>() ?? new ApplicationOperationConfig();

        Console.WriteLine($"🚀 Starting application in {operationConfig.Mode} mode");

        // ================================================================
        // DATA STORE SETUP
        // ================================================================
        // Step 1 — Register StoreSettings + IStoreTypeResolver FIRST,
        //            before any repository or store factory is wired up.
        RegisterStoreSettings(services, configuration);

        // Step 2 — Fail fast at startup if appsettings contains bad StoreType values.
        ValidateStoreSettings(configuration);
        // ================================================================
        // OPTION 2: Keep LiteDB (for reference/fallback)
        // Uncomment below and comment out AddMongoDbDataStore above
        // ================================================================
        bool usingLiteDb = (!string.IsNullOrWhiteSpace(paramDbPath)
                    && !string.IsNullOrWhiteSpace(paramJobDbPath))
                   ||
                   (configuration != null
                    && string.Equals(
                        configuration["StoreSettings:Default"],
                        "LiteDb",
                        StringComparison.OrdinalIgnoreCase));

        if (usingLiteDb)
        {
            // Resolve paths — code params take priority, fall back to defaults
            var dbPath = !string.IsNullOrWhiteSpace(paramDbPath)
                ? paramDbPath
                : Path.Combine(
                    Application.Common.StoragePaths.DefaultDatabasePath, "MatchLogic.db");

            var jobDbPath = !string.IsNullOrWhiteSpace(paramJobDbPath)
                ? paramJobDbPath
                : Path.Combine(
                    Application.Common.StoragePaths.DefaultDatabasePath, "JobProgress.db");

            AddLiteDbStoreFactory(services, dbPath, jobDbPath);
        }
        else
        {
            services.AddMongoDbDataStore(configuration);
        }

        // ================================================================
        // CORE SERVICES (Always registered - needed by both modes)
        // ================================================================
        services.AddScoped<ICommandFactory, CommandFactory>();
        services.AddSingleton<ITransliterator, UnidecodeTransliterator>();
        services.AddSingleton<IPhoneticEncoder, PhonixEncoder>();
        services.AddSingleton<IPhoneticConverter, PhoneticConverter>();
        services.AddSingleton<IStringSimilarityCalculator, JaroWinklerCalculator>();
        services.AddSingleton<ComparatorConfigFactory, ComparatorConfigFactory>();
        services.AddSingleton<ComparatorStrategyFactory, ComparatorStrategyFactory>();
        services.AddScoped<IComparatorBuilder, ComparatorBuilder>();

        #region Data Import and Reader Services  (Always registered)
        services.AddSingleton<ConnectionConfigFactory, ConnectionConfigFactory>();
        services.AddSingleton<ConnectionReaderStrategyFactory, ConnectionReaderStrategyFactory>();
        services.AddScoped<IConnectionBuilder, ConnectionBuilder>();
        services.AddSingleton<IEncryptionService, EncryptionService>();
        services.AddSingleton<IHeaderUtility, HeaderUtility>();
        services.AddScoped<ISecureParameterHandler, SecureParameterHandler>();
        #endregion

        #region Remote Storage & OAuth Services
        services.AddSingleton<RemoteFileConnectorFactory>();
        services.AddSingleton<ITempFileManager, TempFileManager>();
        services.AddScoped<IOAuthTokenService, OAuthTokenService>();
        #endregion

        // ================================================================
        // SHARED MATCHING SERVICES (Used by both Batch and Live Search)
        // ================================================================
        AddSharedMatchingServices(services);

        // ================================================================
        // CONDITIONAL REGISTRATION BASED ON OPERATION MODE
        // ================================================================
        switch (operationConfig.Mode)
        {
            case OperationMode.Batch:
                Console.WriteLine("📦 Registering BATCH MODE services");
                AddBatchServices(services, configuration);
                AddLiveSearchServices(services, configuration);
                break;

            case OperationMode.LiveSearch:
                Console.WriteLine("🔍 Registering LIVE SEARCH MODE services");
                AddBatchServices(services, configuration);
                AddLiveSearchServices(services, configuration);
                break;

            default:
                throw new InvalidOperationException($"Unknown operation mode: {operationConfig.Mode}");
        }


        // ================================================================
        // COMMON INFRASTRUCTURE (Always registered)
        // ================================================================
        NewId.SetProcessIdProvider(new CurrentProcessIdProvider());

        services.AddRepositories();
        AddEventBus(services);
        AddInfrastructureServices(services);
        AddDataProfiling(services);
        AddDataCleansingServices(services);
        AddMatchConfiguration(services);
        AddWordSmithServices(services);
        AddSchedulingServices(services, configuration, usingLiteDb);
        ProperCaseOptions(services).Wait();
        Console.WriteLine($"Application setup completed for {operationConfig.Mode} mode");
        //AddFieldOverwriting(services);
        //AddFinalExport(services);

        // AddLicensingServices() removed — SaaS uses IQuotaService (M4) instead.
        return services;
    }

    private static void RegisterStoreSettings(
       IServiceCollection services,
       IConfiguration configuration)
    {
        // Bind StoreSettings from config; if section is absent, defaults kick in
        // (Default = "MongoDB", Overrides = empty) so everything still works.
        if (configuration != null)
        {
            services.Configure<StoreSettings>(
                configuration.GetSection(StoreSettings.SectionName));
        }
        else
        {
            // No configuration at all (e.g. unit tests wiring up manually) —
            // register defaults so the resolver never throws.
            services.Configure<StoreSettings>(_ => { });
        }

        // Singleton: results are cached per-type after first resolution.
        services.AddSingleton<IStoreTypeResolver, StoreTypeResolver>();
    }

    // NEW — Fail-fast validation: catches typos in appsettings at startup,
    //          not silently at the first repository resolution.
    private static void ValidateStoreSettings(IConfiguration configuration)
    {
        if (configuration == null) return;

        var section = configuration.GetSection(StoreSettings.SectionName);
        if (!section.Exists()) return;

        var settings = section.Get<StoreSettings>();
        if (settings == null) return;

        var validNames = Enum.GetNames<StoreType>();

        // Collect ALL values (Default + every Override) and validate each one
        var allEntries = new List<(string Label, string Value)>
        {
            ("StoreSettings:Default", settings.Default)
        };

        foreach (var kvp in settings.Overrides)
            allEntries.Add(($"StoreSettings:Overrides:{kvp.Key}", kvp.Value));

        foreach (var (label, value) in allEntries)
        {
            if (string.IsNullOrWhiteSpace(value)) continue;

            if (!Enum.TryParse<StoreType>(value, ignoreCase: true, out _))
            {
                throw new InvalidOperationException(
                    $"Invalid StoreType '{value}' found at '{label}' in appsettings. " +
                    $"Valid values are: {string.Join(", ", validNames)}");
            }
        }
    }

    #region Shared Services (Used by Both Modes)

    /// <summary>
    /// Services shared between Batch and Live Search modes
    /// </summary>
    private static IServiceCollection AddSharedMatchingServices(IServiceCollection services)
    {
        // Comparison services - used by both batch and live search
        services.AddScoped<IEnhancedRecordComparisonService, EnhancedRecordComparisonServiceDME>();
        services.AddScoped<IDataSourceIndexMapper, DataSourceIndexMapper>();

        // Record comparison support
        services.AddScoped<IRecordComparisonService, RecordComparisonService>();
        services.AddScoped<IFinalScoresService, FinalScoresService>();
        services.AddTransient<ITelemetry, RecordLinkageTelemetry>();
        services.AddTransient<IRecordHasher, SHA256RecordHasher>();

        return services;
    }

    #endregion

    #region Batch Mode Services

    /// <summary>
    /// Services specific to Batch processing mode
    /// Only registered when Mode = Batch
    /// </summary>
    private static IServiceCollection AddBatchServices(
        IServiceCollection services,
        IConfiguration configuration)
    {
        Console.WriteLine("  → Registering batch indexer (OptimizedQGramIndexer)");
        services.AddScoped<IQGramIndexer>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<OptimizedQGramIndexer>>();
            return new OptimizedQGramIndexer(3, logger);
        });

        Console.WriteLine("  → Registering batch production indexer (ProductionQGramIndexerDME)");
        services.AddScoped<IProductionQGramIndexer, ProductionQGramIndexerDME>();

        Console.WriteLine("  → Registering batch orchestration services");
        services.Configure<RecordLinkageOptions>(options =>
        {
            options.MaxDegreeOfParallelism = Environment.ProcessorCount;
        });
        services.Configure<ProbabilisticOption>(options =>
        {
            options.MaxDegreeOfParallelism = Environment.ProcessorCount;
            options.DecimalPlaces = 3;
        });

        services.AddScoped<IBlockingStrategy, ParallelBlockingStrategy>();
        services.AddScoped<IRecordMatcher, ParallelRecordMatcher>();
        services.AddScoped<SimpleRecordPairer, SimpleRecordPairer>();
        services.AddSingleton<MatchGroupingServiceFactory, MatchGroupingServiceFactory>();
        services.AddScoped<ParallelEM>();

        Console.WriteLine("  → Registering batch grouping services");
        services.AddScoped<IEnhancedGroupingService, EnhancedGroupingServiceDME>();

        Console.WriteLine("  → Registering batch orchestrator");
        services.AddScoped<IRecordLinkageOrchestrator, RecordLinkageOrchestratorDME>();

        Console.WriteLine("  → Registering batch storage services");
        services.AddScoped<IMatchGraphStorage, MatchGraphStorage>();
        services.AddSingleton<MatchGraphStorage>();

        services.AddScoped<MatchQualityAnalysisDME>();

        Console.WriteLine("  → Registering batch commands");
        services.AddTransient<DataImportCommand>();
        services.AddScoped<IDataSourceService, DataSourceService>();
        services.AddScoped<IProjectService, ProjectService>();
        services.AddScoped<IFileImportService, FileImportService>();
        services.AddScoped<DataProfilingCommand>();
        services.AddScoped<AdvanceDataProfilingCommand>();
        services.AddScoped<ProbabilisticRecordLinkage>();
        services.AddScoped<IColumnFilter, ColumnFilter>();
        services.AddScoped<ISchemaValidationService, SchemaValidationService>();
        services.AddScoped<MatchingCommand>();

        // Transformation Services
        services.AddScoped<IDataTransformerFactory, DataTransformerFactory>();
        services.AddScoped<IExportDataWriterStrategyFactory, ExportDataWriterStrategyFactory>();

        Console.WriteLine("  → Registering Master Record Determination");
        AddMasterRecordDetermination(services);

        Console.WriteLine("  → Registering Field Overwriting");
        AddFieldOverwriting(services);

        Console.WriteLine("  → Registering Final Export");
        AddFinalExport(services);

        return services;
    }

    #endregion

    #region Live Search Mode Services

    /// <summary>
    /// Services specific to Live Search mode
    /// Only registered when Mode = LiveSearch
    /// </summary>
    private static IServiceCollection AddLiveSearchServices(
        IServiceCollection services,
        IConfiguration configuration)
    {
        // Delegate to the dedicated Live Search setup extension
        services.AddLiveSearchServices(configuration);
        return services;
    }

    #endregion
    private static IServiceCollection AddEventBus(this IServiceCollection services)
    {
        services.AddScoped<IEventBus, MediatREventBus>();
        return services;
    }

    private static IServiceCollection AddInfrastructureServices(
            this IServiceCollection services)
    {       

         services.AddSingleton<IJobCancellationRegistry, JobCancellationRegistry>();
              // Cleanup stale InProgress jobs from previous runs BEFORE starting processing
        services.AddHostedService<StaleJobCleanupService>();
      
        services.AddScoped<IJobEventPublisher, JobEventPublisher>();
        //services.AddSingleton<IParameterStrategyFactory, ParameterStrategyFactory>();
        services.AddScoped<IRecordMatchingFacade, RecordMatchingFacadeWithGrouping>();

        services.AddScoped<ISchedulerService, SchedulerService>();
        services.AddHostedService<ScheduleRecoveryService>();

        return services;
    }

    public static IServiceCollection AddRepositories(this IServiceCollection services)
    {
        // Register the generic repository first
        services.AddScoped(typeof(IGenericRepository<,>), typeof(GenericRepository<,>));

        // Find all repository implementations
        var repositoryTypes = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(s => s.GetTypes())
            .Where(t =>
                t.IsClass &&
                !t.IsAbstract &&
                t.GetInterfaces().Any(i =>
                    i.IsGenericType &&
                    i.GetGenericTypeDefinition() == typeof(IGenericRepository<,>)));

        foreach (var repositoryType in repositoryTypes)
        {
            var repositoryInterfaces = repositoryType.GetInterfaces()
                .Where(i =>
                    !i.IsGenericType && // Only non-generic specific interfaces
                    i.GetInterfaces() // Get interfaces that this interface implements
                        .Any(baseInterface =>
                            baseInterface.IsGenericType &&
                            baseInterface.GetGenericTypeDefinition() == typeof(IGenericRepository<,>)));

            foreach (var repositoryInterface in repositoryInterfaces)
            {
                services.AddScoped(repositoryInterface, repositoryType);
            }
        }

        return services;
    }

    /// <summary>
    /// Adds transformation services to the service collection
    /// </summary>
    public static IServiceCollection AddTransformationServices(this IServiceCollection services)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        // Register core transformation services        
        services.AddScoped<IEnhancedRuleFactory, EnhancedRuleFactory>();
        services.AddScoped<IEnhancedRuleRegistry, EnhancedRuleRegistry>();
        services.AddScoped<IEnhancedDependencyResolver, EnhancedDependencyResolver>();
        services.AddScoped<IEnhancedRuleScheduler, EnhancedRuleScheduler>();
        services.AddScoped<IRulesManager<EnhancedTransformationRule>, EnhancedRulesManager>();
        services.AddScoped<ITransformationContext, TransformationContext>();
        services.AddSingleton<PredefinedStringTypes>(sp =>
        {
            var types = new PredefinedStringTypes();
            types.LoadNameDictionaries(); // Load once here
            return types;
        });

        services.AddSingleton<FirstNameParser>();
        services.AddSingleton<FullNameParserOptimized>();
        services.AddSingleton<AbbreviationParser>();
        //services.AddScoped<WordSmithRuleBuilder>();

        return services;
    }

    /// <summary>
    /// Preloads dictionaries for first name parser (call this during application startup)
    /// </summary>
    public static async Task PreloadFirstNameDictionariesAsync(this IServiceProvider serviceProvider)
    {
        var parser = serviceProvider.GetRequiredService<FirstNameParser>();
        await parser.LoadDictionariesAsync();

    }

    public static async Task ProperCaseOptions(this IServiceCollection services)
    {
        // Register dependencies first
        services.AddSingleton<ProperCaseOptions>(sp =>
        {
            // This will be overridden later
            return new ProperCaseOptions();
        });

        // Build temp provider so we can resolve dependencies
        using var provider = services.BuildServiceProvider();

        var genericRepository = provider.GetRequiredService<IDataStore>();

        var options = await genericRepository.GetAllAsync<ProperCaseOptions>(Constants.Collections.ProperCaseOptions);

        if (options == null || options.Count == 0)
        {
            var addOptions = new ProperCaseOptions
            {
                Delimiters = " ,;.",
                IgnoreCaseOnExceptions = false,
                Exceptions = new List<string>(),
                ActionOnException = ActionOnException.LeaveCaseAsItIs
            };

            await genericRepository.InsertAsync(addOptions, Constants.Collections.ProperCaseOptions);

            options = new List<ProperCaseOptions> { addOptions };
        }

        // Re-register singleton with correct value
        services.AddSingleton(options.First());
    }

    /// <summary>
    /// Adds the specific data cleansing services
    /// </summary>
    public static IServiceCollection AddDataCleansingServices(this IServiceCollection services, int maxDegreeOfParallelism = 4)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        // Add transformation services
        services.AddTransformationServices();

        services.AddScoped<ICleansingModule, DataCleansingModule>();
        services.AddScoped<DataCleansingCommand>();
     


        return services;
    }

    public static IServiceCollection AddDataProfiling(this IServiceCollection services)
    {
        // Register profiling options
        services.Configure<ProfilingOptions>(options => {
            options.BatchSize = 5000;
            options.MaxDegreeOfParallelism = Environment.ProcessorCount;
            options.BufferSize = 10000;
            options.SampleSize = 100;
            options.MaxRowsPerCategory = 50;
            options.MaxDistinctValuesToTrack = 100;
            options.StoreCompleteRows = true;
        });

      // Register comprehensive data profiler
        services.AddScoped<IDataProfiler, DataProfiler>();
        services.AddScoped<IRegexInfoService, RegexInfoService>();
        services.AddScoped<IDictionaryCategoryService, DictionaryCategoryService>();

        // Register concrete seed providers
        services.AddScoped<IDataSeedProvider<DictionaryCategory>, DictionaryCategorySeedProvider>();
        services.AddScoped<IDataSeedProvider<RegexInfo>, RegexSeedProvider>();

        // Register adapters for MigrationService
        services.AddScoped<IDataSeedProvider<IEntity>>(sp =>
            new DataSeedProviderAdapter<DictionaryCategory>(
                sp.GetRequiredService<IDataSeedProvider<DictionaryCategory>>()));

        services.AddScoped<IDataSeedProvider<IEntity>>(sp =>
            new DataSeedProviderAdapter<RegexInfo>(
                sp.GetRequiredService<IDataSeedProvider<RegexInfo>>()));

        // Register MigrationService
        services.AddScoped<IMigrationService, MigrationService>();

        services.AddScoped<IProfileRepository, ProfileRepository>();
        services.AddScoped<IProfileService, ProfileService>();

        // Register profiling options
        services.Configure<AdvancedProfilingOptions>(options => {
            options.BatchSize = 5000;
            options.MaxDegreeOfParallelism = Environment.ProcessorCount;
            options.BufferSize = 10000;
            options.SampleSize = 100;
            options.MaxRowsPerCategory = 100;
            options.MaxDistinctValuesToTrack = 100;
            options.StoreCompleteRows = true;
        });

        services.AddScoped<IAdvancedDataProfiler, AdvancedDataProfiler>();        
        services.AddScoped<ITypeDetectionService, TypeDetectionService>();        
        services.AddScoped<IDataQualityService, DataQualityService>();        
        services.AddScoped<IOutlierDetectionService, OutlierDetectionService>();        
        services.AddScoped<IPatternDiscoveryService, PatternDiscoveryService>();        
        services.AddScoped<IValidationRuleService, ValidationService>();        
        services.AddScoped<IClusteringService, ClusteringService>();        

        return services;
    }

    private static IServiceCollection AddMatchConfiguration(this  IServiceCollection services)
    {
        services.AddScoped<IMatchConfigurationService, MatchConfigurationService>();
        services.AddScoped<IAutoMappingService, AutoMappingService>();
        services.AddScoped<IFieldMappingService, FieldMappingService>();
        return services;
    }

    public static IServiceCollection AddWordSmithServices(this IServiceCollection services)
    {
        // Register WordSmith dictionary services
        services.AddScoped<IWordSmithDictionaryService, WordSmithDictionaryService>();
        services.AddScoped<WordSmithDictionaryLoader>();

        return services;
    }

    //private static IServiceCollection AddMatching(this IServiceCollection services)
    //{
    //    services.AddScoped<IDataSourceIndexMapper, DataSourceIndexMapper>();
    //    services.AddScoped<IProductionQGramIndexer, ProductionQGramIndexerDME>();
    //    //services.AddScoped<IProductionQGramIndexer, OptimizedProductionQGramIndexer>();
    //    services.AddScoped<IEnhancedRecordComparisonService, EnhancedRecordComparisonServiceDME>();
    //    services.AddScoped<IEnhancedGroupingService, EnhancedGroupingServiceDME>();
    //    services.AddScoped<IRecordLinkageOrchestrator, RecordLinkageOrchestratorDME>();
    //    services.AddScoped<MatchQualityAnalysisDME>();
    //    services.AddScoped<IMatchGraphStorage, MatchGraphStorage>();
    //    services.AddScoped<MatchingCommand>();
    //    // We don't need this configuration here 
    //    //services.Configure<QGramIndexerOptions>(opt => { });
    //    return services;
    //}

    /// <summary>
    /// Original LiteDB setup (kept for fallback/reference)
    /// </summary>
    private static void AddLiteDbStoreFactory(
        IServiceCollection services,
        string paramDbPath = "",
        string paramJobDbPath = "")
    {
        services.AddSingleton<Func<StoreType, IDataStore>>((sp) =>
        {
            var stores = new Dictionary<StoreType, Lazy<IDataStore>>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var dbPath = Application.Common.StoragePaths.DefaultDatabasePath;

            Directory.CreateDirectory(dbPath);
            var dbFilePath = string.IsNullOrEmpty(paramDbPath)
                ? Path.Combine(dbPath, "MatchLogic.db")
                : paramDbPath;
            var progressDbPath = string.IsNullOrEmpty(paramJobDbPath)
                ? Path.Combine(dbPath, "JobProgress.db")
                : paramJobDbPath;

            stores[StoreType.LiteDb] = new Lazy<IDataStore>(() =>
                new LiteDbDataStore(dbFilePath, loggerFactory.CreateLogger<LiteDbDataStore>()));

            stores[StoreType.ProgressLiteDb] = new Lazy<IDataStore>(() =>
                new LiteDbDataStore(progressDbPath, loggerFactory.CreateLogger<LiteDbDataStore>()));

            stores[StoreType.InMemory] = new Lazy<IDataStore>(() =>
                new InMemoryStore(loggerFactory.CreateLogger<InMemoryStore>()));

            stores[StoreType.MongoDB] = stores[StoreType.LiteDb];
            stores[StoreType.ProgressMongoDB] = stores[StoreType.ProgressLiteDb];

            return (storeType) =>
            {
                if (!stores.ContainsKey(storeType))
                    throw new ArgumentException($"Invalid store type: {storeType}");
                return stores[storeType].Value;
            };
        });

        services.AddSingleton<IDataStore>(sp =>
        {
            var storeFactory = sp.GetRequiredService<Func<StoreType, IDataStore>>();
            return storeFactory(StoreType.LiteDb);
        });
    }

    private static IServiceCollection AddMasterRecordDetermination(this IServiceCollection services)
    {
        // Configuration
        services.Configure<MasterRecordDeterminationConfig>(options =>
        {
            options.BatchSize = 500;
            options.MaxConcurrentBatches = 4;
            options.ChannelCapacity = 1000;
            options.MaxDatabaseConcurrency = 2;
            options.ProgressReportingInterval = TimeSpan.FromSeconds(5);
            options.EnableDetailedLogging = false;
            options.EnableAuditTrail = true;
            options.GroupProcessingTimeout = TimeSpan.FromSeconds(30);
        });

        // Repository
        services.AddScoped<IMasterRecordRuleSetRepository, MasterRecordRuleSetRepository>();

        // Core Services
        services.AddScoped<ILogicalFieldResolver, LogicalFieldResolver>();
        services.AddScoped<RuleExecutorFactory>();
        services.AddScoped<IMasterRecordDeterminationService, MasterRecordDeterminationService>();

        // Orchestrator
        services.AddScoped<IMasterRecordDeterminationOrchestrator, MasterRecordDeterminationOrchestrator>();

        services.AddTransient<MasterRecordDeterminationCommand>();

        // Rule Executors (registered individually for factory resolution)
        services.AddTransient<LongestValueRuleExecutor>();
        services.AddTransient<ShortestValueRuleExecutor>();
        services.AddTransient<MaxValueRuleExecutor>();
        services.AddTransient<MinValueRuleExecutor>();
        services.AddTransient<MostPopularRuleExecutor>();
        services.AddTransient<PreferDataSourceRuleExecutor>();
        services.AddTransient<FirstNonNullRuleExecutor>();
        services.AddTransient<MostRecentRuleExecutor>();

        return services;
    }

    public static IServiceCollection AddFieldOverwriting(this IServiceCollection services)
    {
        // Register configuration
        services.Configure<FieldOverwriteConfig>(config =>
        {
            config.BatchSize = 1000;
            config.ChannelCapacity = 100;
            config.MaxConcurrentBatches = 4;
            config.EnableDetailedLogging = false;
            config.ValidateRules = true;
        });

        // Register repository
        services.AddScoped<IFieldOverwriteRuleSetRepository, FieldOverwriteRuleSetRepository>();

        // Register factory (singleton for reuse)
        services.AddScoped<OverwriteRuleExecutorFactory>();

        // Register service (does the actual work)
        services.AddScoped<IFieldOverwriteService, FieldOverwriteService>();

        // Register orchestrator (coordinates database operations)
        services.AddScoped<IFieldOverwriteOrchestrator, FieldOverwriteOrchestrator>();

        // Register all 8 rule executors (scoped - they need ILogicalFieldResolver)
        services.AddTransient<LongestValueExecutor>();
        services.AddTransient<ShortestValueExecutor>();
        services.AddTransient<MaxValueExecutor>();
        services.AddTransient<MinValueExecutor>();
        services.AddTransient<MostPopularValueExecutor>();
        services.AddTransient<FromMasterExecutor>();
        services.AddTransient<FromBestRecordExecutor>();
        services.AddTransient<MergeAllValuesExecutor>();

        services.AddTransient<FieldOverwriteCommand>();
        // Register repository
        services.AddScoped<IFieldOverwriteRuleSetRepository, FieldOverwriteRuleSetRepository>();



        return services;
    }
    private static IServiceCollection AddFinalExport(this IServiceCollection services)
    {
        services.AddScoped<IFinalExportService, FinalExportService>();
        services.AddScoped<IExportFilePathHelper, ExportFilePathHelper>();
        services.AddScoped<FinalExportCommand>();

        return services;
    }
    /// <summary>
    /// Add BackgroundService-based scheduler (Legacy mode)
    /// </summary>
    private static IServiceCollection AddBackgroundServiceScheduler(
        this IServiceCollection services)
    {
        // Register legacy background service components
        services.AddSingleton(typeof(IBackgroundJobQueue<>), typeof(BackgroundJobQueue<>));
        services.AddHostedService<ProjectBackgroundService>();

        // Register BackgroundService implementation
        services.AddSingleton<IScheduler, BackgroundServiceScheduler>();

        return services;
    }
    public static IServiceCollection AddSchedulingServices(
    this IServiceCollection services,
    IConfiguration configuration,
    bool usingLiteDb = false)
    {

        if (configuration != null)
        {
            services.Configure<HangfireSettings>(
                configuration.GetSection(HangfireSettings.SectionName));
            services.Configure<SchedulerSettings>(
                configuration.GetSection(SchedulerSettings.SectionName));
        }
        else
        {
            services.Configure<HangfireSettings>(_ => { });
            services.Configure<SchedulerSettings>(_ => { });
        }

        if (usingLiteDb)
        {            
            AddBackgroundServiceScheduler(services);
            return services;
        }
        // Guard: no configuration available (e.g. unit tests that pass null).
        //          Fall back to the lightweight BackgroundService scheduler so
        //          the DI container is still valid without Hangfire/MongoDB config.
        if (configuration == null)
        {
            AddBackgroundServiceScheduler(services);
            return services;
        }
        // Bind settings
        var hangfireSettings = new HangfireSettings();
        configuration.GetSection(HangfireSettings.SectionName).Bind(hangfireSettings);
        services.Configure<HangfireSettings>(
            configuration.GetSection(HangfireSettings.SectionName));

        var schedulerSettings = new SchedulerSettings();
        configuration.GetSection(SchedulerSettings.SectionName).Bind(schedulerSettings);
        services.Configure<SchedulerSettings>(
            configuration.GetSection(SchedulerSettings.SectionName));

        if (!schedulerSettings.Enabled)
        {
            AddBackgroundServiceScheduler(services);
            return services;
        }

        var retryAttempts = schedulerSettings.DefaultRetryAttempts >= 0
                            ? schedulerSettings.DefaultRetryAttempts   // 0 = no retries (your intent)
                            : hangfireSettings.AutomaticRetryAttempts;

        // Get MongoDB connection from existing config
        var mongoConnectionString = configuration["MongoDB:ConnectionString"];
        var mongoDatabaseName = configuration["MongoDB:DatabaseName"];

        var jsonSettings = new Newtonsoft.Json.JsonSerializerSettings
        {
            TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Objects,  // Preserve .NET types
            NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore,
            Converters = new Newtonsoft.Json.JsonConverter[]
        {
            new GuidDictionaryJsonConverter(),  // ✅ Custom converter for Dictionary<string, object>
            new Newtonsoft.Json.Converters.StringEnumConverter()
        }
        };

        // Configure Hangfire with MongoDB
        services.AddHangfire(config => config
            .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
            .UseSimpleAssemblyNameTypeSerializer()
            .UseSerializerSettings(jsonSettings)
            .UseMongoStorage(
                mongoConnectionString,
                $"{mongoDatabaseName}_Hangfire",
                new MongoStorageOptions
                {
                    MigrationOptions = new MongoMigrationOptions
                    {
                        MigrationStrategy = new MigrateMongoMigrationStrategy(),
                        BackupStrategy = new CollectionMongoBackupStrategy()
                    },
                    Prefix = hangfireSettings.StorageOptions.Prefix,
                    CheckConnection = hangfireSettings.StorageOptions.CheckConnection,
                    CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.TailNotificationsCollection,
                    SlidingInvisibilityTimeout = hangfireSettings.StorageOptions.InvisibilityTimeout
                }));

        // Add Hangfire server
        services.AddHangfireServer(options =>
        {
            options.ServerName = $"{hangfireSettings.ServerName}-{Environment.MachineName}";
            options.WorkerCount = hangfireSettings.WorkerCount;
            options.Queues = hangfireSettings.Queues;
            options.ServerTimeout = TimeSpan.FromSeconds(hangfireSettings.ServerTimeout);
            options.SchedulePollingInterval = TimeSpan.FromSeconds(hangfireSettings.SchedulePollingInterval);
        });

        // Configure automatic retry
        GlobalJobFilters.Filters.Add(
            new AutomaticRetryAttribute
            {
                Attempts = hangfireSettings.AutomaticRetryAttempts
            });

        // Register scheduler services
        services.AddScoped<IScheduler, HangfireScheduler>();
        services.AddScoped<IJobExecutor, JobExecutor>();

        return services;
    }

    // AddLicensingServices and ValidateLicensePublicKey removed during saas-extract.
    // SaaS abuse prevention is layered: WAF + Turnstile + Cognito OTP + IQuotaService
    // (1000-record lifetime cap, atomic two-phase enforcement) + AbuseScoringService.
}