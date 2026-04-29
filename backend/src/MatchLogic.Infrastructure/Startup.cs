using MatchLogic.Application.Common;
using MatchLogic.Application.Core;
using MatchLogic.Application.Features.DataProfiling;
using MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Features.Regex;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Interfaces.Dictionary;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Regex;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Application.Interfaces.Storage;
using MatchLogic.Domain.Dictionary;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Regex;
using MatchLogic.Infrastructure.BackgroundJob;
using MatchLogic.Infrastructure.Common;
using MatchLogic.Infrastructure.Configuration;
using MatchLogic.Infrastructure.Dictionary;
using MatchLogic.Infrastructure.Events.Providers;
using MatchLogic.Infrastructure.Import;
using MatchLogic.Infrastructure.Persistence;
using MatchLogic.Infrastructure.Persistence.MongoDB;
using MatchLogic.Infrastructure.Project;
using MatchLogic.Infrastructure.Project.Commands;
using MatchLogic.Infrastructure.Project.DataProfiling;
using MatchLogic.Infrastructure.Project.DataSource;
using MatchLogic.Infrastructure.Repository;
using MatchLogic.Infrastructure.Scheduling;
using MatchLogic.Infrastructure.Security;
using MatchLogic.Infrastructure.Storage;
using Amazon;
using Amazon.S3;
using Hangfire;
using Hangfire.Mongo;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using MassTransit;
using MassTransit.NewIdProviders;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
// Profiling SaaS slice — slimmed from main-product Startup during saas-extract.
// Removed: matching/cleansing/merge/survivorship/wordsmith/livesearch/finalexport pipelines,
// LiteDb/Sqlite/InMemory data stores, phonetic/comparator/transliterator services, dual mode-switch.
// SaaS uses MongoDB + Hangfire + DataProfiling only.

namespace MatchLogic.Infrastructure;

public static class ApplicationSetup
{
    public static IServiceCollection AddApplicationSetup(
        this IServiceCollection services,
        string paramDbPath = "",
        string paramJobDbPath = "",
        IConfiguration configuration = null)
    {
        // Data store: MongoDB only (LiteDb/Sqlite/InMemory removed in saas-extract).
        RegisterStoreSettings(services, configuration);
        ValidateStoreSettings(configuration);
        services.AddMongoDbDataStore(configuration);

        // Core services
        services.AddScoped<ICommandFactory, CommandFactory>();

        // Data import + readers
        services.AddSingleton<ConnectionConfigFactory, ConnectionConfigFactory>();
        services.AddSingleton<ConnectionReaderStrategyFactory, ConnectionReaderStrategyFactory>();
        services.AddScoped<IConnectionBuilder, ConnectionBuilder>();
        services.AddSingleton<IEncryptionService, EncryptionService>();
        services.AddSingleton<IHeaderUtility, HeaderUtility>();
        services.AddScoped<ISecureParameterHandler, SecureParameterHandler>();

        // Remote storage / OAuth — TODO (M4e): drop entirely if SaaS stays CSV/Excel-upload-only.
        services.AddSingleton<RemoteFileConnectorFactory>();
        services.AddSingleton<ITempFileManager, TempFileManager>();
        services.AddScoped<IOAuthTokenService, OAuthTokenService>();

        // Common infrastructure
        NewId.SetProcessIdProvider(new CurrentProcessIdProvider());
        services.AddRepositories();
        AddEventBus(services);
        AddInfrastructureServices(services);
        AddDataProfiling(services);
        AddSchedulingServices(services, configuration);
        AddS3Storage(services, configuration);

        return services;
    }

    private static IServiceCollection AddS3Storage(
        IServiceCollection services,
        IConfiguration configuration)
    {
        // Bucket name + region come from SSM in deployed Fargate, appsettings.Development.json
        // locally. AWS credentials resolve via the standard chain — task IAM role in Fargate,
        // ~/.aws/credentials locally.
        services.Configure<S3Options>(configuration.GetSection(S3Options.SectionName));

        services.AddSingleton<IAmazonS3>(sp =>
        {
            var opts = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<S3Options>>().Value;
            var region = string.IsNullOrWhiteSpace(opts.Region) ? "us-east-1" : opts.Region;
            return new AmazonS3Client(RegionEndpoint.GetBySystemName(region));
        });

        services.AddSingleton<IFileStorageService, S3FileStorageService>();
        services.AddScoped<IFileSourceResolver, FileSourceResolver>();

        return services;
    }

    private static void RegisterStoreSettings(
        IServiceCollection services,
        IConfiguration configuration)
    {
        if (configuration != null)
            services.Configure<StoreSettings>(configuration.GetSection(StoreSettings.SectionName));
        else
            services.Configure<StoreSettings>(_ => { });

        services.AddSingleton<IStoreTypeResolver, StoreTypeResolver>();
    }

    private static void ValidateStoreSettings(IConfiguration configuration)
    {
        if (configuration == null) return;

        var section = configuration.GetSection(StoreSettings.SectionName);
        if (!section.Exists()) return;

        var settings = section.Get<StoreSettings>();
        if (settings == null) return;

        var validNames = Enum.GetNames<StoreType>();
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
                throw new InvalidOperationException(
                    $"Invalid StoreType '{value}' found at '{label}' in appsettings. " +
                    $"Valid values are: {string.Join(", ", validNames)}");
        }
    }

    private static IServiceCollection AddEventBus(this IServiceCollection services)
    {
        services.AddScoped<IEventBus, MediatREventBus>();
        return services;
    }

    private static IServiceCollection AddInfrastructureServices(this IServiceCollection services)
    {
        services.AddSingleton<IJobCancellationRegistry, JobCancellationRegistry>();
        services.AddHostedService<StaleJobCleanupService>();
        services.AddScoped<IJobEventPublisher, JobEventPublisher>();
        return services;
    }

    public static IServiceCollection AddRepositories(this IServiceCollection services)
    {
        services.AddScoped(typeof(IGenericRepository<,>), typeof(GenericRepository<,>));
        services.AddScoped<IJobStatusRepository, JobStatusRepository>();
        // IProfileRepository is registered in AddDataProfiling.

        // Application services kept after the orphan prune.
        services.AddScoped<IDataSourceService, DataSourceService>();
        services.AddScoped<IFileImportService, FileImportService>();
        services.AddScoped<ISchemaValidationService, SchemaValidationService>();
        services.AddScoped<IColumnFilter, ColumnFilter>();

        // Hangfire commands resolved by CommandFactory at job-execution time.
        services.AddScoped<DataImportCommand>();
        services.AddScoped<DataProfilingCommand>();
        services.AddScoped<AdvanceDataProfilingCommand>();
        services.AddSingleton<MatchLogic.Application.Interfaces.Common.IRecordHasher, SHA256RecordHasher>();

        return services;
    }

    public static IServiceCollection AddDataProfiling(this IServiceCollection services)
    {
        services.Configure<ProfilingOptions>(options =>
        {
            options.BatchSize = 5000;
            options.MaxDegreeOfParallelism = Environment.ProcessorCount;
            options.BufferSize = 10000;
            options.SampleSize = 100;
            options.MaxRowsPerCategory = 50;
            options.MaxDistinctValuesToTrack = 100;
            options.StoreCompleteRows = true;
        });

        services.AddScoped<IDataProfiler, DataProfiler>();
        services.AddScoped<IRegexInfoService, RegexInfoService>();
        services.AddScoped<IDictionaryCategoryService, DictionaryCategoryService>();

        services.AddScoped<IDataSeedProvider<DictionaryCategory>, DictionaryCategorySeedProvider>();
        services.AddScoped<IDataSeedProvider<RegexInfo>, RegexSeedProvider>();

        services.AddScoped<IDataSeedProvider<IEntity>>(sp =>
            new DataSeedProviderAdapter<DictionaryCategory>(
                sp.GetRequiredService<IDataSeedProvider<DictionaryCategory>>()));
        services.AddScoped<IDataSeedProvider<IEntity>>(sp =>
            new DataSeedProviderAdapter<RegexInfo>(
                sp.GetRequiredService<IDataSeedProvider<RegexInfo>>()));

        services.AddScoped<IMigrationService, MigrationService>();

        services.AddScoped<IProfileRepository, ProfileRepository>();
        services.AddScoped<IProfileService, ProfileService>();

        services.Configure<AdvancedProfilingOptions>(options =>
        {
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

    public static IServiceCollection AddSchedulingServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var hangfireSettings = new HangfireSettings();
        configuration.GetSection(HangfireSettings.SectionName).Bind(hangfireSettings);
        services.Configure<HangfireSettings>(configuration.GetSection(HangfireSettings.SectionName));

        var mongoConnectionString = configuration["MongoDB:ConnectionString"];
        var mongoDatabaseName = configuration["MongoDB:DatabaseName"];

        var jsonSettings = new Newtonsoft.Json.JsonSerializerSettings
        {
            TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Objects,
            NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore,
            Converters = new Newtonsoft.Json.JsonConverter[]
            {
                new GuidDictionaryJsonConverter(),
                new Newtonsoft.Json.Converters.StringEnumConverter()
            }
        };

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

        services.AddHangfireServer(options =>
        {
            options.ServerName = $"{hangfireSettings.ServerName}-{Environment.MachineName}";
            options.WorkerCount = hangfireSettings.WorkerCount;
            options.Queues = hangfireSettings.Queues;
            options.ServerTimeout = TimeSpan.FromSeconds(hangfireSettings.ServerTimeout);
            options.SchedulePollingInterval = TimeSpan.FromSeconds(hangfireSettings.SchedulePollingInterval);
        });

        GlobalJobFilters.Filters.Add(new AutomaticRetryAttribute
        {
            Attempts = hangfireSettings.AutomaticRetryAttempts
        });

        services.AddScoped<IScheduler, HangfireScheduler>();
        services.AddScoped<IJobExecutor, JobExecutor>();

        return services;
    }
    // AddLicensingServices and ValidateLicensePublicKey removed during saas-extract.
    // SaaS abuse prevention is layered: WAF + Turnstile + Cognito OTP + IQuotaService
    // (1000-record lifetime cap, atomic two-phase enforcement) + AbuseScoringService.
}
