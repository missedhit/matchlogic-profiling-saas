using MatchLogic.Application.Common;
using MatchLogic.Application.Core;
using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Dictionary;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Regex;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Infrastructure.Repository;
using MatchLogic.Infrastructure.Persistence;
using MatchLogic.Domain.Auth.Interfaces;
using Moq;

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Minimal IStoreTypeResolver stub for unit tests.
/// Always returns the specified StoreType regardless of repository type,
/// which is exactly what test isolation requires.
/// </summary>
file sealed class FixedStoreTypeResolver : IStoreTypeResolver
{
    private readonly StoreType _storeType;

    public FixedStoreTypeResolver(StoreType storeType = StoreType.LiteDb)
        => _storeType = storeType;

    public StoreType Resolve(Type repositoryType) => _storeType;
}
public class MigrationServiceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly ILogger<MigrationService> _logger;
    private readonly Func<StoreType, IDataStore> _storeFactory;
    private readonly IDataStore _dataStore;
    private readonly IGenericRepository<RegexInfo, Guid> _regexRepository;
    private readonly IGenericRepository<DictionaryCategory, Guid> _dictionaryRepository;
    private readonly IDataSeedProvider<RegexInfo> _regexSeedProvider;
    private readonly IDataSeedProvider<DictionaryCategory> _dictionarySeedProvider;
    private readonly List<IDataSeedProvider<IEntity>> _seedProviders = new();
    private readonly IGenericRepository<IEntity, Guid> _genericRepository;
    private readonly MigrationService _migrationService;
    private readonly IStoreTypeResolver _storeTypeResolver;

    public MigrationServiceTests()
    {
        // Create temporary database for tests
        _dbPath = Path.GetTempFileName();
        _logger = new NullLogger<MigrationService>();

        // Resolver for tests: always resolve to LiteDb — no config or attribute needed
        _storeTypeResolver = new FixedStoreTypeResolver(StoreType.LiteDb);
        // Setup data store factory
        _storeFactory = (storeType) =>
        {
            return _dataStore ?? new LiteDbDataStore(_dbPath, new NullLogger<LiteDbDataStore>());
        };

        // Create the data store
        _dataStore = _storeFactory(StoreType.LiteDb);

        // Create concrete repositories
        var mockCurrentUser = new Mock<ICurrentUser>();
        _regexRepository = new GenericRepository<RegexInfo, Guid>(_storeFactory, _storeTypeResolver, mockCurrentUser.Object);
        _dictionaryRepository = new GenericRepository<DictionaryCategory, Guid>(_storeFactory, _storeTypeResolver, mockCurrentUser.Object);
        _genericRepository = new GenericRepository<IEntity, Guid>(_storeFactory, _storeTypeResolver, mockCurrentUser.Object);

        // Create concrete seed providers
        _regexSeedProvider = new RegexSeedProvider();
        _dictionarySeedProvider = new DictionaryCategorySeedProvider();

        // Create seed provider adapters to convert to IEntity<Guid>
        _seedProviders.Add(new DataSeedProviderAdapter<RegexInfo>(_regexSeedProvider));
        _seedProviders.Add(new DataSeedProviderAdapter<DictionaryCategory>(_dictionarySeedProvider));

        // Create the migration service
        _migrationService = new MigrationService(_logger, _seedProviders, _dataStore);
    }

    public void Dispose()
    {
        // Clean up the temporary database
        if (File.Exists(_dbPath))
        {
            _dataStore.Dispose();
            File.Delete(_dbPath);
        }
    }

    [Fact]
    public async Task NeedsInitialization_EmptyDatabase_ReturnsTrue()
    {
        // Act
        var result = await _migrationService.NeedsInitialization();

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task InitializeDatabase_EmptyDatabase_SeedsAllCollections()
    {
        // Arrange
        var regexSeedData = _regexSeedProvider.GetSeedData();
        var dictionarySeedData = _dictionarySeedProvider.GetSeedData();

        // Act
        await _migrationService.InitializeDatabase();

        // Assert
        var regexItems = await _regexRepository.GetAllAsync(Constants.Collections.RegexInfo);
        var dictionaryItems = await _dictionaryRepository.GetAllAsync(Constants.Collections.DictionaryCategory);

        Assert.Equal(regexSeedData.Count(), regexItems.Count);
        Assert.Equal(dictionarySeedData.Count(), dictionaryItems.Count);

        // Verify some specific items
        Assert.Contains(regexItems, r => r.Name == "Email Address");
        Assert.Contains(dictionaryItems, d => d.Name == "US States");
        Assert.Contains(dictionaryItems, d => d.Name == "First Names");
    }

    [Fact]
    public async Task NeedsInitialization_AfterInitialization_ReturnsFalse()
    {
        // Arrange
        await _migrationService.InitializeDatabase();

        // Act
        var result = await _migrationService.NeedsInitialization();

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task InitializeDatabase_AlreadyInitialized_DoesNotDuplicateData()
    {
        // Arrange
        var regexSeedData = _regexSeedProvider.GetSeedData();
        var dictionarySeedData = _dictionarySeedProvider.GetSeedData();

        // First initialization
        await _migrationService.InitializeDatabase();

        // Act - second initialization
        await _migrationService.InitializeDatabase();

        // Assert
        var regexItems = await _regexRepository.GetAllAsync(Constants.Collections.RegexInfo);
        var dictionaryItems = await _dictionaryRepository.GetAllAsync(Constants.Collections.DictionaryCategory);

        // Counts should still match the original seed data
        Assert.Equal(regexSeedData.Count(), regexItems.Count);
        Assert.Equal(dictionarySeedData.Count(), dictionaryItems.Count);
    }

    [Fact]
    public async Task InitializeDatabase_VerifyItemContent()
    {
        // Arrange
        await _migrationService.InitializeDatabase();

        // Act
        var regexItems = await _regexRepository.GetAllAsync(Constants.Collections.RegexInfo);
        var dictionaryItems = await _dictionaryRepository.GetAllAsync(Constants.Collections.DictionaryCategory);

        // Assert - Check regex item content
        var emailRegex = regexItems.FirstOrDefault(r => r.Name == "Email Address");
        Assert.NotNull(emailRegex);
        Assert.Equal("Matches valid email addresses", emailRegex.Description);
        Assert.True(emailRegex.IsSystem);
        Assert.True(emailRegex.IsDefault);

        // Assert - Check dictionary item content
        var usStates = dictionaryItems.FirstOrDefault(d => d.Name == "US States");
        Assert.NotNull(usStates);
        Assert.Equal("List of US states and territories", usStates.Description);
        Assert.True(usStates.IsSystem);
        Assert.Contains("California", usStates.Items);
        Assert.Contains("Texas", usStates.Items);
        Assert.Equal(56, usStates.Items.Count); // 50 states + DC + territories

        var firstNames = dictionaryItems.FirstOrDefault(d => d.Name == "First Names");
        Assert.NotNull(firstNames);
        Assert.Contains("John", firstNames.Items);
        Assert.Contains("Mary", firstNames.Items);
    }

    [Fact]
    public async Task InitializeDatabase_PartiallyPopulatedDatabase_OnlyFillsEmptyCollections()
    {
        // Arrange - Add a single regex item but leave dictionary empty
        var singleRegex = new RegexInfo
        {
            Id = Guid.NewGuid(),
            Name = "Test Regex",
            Description = "Test Description",
            RegexExpression = @"^test$",
            IsDefault = true,
            IsSystem = false,
            Version = 1
        };

        await _regexRepository.InsertAsync(singleRegex, Constants.Collections.RegexInfo);

        // Act
        await _migrationService.InitializeDatabase();

        // Assert
        var regexItems = await _regexRepository.GetAllAsync(Constants.Collections.RegexInfo);
        var dictionaryItems = await _dictionaryRepository.GetAllAsync(Constants.Collections.DictionaryCategory);

        // Regex items should not be duplicated (should just keep our single item)
        Assert.Single(regexItems);
        Assert.Equal("Test Regex", regexItems.First().Name);

        // Dictionary items should be populated
        Assert.Equal(_dictionarySeedProvider.GetSeedData().Count(), dictionaryItems.Count);
    }
}


