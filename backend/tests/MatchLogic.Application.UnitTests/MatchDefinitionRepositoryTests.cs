using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.Persistence;
using MatchLogic.Infrastructure.Repository;
using LiteDB;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
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
public class MatchDefinitionRepositoryTests : IDisposable
{
    private readonly string _dbPath;
    private IDataStore _dataStore;
    private IGenericRepository<MatchDefinition,Guid> _repository;
    private const string CollectionName = "matchDefinitions";
    private readonly ILogger _logger;
    Func<StoreType, IDataStore> storeFactory;
    private readonly IStoreTypeResolver _storeTypeResolver;
    public MatchDefinitionRepositoryTests()
    {
        _dbPath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<MatchDefinitionRepositoryTests>();
        // Resolver for tests: always resolve to LiteDb — no config or attribute needed
        _storeTypeResolver = new FixedStoreTypeResolver(StoreType.LiteDb);
        storeFactory = (StoreType storeType) =>
        {
            return  new LiteDbDataStore(_dbPath, _logger);
        };
    }

    [Fact]
    public async Task GetByJobId_WhenMatchExists_ReturnsMatch()
    {
        //
        _repository = new GenericRepository<MatchDefinition,Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);
        // Arrange
        var jobId = Guid.NewGuid();
        var id = Guid.NewGuid();
        var matchDefinition = new MatchDefinition
        {
            Id = id,
            JobId = jobId,
            Name = "Test Match Definition",
            Criteria = new List<MatchCriteria>()
            {
                new MatchCriteria()
                {                    
                    DataType= CriteriaDataType.Text,
                    FieldName = "City",
                    MatchingType = MatchingType.Exact,
                }
            }
        };
        await _repository.InsertAsync(matchDefinition, CollectionName);

        // Act
        var result = await _repository.QueryAsync(x=> x.JobId == jobId, CollectionName);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.FirstOrDefault());
        Assert.Equal(jobId, result?.FirstOrDefault()?.JobId);
    }

    [Fact]
    public async Task GetByJobId_WhenNoMatch_ReturnsNull()
    {
        //
        _repository = new  GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);

        // Arrange
        var nonexistentJobId = Guid.NewGuid();

        // Act
        var result = await _repository.QueryAsync(x => x.JobId == nonexistentJobId, CollectionName);

        // Assert
        Assert.Equal(result?.Count,0);
    }

    [Fact]
    public async Task GetByJobId_WithEmptyCollection_ReturnsNull()
    {
        
        _repository =  new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);
        // Act
        var result = await _repository.QueryAsync(x=> x.JobId == Guid.NewGuid(), CollectionName);

        // Assert
        Assert.Equal(result?.Count, 0);
    }
    [Fact]
    public async Task GetAllAsync_WhenEmpty_ShouldReturnEmptyList()
    {
        
        _repository = new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);
        // Act
        var result = await _repository.GetAllAsync(CollectionName);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task InsertAndGetAll_ShouldReturnAllInsertedEntities()
    {
        
        _repository = new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);
        // Arrange
        var entities = new List<MatchDefinition>
        {
            new()
            {
                Id = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Match 1",
                Criteria = new List<MatchCriteria>
                {
                    new()
                    {                        
                        FieldName = "Name",
                        MatchingType = MatchingType.Exact,
                        DataType = CriteriaDataType.Text,
                        Arguments = new Dictionary<ArgsValue, string>()
                    }
                }
            },
            new()
            {
                Id = Guid.NewGuid(),
                JobId = Guid.NewGuid(),
                Name = "Match 2",
                Criteria = new List<MatchCriteria>
                {
                    new()
                    {                        
                        FieldName = "Email",
                        MatchingType = MatchingType.Fuzzy,
                        DataType = CriteriaDataType.Text,
                        Arguments = new Dictionary<ArgsValue, string>()
                    }
                }
            }
        };

        // Act
        foreach (var entity in entities)
        {
            await _repository.InsertAsync(entity, CollectionName);
        }

        var result = await _repository.GetAllAsync(CollectionName);

        // Assert
        Assert.Equal(2,result?.Count());
        
    }

    [Fact]
    public async Task UpdateAsync_ShouldModifyExistingEntity()
    {
        
        _repository = new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);

        // Arrange
        var entity = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Original Name",
            Criteria = new List<MatchCriteria>()
        };

        await _repository.InsertAsync(entity, CollectionName);

        // Act
        entity.Name = "Updated Name";
        entity.Criteria.Add(new MatchCriteria
        {            
            FieldName = "NewField",
            MatchingType = MatchingType.Exact,
            DataType = CriteriaDataType.Text,
            Arguments = new Dictionary<ArgsValue, string>()
        });

        await _repository.UpdateAsync(entity, CollectionName);

        var result = (await _repository.GetAllAsync(CollectionName)).FirstOrDefault();

        // Assert
        Assert.NotNull(result);
        Assert.Equal("Updated Name", result?.Name);
        Assert.Equal(1, result?.Criteria.Count);
        Assert.Equal("NewField", result?.Criteria.First().FieldName);
    }

    [Fact]
    public async Task DeleteAsync_ShouldRemoveEntity()
    {
        
        _repository = new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);

        // Arrange
        var entity = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "To Be Deleted",
            Criteria = new List<MatchCriteria>()
        };

        await _repository.InsertAsync(entity, CollectionName);

        // Act
        await _repository.DeleteAsync(entity.Id, CollectionName);
        var result = await _repository.GetAllAsync(CollectionName);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task InsertAsync_WithDuplicateId_ShouldThrowException()
    {
        
        _repository = new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);

        // Arrange
        var id = Guid.NewGuid();
        var entity1 = new MatchDefinition
        {
            Id = id,
            JobId = Guid.NewGuid(),
            Name = "First",
            Criteria = new List<MatchCriteria>()
        };

        var entity2 = new MatchDefinition
        {
            Id = id, // Same ID
            JobId = Guid.NewGuid(),
            Name = "Second",
            Criteria = new List<MatchCriteria>()
        };

        await _repository.InsertAsync(entity1, CollectionName);

        // Act & Assert
        await Assert.ThrowsAsync<LiteException>(() =>
            _repository.InsertAsync(entity2, CollectionName));
    }
    [Fact]
    public async Task InsertMultipleAndVerifyOrder()
    {
        
        _repository = new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);

        // Arrange
        var entities = Enumerable.Range(1, 3).Select(i => new MatchDefinition
        {
            Id = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = $"Match {i}",
            Criteria = new List<MatchCriteria>
            {
                new()
                {                    
                    FieldName = $"Field{i}",
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Arguments = new Dictionary<ArgsValue, string>()
                }
            }
        }).ToList();

        // Act
        foreach (var entity in entities)
        {
            await _repository.InsertAsync(entity, CollectionName);
        }
        var result = await _repository.GetAllAsync(CollectionName);

        // Assert
        Assert.NotEmpty(result);
        Assert.Equal(3, result.Count);

        foreach (var expectedEntity in entities)
        {
            // Verify only one entity exists with this Id
            Assert.Single(result,(x => x.Id == expectedEntity.Id));

            // Get the saved entity
            var savedEntity = result.First(x => x.Id == expectedEntity.Id);

            // Verify basic properties
            Assert.Equal(expectedEntity.Name, savedEntity.Name);
            Assert.Equal(expectedEntity.JobId, savedEntity.JobId);

            // Verify Criteria collection
            Assert.NotNull(savedEntity.Criteria);
            Assert.Single(savedEntity.Criteria);
            Assert.Equal(
                expectedEntity.Criteria.First().FieldName,
                savedEntity.Criteria.First().FieldName
            );
        }
    }

    [Fact]
    public async Task InsertAsync_WithComplexCriteria_ShouldPersistCorrectly()
    {
        
        _repository = new GenericRepository<MatchDefinition, Guid>(storeFactory, _storeTypeResolver, new Mock<ICurrentUser>().Object);
        // Arrange
        var entity = new MatchDefinition
        {
            Id = Guid.NewGuid(),
            JobId = Guid.NewGuid(),
            Name = "Complex Match",
            Criteria = new List<MatchCriteria>
            {
                new()
                {                    
                    FieldName = "Name",
                    MatchingType = MatchingType.Fuzzy,
                    DataType = CriteriaDataType.Text,
                    Arguments = new Dictionary<ArgsValue, string>
                    {
                        { ArgsValue.FastLevel, "0.9" },
                        { ArgsValue.Level, "0.95" }
                    }
                },
                new()
                {                    
                    FieldName = "Email",
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Arguments = new Dictionary<ArgsValue, string>()
                }
            }
        };

        // Act
        await _repository.InsertAsync(entity, CollectionName);
        var result = await _repository.GetAllAsync(CollectionName);

        // Assert
        Assert.NotNull(result);
        Assert.Single(result);

        var savedEntity = Assert.Single(result);
        Assert.NotNull(savedEntity);
        Assert.Equal(entity.Id, savedEntity.Id);
        Assert.Equal("Complex Match", savedEntity.Name);

        Assert.NotNull(savedEntity.Criteria);
        Assert.Equal(2, savedEntity.Criteria.Count);

        // Find and verify the Name criteria
        var nameCriteria = Assert.Single(savedEntity.Criteria,
            c => c.FieldName == "Name" && c.MatchingType == MatchingType.Fuzzy);

        Assert.True(nameCriteria.Arguments.ContainsKey(ArgsValue.FastLevel));
        Assert.Equal("0.9", nameCriteria.Arguments[ArgsValue.FastLevel]);

        // Verify the Email criteria
        Assert.Single(savedEntity.Criteria,
           c => c.FieldName == "Email" && c.MatchingType == MatchingType.Exact);
    }

    public void Dispose()
    {
        //Thread.Sleep(100);
        //try
        //{
        //    if (File.Exists(_dbPath))
        //    {
        //        File.Delete(_dbPath);
        //    }
        //}
        //catch (IOException)
        //{
        //    // Log or handle the case where the file cannot be deleted
        //}
    }
}
