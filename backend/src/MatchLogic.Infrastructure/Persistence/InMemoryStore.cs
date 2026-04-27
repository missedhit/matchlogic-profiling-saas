using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence;
public class InMemoryStore : IDataStore
{
    private readonly ILogger<InMemoryStore> _logger;
    private bool _disposed;
    public InMemoryStore(ILogger<InMemoryStore> logger)
    {
        _collections = new ConcurrentDictionary<string, ConcurrentDictionary<object, object>>();
        _logger = logger;
    }

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<object, object>> _collections;
    public Task<int> DeleteAllAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task DeleteAsync<Tkey>(Tkey id, string collectionName)
    {
        if (id == null) throw new ArgumentNullException(nameof(id));

        if (_collections.TryGetValue(collectionName, out var collection))
        {
            collection.TryRemove(id, out _);
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _collections.Clear();
    }

    public Task<List<T>> GetAllAsync<T>(string collectionName)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<IDictionary<string, object>> StreamDataAsync(string collectionName, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public Task<T> GetByIdAsync<T, Tkey>(Tkey id, string collectionName)
    {
        if (id == null) throw new ArgumentNullException(nameof(id));

        if (!_collections.TryGetValue(collectionName, out var collection))
        {
            throw new KeyNotFoundException($"Collection {collectionName} not found");
        }

        if (!collection.TryGetValue(id, out var entity))
        {
            throw new KeyNotFoundException($"Entity with ID {id} not found");
        }

        return Task.FromResult((T)entity);
    }

    public Task<IEnumerable<IDictionary<string, object>>> GetJobDataAsync(Guid jobId)
    {
        throw new NotImplementedException();
    }

    public Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedDataAsync(string collectionName, int pageNumber, int pageSize)
    {
        throw new NotImplementedException();
    }

    public Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobDataAsync(Guid jobId, int pageNumber, int pageSize)
    {
        throw new NotImplementedException();
    }

    public Task<Guid> InitializeJobAsync(string collectionName = "")
    {
        throw new NotImplementedException();
    }

    public Task<bool> DeleteCollection(string CollectionName)
    {
        throw new NotImplementedException();
    }

    public Task InsertAsync<T>(T entity, string collectionName)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var collection = _collections.GetOrAdd(collectionName,
            _ => new ConcurrentDictionary<object, object>());

        var id = GetEntityId(entity);
        if (!collection.TryAdd(id, entity))
        {
            throw new InvalidOperationException($"Entity with ID {id} already exists");
        }

        return Task.CompletedTask;
    }

    public Task InsertBatchAsync(Guid jobId, IEnumerable<IDictionary<string, object>> batch, string collectionName = "")
    {
        throw new NotImplementedException();
    }

    public Task InsertBatchAsync(string collectionName, IEnumerable<IDictionary<string, object>> batch)
    {
        throw new NotImplementedException();
    }

    public Task<List<T>> QueryAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        if (!_collections.TryGetValue(collectionName, out var collection))
        {
            return Task.FromResult(new List<T>());
        }

        var compiledPredicate = predicate.Compile();
        var result = collection.Values.OfType<T>().Where(compiledPredicate).ToList();
        return Task.FromResult(result);
    }

    public IAsyncEnumerable<IDictionary<string, object>> StreamJobDataAsync(Guid jobId, IStepProgressTracker progressTracker, string collectionName = "", CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync<T>(T entity, string collectionName)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var collection = _collections.GetOrAdd(collectionName,
            _ => new ConcurrentDictionary<object, object>());

        var id = GetEntityId(entity);
        if (!collection.TryUpdate(id, entity, collection[id]))
        {
            throw new InvalidOperationException($"Entity with ID {id} not found or was modified");
        }

        return Task.CompletedTask;
    }

    private static object GetEntityId<T>(T entity)
    {
        var idProperty = typeof(T).GetProperty("Id") ??
                        typeof(T).GetProperty("ID") ??
                        typeof(T).GetProperty("id");

        if (idProperty == null)
        {
            throw new InvalidOperationException("Entity must have an Id property");
        }

        return idProperty.GetValue(entity) ??
               throw new InvalidOperationException("Id cannot be null");
    }


    public IAsyncEnumerable<IDictionary<string, object>> GetRandomSample(double maxPairs, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task SampleAndStoreTempData(string sourceCollectionName, string tempCollectionName, double maxPairs)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<IDictionary<string, object>> GetStreamFromTempCollection(string _tempCollectionName, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task InsertProbabilisticBatchAsync(string collectionName, IEnumerable<MatchResult> batch)
    {
        throw new NotImplementedException();
    }

    public Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobWithSortingAndFilteringDataAsync(string collectionName, int pageNumber, int pageSize, string filterText = null, string sortColumn = null, bool ascending = true, string filters = "",
            GroupQueryFilter groupFilter = null)
    {
        throw new NotImplementedException();
    }

    public Task BulkInsertAsync<T>(IEnumerable<T> entity, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync(IDictionary<string, object> entity, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedWithSmartFilteringAndProjectionAsync(string collectionName, int pageNumber, int pageSize, string filterText = null, string sortColumn = null, bool ascending = true, string filters = "")
    {
        throw new NotImplementedException();
    }

    public Task BulkUpdateAsync(IEnumerable<IDictionary<string, object>> entities, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task<bool> UpdateByFieldAsync<TField>(IDictionary<string, object> data, string collectionName, string fieldName, TField fieldValue)
    {
        throw new NotImplementedException();
    }

    public Task<bool> RenameCollection(string oldName, string newName)
    {
        throw new NotImplementedException();
    }

    public Task<bool> CollectionExistsAsync(string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task BulkUpsertAsync<T>(IEnumerable<T> entities, string collectionName)
    {
        foreach (var entity in entities)
        {
            var collection = _collections.GetOrAdd(collectionName,
                _ => new ConcurrentDictionary<object, object>());

            var id = GetEntityId(entity);
            collection.AddOrUpdate(id, entity, (key, oldValue) => entity);
        }

        return Task.CompletedTask;
    }

    public Task BulkUpsertByFieldsAsync<T>(
        IEnumerable<T> entities,
        string collectionName,
        Expression<Func<T, object>>[] matchFields)
    {
        // For in-memory, just use Id-based upsert
        return BulkUpsertAsync(entities, collectionName);
    }

    public Task CreateGroupFilterIndexesAsync(string collectionName)
    {
        throw new NotImplementedException();
    }
}
