using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Auth.Interfaces;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Infrastructure.Persistence;
using LiteDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Repository;
public class GenericRepository<T, TKey> : IGenericRepository<T, TKey> where T : IEntity
{
    protected readonly IDataStore _dataStore;
    private readonly ICurrentUser _currentUser;

    public GenericRepository(Func<StoreType, IDataStore> storeFactory,
        IStoreTypeResolver storeTypeResolver,
        ICurrentUser currentUser)
    {
        // GetType() returns the concrete derived type (e.g. JobStatusRepository),
        // so resolver overrides keyed by short class name work correctly.
        var storeType = storeTypeResolver.Resolve(GetType());
        _dataStore = storeFactory(storeType);
        _currentUser = currentUser;
    }

    public async Task<T> GetByIdAsync(TKey id, string collectionName)
    {
        return await Task.Run(async () => await _dataStore.GetByIdAsync<T, TKey>(id, collectionName));
    }

    public async Task<List<T>> GetAllAsync(string collectionName)
    {
        return await Task.Run(async () => await _dataStore.GetAllAsync<T>(collectionName));
    }

    public async Task InsertAsync(T entity, string collectionName)
    {
        if (entity is IAuditableEntity auditable)
        {
            auditable.CreatedBy  = _currentUser.UserId;
            auditable.CreatedAt  = _currentUser.UtcNow;
            auditable.ModifiedBy = null;
            auditable.ModifiedAt = null;
        }
        await Task.Run(async () => await _dataStore.InsertAsync<T>(entity, collectionName));
    }

    public async Task UpdateAsync(T entity, string collectionName)
    {
        if (entity is IAuditableEntity auditable)
        {
            // Do NOT overwrite CreatedBy or CreatedAt
            auditable.ModifiedBy = _currentUser.UserId;
            auditable.ModifiedAt = _currentUser.UtcNow;
        }
        await Task.Run(async () => await _dataStore.UpdateAsync<T>(entity, collectionName));
    }

    public async Task DeleteAsync(TKey id, string collectionName)
    {
        await Task.Run(async () => await _dataStore.DeleteAsync(id, collectionName));
    }

    public async Task<List<T>> QueryAsync(Expression<Func<T, bool>> predicate, string collectionName)
    {
        return await Task.Run(async () => await _dataStore.QueryAsync(predicate, collectionName));
    }

    public async Task<int> DeleteAllAsync(Expression<Func<T, bool>> predicate, string collectionName)
    {
        return await Task.Run(async () => await _dataStore.DeleteAllAsync(predicate, collectionName));
    }

    public async Task BulkInsertAsync(IEnumerable<T> entity, string collectionName)
    {
        await Task.Run(async () => await _dataStore.BulkInsertAsync<T>(entity, collectionName));
    }
}
