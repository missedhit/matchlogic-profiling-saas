using MatchLogic.Application.Interfaces.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Persistence;
public interface IDataStore : IDisposable
{
    Task<Guid> InitializeJobAsync(string collectionName = "");
    Task<bool> DeleteCollection(string collectionName);
    Task InsertBatchAsync(Guid jobId, IEnumerable<IDictionary<string, object>> batch, string collectionName = "");
    Task<IEnumerable<IDictionary<string, object>>> GetJobDataAsync(Guid jobId);
    Task InsertBatchAsync(string collectionName, IEnumerable<IDictionary<string, object>> batch);
    Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobDataAsync(Guid jobId, int pageNumber, int pageSize);
    Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedDataAsync(string collectionName, int pageNumber, int pageSize);
    IAsyncEnumerable<IDictionary<string, object>> StreamDataAsync(string collectionName, CancellationToken cancellationToken = default);


    IAsyncEnumerable<IDictionary<string, object>> StreamJobDataAsync(Guid jobId, IStepProgressTracker progressTracker, string collectionName = "", CancellationToken cancellationToken = default);

    Task<List<T>> QueryAsync<T>(Expression<Func<T, bool>> predicate, string collectionName);
    Task<T> GetByIdAsync<T, Tkey>(Tkey id, string collectionName);
    Task<List<T>> GetAllAsync<T>(string collectionName);
    Task InsertAsync<T>(T entity, string collectionName);
    Task UpdateAsync<T>(T entity, string collectionName);
    Task UpdateAsync(IDictionary<string, object> entity, string collectionName);
    Task BulkUpdateAsync(IEnumerable<IDictionary<string, object>> entities, string collectionName);
    Task DeleteAsync<Tkey>(Tkey id, string collectionName);
    Task<int> DeleteAllAsync<T>(Expression<Func<T, bool>> predicate, string collectionName);
    Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobWithSortingAndFilteringDataAsync(
       string collectionName,
       int pageNumber,
       int pageSize,
       string filterText = null,
       string sortColumn = null,
       bool ascending = true,
       string filters = "");
    Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedWithSmartFilteringAndProjectionAsync(
        string collectionName,
        int pageNumber,
        int pageSize,
        string filterText = null,
        string sortColumn = null,
        bool ascending = true,
        string filters = "");

    IAsyncEnumerable<IDictionary<string, object>> GetRandomSample(double maxPairs, string collectionName);

    Task SampleAndStoreTempData(string sourceCollectionName, string tempCollectionName, double maxPairs);
    IAsyncEnumerable<IDictionary<string, object>> GetStreamFromTempCollection(string _tempCollectionName, CancellationToken cancellationToken);
    Task BulkInsertAsync<T>(IEnumerable<T> entity, string collectionName);
    Task<bool> UpdateByFieldAsync<TField>(
    IDictionary<string, object> data, string collectionName, string fieldName, TField fieldValue);

    Task<bool> RenameCollection(string oldName, string newName);
    Task<bool> CollectionExistsAsync(string collectionName);

    Task BulkUpsertAsync<T>(IEnumerable<T> entities, string collectionName);

    // NEW: Bulk upsert with custom match criteria (for composite keys)
    Task BulkUpsertByFieldsAsync<T>(
        IEnumerable<T> entities,
        string collectionName,
        Expression<Func<T, object>>[] matchFields);

    Task CreateGroupFilterIndexesAsync(string collectionName);
}

