using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Persistence;
public interface IGenericRepository<T, TKey> where T : IEntity
{
    Task<T> GetByIdAsync(TKey id, string collectionName);
    Task<List<T>> GetAllAsync(string collectionName);
    Task InsertAsync(T entity, string collectionName);
    Task UpdateAsync(T entity, string collectionName);
    Task DeleteAsync(TKey id, string collectionName);
    Task<List<T>> QueryAsync(Expression<Func<T, bool>> predicate, string collectionName);
    Task<int> DeleteAllAsync(Expression<Func<T, bool>> predicate, string collectionName);

    Task BulkInsertAsync(IEnumerable<T> entity, string collectionName);
}