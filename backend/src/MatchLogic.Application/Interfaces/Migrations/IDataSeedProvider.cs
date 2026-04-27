using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Migrations;

public interface IDataSeedProvider<T> where T : IEntity
{
    IEnumerable<T> GetSeedData();
    string GetCollectionName();

    // Add these methods for concrete type handling
    Task<bool> IsCollectionEmptyAsync(IDataStore dataStore);
    Task SeedDataAsync(IDataStore dataStore);
}
