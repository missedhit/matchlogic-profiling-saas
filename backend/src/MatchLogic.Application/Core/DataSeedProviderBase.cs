using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Application.Core;

public abstract class DataSeedProviderBase<T> : IDataSeedProvider<T> where T : IEntity
{
    public abstract string GetCollectionName();
    public abstract IEnumerable<T> GetSeedData();

    public async Task<bool> IsCollectionEmptyAsync(IDataStore dataStore)
    {
        // Uses concrete type T - no polymorphism issues
        var items = await dataStore.GetAllAsync<T>(GetCollectionName());
        return items == null || !items.Any();
    }

    public async Task SeedDataAsync(IDataStore dataStore)
    {
        var seedData = GetSeedData();
        foreach (var item in seedData)
        {
            // Uses concrete type T - no polymorphism issues
            await dataStore.InsertAsync(item, GetCollectionName());
        }
    }
}