using MatchLogic.Application.Interfaces.Migrations;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Core;

public class DataSeedProviderAdapter<T> : IDataSeedProvider<IEntity> where T : IEntity
{
    private readonly IDataSeedProvider<T> _provider;

    public DataSeedProviderAdapter(IDataSeedProvider<T> provider)
    {
        _provider = provider;
    }

    public string GetCollectionName()
    {
        return _provider.GetCollectionName();
    }

    public IEnumerable<IEntity> GetSeedData()
    {
        return _provider.GetSeedData().Cast<IEntity>();
    }
    // Delegate to concrete provider - THIS IS THE KEY FIX
    public Task<bool> IsCollectionEmptyAsync(IDataStore dataStore)
    {
        return _provider.IsCollectionEmptyAsync(dataStore);
    }

    public Task SeedDataAsync(IDataStore dataStore)
    {
        return _provider.SeedDataAsync(dataStore);
    }
}
