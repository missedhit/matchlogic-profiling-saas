using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch
{
    // Per-request MongoDB traffic for Live Search search endpoints was dominated by metadata
    // lookups (data sources, match definitions, index maps) that do not change after startup.
    // This cache consolidates those lookups into a single lazy load per project and hands out
    // immutable snapshots. Registered as a Singleton in the Live Search DI wiring only, so batch
    // paths continue to use their existing scoped services unchanged.
    public interface ILiveSearchMetadataCache
    {
        Task<LiveSearchMetadata> GetAsync(Guid projectId, CancellationToken cancellationToken = default);
        void Invalidate(Guid projectId);
    }

    public sealed class LiveSearchMetadata
    {
        public required Guid ProjectId { get; init; }
        public required IReadOnlyList<DataSource> DataSources { get; init; }
        public required IReadOnlyDictionary<Guid, string> DataSourceNames { get; init; }
        public required IReadOnlyDictionary<Guid, int> DataSourceIndex { get; init; }
        public required IReadOnlyDictionary<int, Guid> DataSourceIdByIndex { get; init; }
        public required MatchDefinitionCollection MatchDefinitions { get; init; }
        public required IReadOnlyDictionary<Guid, int> DefinitionIndex { get; init; }
    }

    public sealed class LiveSearchMetadataCache : ILiveSearchMetadataCache
    {
        // This cache is registered as a Singleton, but the underlying IGenericRepository<,>
        // implementations are Scoped. Inject IServiceScopeFactory instead of the repos directly
        // and create a short-lived scope per lookup to avoid the captive-dependency error
        // ("Cannot consume scoped service ... from singleton ...") that ASP.NET Core raises
        // when a Singleton has Scoped constructor parameters.
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<LiveSearchMetadataCache> _logger;

        private readonly ConcurrentDictionary<Guid, LiveSearchMetadata> _cache = new();
        private readonly ConcurrentDictionary<Guid, SemaphoreSlim> _locks = new();

        public LiveSearchMetadataCache(
            IServiceScopeFactory scopeFactory,
            ILogger<LiveSearchMetadataCache> logger)
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<LiveSearchMetadata> GetAsync(Guid projectId, CancellationToken cancellationToken = default)
        {
            if (_cache.TryGetValue(projectId, out var cached))
                return cached;

            // Double-checked lock per project so concurrent first-hit callers don't all query Mongo.
            var gate = _locks.GetOrAdd(projectId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken);
            try
            {
                if (_cache.TryGetValue(projectId, out cached))
                    return cached;

                _logger.LogInformation("Loading live search metadata for project {ProjectId}", projectId);

                // Create a short-lived scope to resolve the scoped repositories. The scope is
                // disposed at the end of this using-block; the cached snapshot (plain data) lives
                // on as long as the singleton.
                using var scope = _scopeFactory.CreateScope();
                var dataSourceRepo = scope.ServiceProvider
                    .GetRequiredService<IGenericRepository<DataSource, Guid>>();
                var matchDefRepo = scope.ServiceProvider
                    .GetRequiredService<IGenericRepository<MatchDefinitionCollection, Guid>>();

                var dataSources = (await dataSourceRepo.QueryAsync(
                    ds => ds.ProjectId == projectId,
                    Constants.Collections.DataSources)).ToList();

                var matchDefCollection = (await matchDefRepo.QueryAsync(
                    md => md.ProjectId == projectId,
                    Constants.Collections.MatchDefinitionCollection)).FirstOrDefault();

                if (matchDefCollection == null)
                    throw new InvalidOperationException($"No match definition collection found for project {projectId}");

                var idxById = new Dictionary<Guid, int>(dataSources.Count);
                var idxToId = new Dictionary<int, Guid>(dataSources.Count);
                for (int i = 0; i < dataSources.Count; i++)
                {
                    idxById[dataSources[i].Id] = i;
                    idxToId[i] = dataSources[i].Id;
                }

                var defIdx = matchDefCollection.Definitions.ToDictionary(d => d.Id, d => d.UIDefinitionIndex);

                var snapshot = new LiveSearchMetadata
                {
                    ProjectId = projectId,
                    DataSources = dataSources,
                    DataSourceNames = dataSources.ToDictionary(d => d.Id, d => d.Name),
                    DataSourceIndex = idxById,
                    DataSourceIdByIndex = idxToId,
                    MatchDefinitions = matchDefCollection,
                    DefinitionIndex = defIdx,
                };

                _cache[projectId] = snapshot;

                _logger.LogInformation(
                    "Live search metadata cached for project {ProjectId}: {DataSources} data sources, {Definitions} definitions",
                    projectId, dataSources.Count, matchDefCollection.Definitions.Count);

                return snapshot;
            }
            finally
            {
                gate.Release();
            }
        }

        public void Invalidate(Guid projectId)
        {
            _cache.TryRemove(projectId, out _);
        }
    }

    // Lightweight IDataSourceIndexMapper-compatible view over a cached metadata snapshot so Live
    // Search code paths can feed the existing batch comparison service without reinitialising the
    // scoped DataSourceIndexMapper on every request.
    internal sealed class CachedDataSourceIndexMapper : Features.DataMatching.RecordLinkage.IDataSourceIndexMapper
    {
        private readonly LiveSearchMetadata _meta;

        public CachedDataSourceIndexMapper(LiveSearchMetadata meta) => _meta = meta;

        public bool IsInitialized => true;
        public int DataSourceCount => _meta.DataSourceIndex.Count;
        public int DefinitionCount => _meta.DefinitionIndex.Count;

        public Task InitializeAsync(Guid projectId) => Task.CompletedTask;

        public int GetDataSourceIndex(Guid dataSourceId) =>
            _meta.DataSourceIndex.TryGetValue(dataSourceId, out var i)
                ? i
                : throw new KeyNotFoundException($"Data source ID {dataSourceId} not found");

        public int GetDefinitionIndex(Guid definitionId) =>
            _meta.DefinitionIndex.TryGetValue(definitionId, out var i)
                ? i
                : throw new KeyNotFoundException($"Definition ID {definitionId} not found");

        public Guid GetDataSourceId(int index) =>
            _meta.DataSourceIdByIndex.TryGetValue(index, out var id)
                ? id
                : throw new ArgumentOutOfRangeException(nameof(index));

        public Guid GetDefinitionId(int index) => throw new NotImplementedException();

        public bool TryGetDataSourceIndex(Guid dataSourceId, out int index) =>
            _meta.DataSourceIndex.TryGetValue(dataSourceId, out index);

        public bool TryGetDefinitionIndex(Guid definitionId, out int index) =>
            _meta.DefinitionIndex.TryGetValue(definitionId, out index);

        public IReadOnlyCollection<Guid> GetAllDataSourceIds() => _meta.DataSourceIndex.Keys.ToList().AsReadOnly();
        public IReadOnlyCollection<Guid> GetAllDefinitionIds() => _meta.DefinitionIndex.Keys.ToList().AsReadOnly();

        public IReadOnlyDictionary<Guid, int> GetDataSourceIndexMap() => _meta.DataSourceIndex;
        public IReadOnlyDictionary<Guid, int> GetDefinitionIndexMap() => _meta.DefinitionIndex;

        public bool TryGetDataSourceName(Guid dataSourceId, out string dsName)
        {
            return _meta.DataSourceNames.TryGetValue(dataSourceId, out dsName);
        }
    }
}
