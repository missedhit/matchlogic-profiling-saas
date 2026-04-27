using MatchLogic.Application.Extensions;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch
{
    /// <summary>
    /// Thread-safe singleton service that manages Q-gram indexes in memory
    /// Updated to use IDataStore abstraction
    /// </summary>
    public class QGramIndexManager : IQGramIndexManager
    {
        private readonly IDataStore _dataStore;
        private readonly IIndexPersistenceService _persistenceService;
        private readonly ILogger<QGramIndexManager> _logger;
        private readonly ConcurrentDictionary<Guid, QGramIndexData> _indexes;
        private readonly SemaphoreSlim _loadLock;

        public QGramIndexManager(
            IDataStore dataStore,
            IIndexPersistenceService persistenceService,
            ILogger<QGramIndexManager> logger)
        {
            _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
            _persistenceService = persistenceService ?? throw new ArgumentNullException(nameof(persistenceService));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _indexes = new ConcurrentDictionary<Guid, QGramIndexData>();
            _loadLock = new SemaphoreSlim(1, 1);
        }

        public bool IsIndexLoaded(Guid projectId)
        {
            return _indexes.ContainsKey(projectId);
        }

        public Task StoreIndexAsync(Guid projectId, QGramIndexData indexData, CancellationToken cancellationToken = default)
        {
            if (indexData == null)
                throw new ArgumentNullException(nameof(indexData));

            indexData.LoadedAt = DateTime.UtcNow;
            _indexes[projectId] = indexData;

            _logger.LogInformation(
                "Index stored in memory for project {ProjectId}: {Records} records, {Memory:F2} MB",
                projectId,
                indexData.TotalRecords,
                indexData.EstimatedMemoryBytes / (1024.0 * 1024.0));

            return Task.CompletedTask;
        }

        public Task<QGramIndexData> GetIndexAsync(Guid projectId, CancellationToken cancellationToken = default)
        {
            _indexes.TryGetValue(projectId, out var indexData);
            return Task.FromResult(indexData);
        }

        public async Task<bool> LoadIndexFromDatabaseAsync(Guid projectId, CancellationToken cancellationToken = default)
        {
            if (_indexes.ContainsKey(projectId))
            {
                _logger.LogInformation("Index already loaded for project {ProjectId}", projectId);
                return true;
            }

            await _loadLock.WaitAsync(cancellationToken);
            try
            {
                // Double-check after acquiring lock
                if (_indexes.ContainsKey(projectId))
                    return true;

                _logger.LogInformation("Loading index from database for project {ProjectId}", projectId);

                var collectionName = $"qgram_index_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";

                // Use IDataStore abstraction
                var indexData = await _persistenceService.LoadIndexAsync(
                    _dataStore,
                    collectionName,
                    cancellationToken);

                if (indexData == null)
                {
                    _logger.LogWarning("No persisted index found for project {ProjectId}", projectId);
                    return false;
                }

                indexData.LoadedAt = DateTime.UtcNow;
                _indexes[projectId] = indexData;

                _logger.LogInformation(
                    "Index loaded successfully for project {ProjectId}: {Records} records, {Memory:F2} MB",
                    projectId,
                    indexData.TotalRecords,
                    indexData.EstimatedMemoryBytes / (1024.0 * 1024.0));

                return true;
            }
            finally
            {
                _loadLock.Release();
            }
        }

        public async Task<bool> WaitForIndexAvailabilityAsync(
            Guid projectId,
            TimeSpan timeout,
            TimeSpan pollInterval,
            CancellationToken cancellationToken = default)
        {
            var startTime = DateTime.UtcNow;
            var attempts = 0;
            var collectionName = $"qgram_index_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";

            _logger.LogInformation(
                "Waiting for index availability for project {ProjectId} (timeout: {Timeout})",
                projectId,
                timeout);

            while (DateTime.UtcNow - startTime < timeout)
            {
                attempts++;

                // Use IDataStore abstraction
                var exists = await _persistenceService.IndexExistsAsync(
                    _dataStore,
                    collectionName,
                    cancellationToken);

                if (exists)
                {
                    _logger.LogInformation(
                        "Index found for project {ProjectId} after {Attempts} attempts ({Duration:F1}s)",
                        projectId,
                        attempts,
                        (DateTime.UtcNow - startTime).TotalSeconds);

                    // Load it immediately
                    return await LoadIndexFromDatabaseAsync(projectId, cancellationToken);
                }

                _logger.LogDebug(
                    "Index not yet available for project {ProjectId} (attempt {Attempts}), waiting {PollInterval}",
                    projectId,
                    attempts,
                    pollInterval);

                await Task.Delay(pollInterval, cancellationToken);
            }

            _logger.LogError(
                "Timeout waiting for index for project {ProjectId} after {Attempts} attempts ({Duration:F1}s)",
                projectId,
                attempts,
                (DateTime.UtcNow - startTime).TotalSeconds);

            return false;
        }

        public Task ClearIndexAsync(Guid projectId)
        {
            _indexes.TryRemove(projectId, out _);
            _logger.LogInformation("Cleared index from memory for project {ProjectId}", projectId);
            return Task.CompletedTask;
        }

        public IndexManagerStatistics GetStatistics()
        {
            var stats = new IndexManagerStatistics
            {
                LoadedProjects = _indexes.Count,
                TotalMemoryMB = (long)(GC.GetTotalMemory(false) / (1024.0 * 1024.0)),
                ProjectDetails = new List<ProjectIndexStats>()
            };

            foreach (var kvp in _indexes)
            {
                stats.ProjectDetails.Add(new ProjectIndexStats
                {
                    ProjectId = kvp.Key,
                    RecordCount = kvp.Value.TotalRecords,
                    LoadedAt = kvp.Value.LoadedAt,
                    Age = DateTime.UtcNow - kvp.Value.LoadedAt
                });
            }

            return stats;
        }
    }
}
