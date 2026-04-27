using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;

/// <summary>
/// Optimized field-specific index structure for billion-record scale
/// </summary>
public class OptimizedFieldIndex : IDisposable
{
    // Separate index by data source for faster filtering
    private readonly ConcurrentDictionary<Guid, Dictionary<uint, List<int>>> _indexByDataSource;
    private readonly ReaderWriterLockSlim _lock;
    private bool _disposed;

    public OptimizedFieldIndex()
    {
        _indexByDataSource = new ConcurrentDictionary<Guid, Dictionary<uint, List<int>>>();
        _lock = new ReaderWriterLockSlim();
    }

    public void AddEntry(uint hash, Guid dataSourceId, int rowNumber)
    {
        var dsIndex = _indexByDataSource.GetOrAdd(dataSourceId,
            _ => new Dictionary<uint, List<int>>());

        _lock.EnterWriteLock();
        try
        {
            if (!dsIndex.TryGetValue(hash, out var list))
            {
                list = new List<int>();
                dsIndex[hash] = list;
            }
            list.Add(rowNumber);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public IEnumerable<uint> GetCommonHashes(Guid sourceId1, Guid sourceId2)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_indexByDataSource.TryGetValue(sourceId1, out var index1))
                return Enumerable.Empty<uint>();

            // For same-source deduplication
            if (sourceId1 == sourceId2)
                return index1.Keys;

            if (!_indexByDataSource.TryGetValue(sourceId2, out var index2))
                return Enumerable.Empty<uint>();

            // Use smaller index for iteration efficiency
            var (smaller, larger) = index1.Count <= index2.Count
                ? (index1.Keys, index2)
                : (index2.Keys, index1);

            return smaller.Where(hash => larger.ContainsKey(hash));
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public bool TryGetRows(Guid dataSourceId, uint hash, out List<int> rows)
    {
        _lock.EnterReadLock();
        try
        {
            rows = null;
            if (_indexByDataSource.TryGetValue(dataSourceId, out var index))
            {
                if (index.TryGetValue(hash, out var list))
                {
                    // Return a copy to avoid thread safety issues
                    rows = new List<int>(list);
                    return true;
                }
            }
            return false;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public int GetHashCountForDataSource(Guid dataSourceId)
    {
        _lock.EnterReadLock();
        try
        {
            return _indexByDataSource.TryGetValue(dataSourceId, out var index) ? index.Count : 0;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public IEnumerable<uint> GetHashesForDataSource(Guid dataSourceId)
    {
        _lock.EnterReadLock();
        try
        {
            if (_indexByDataSource.TryGetValue(dataSourceId, out var index))
                return index.Keys.ToList(); // Return a copy
            return Enumerable.Empty<uint>();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }
    public void Dispose()
    {
        if (!_disposed)
        {
            _lock?.Dispose();
            _disposed = true;
        }
    }
}

/// <summary>
/// Highly optimized QGram indexer for billion-record scale processing
/// </summary>
public class OptimizedProductionQGramIndexer : IProductionQGramIndexer, IDisposable
{
    private readonly QGramIndexerOptions _options;
    private readonly MemoryMappedStoreOptions _memoryMappedStoreOptions;
    private readonly ILogger<OptimizedProductionQGramIndexer> _logger;
    private readonly QGramGenerator _qgramGenerator;

    // Optimized index structure: field -> datasource -> hash -> rows
    private readonly ConcurrentDictionary<string, OptimizedFieldIndex> _fieldIndexes;
    private readonly ConcurrentDictionary<Guid, IRecordStore> _recordStores;
    private readonly ConcurrentDictionary<(Guid, int), RowMetadata> _rowMetadata;
    private readonly IQGramSimilarityStrategy _similarityStrategy;

    // Thread-safe candidate deduplication
    private readonly ConcurrentDictionary<(Guid, int, Guid, int), byte> _globalProcessedPairs;
    private readonly object[] _partitionLocks;
    private const int PARTITION_COUNT = 256;

    private bool _disposed;

    public OptimizedProductionQGramIndexer(
        IOptions<QGramIndexerOptions> options,
        ILogger<OptimizedProductionQGramIndexer> logger)
    {
        _options = options?.Value ?? new QGramIndexerOptions();
        _memoryMappedStoreOptions = new MemoryMappedStoreOptions
        {
            EnableCompression = true,
            MaxCachedPages = 1000,
            PageSize = 8192
        };
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _qgramGenerator = new QGramGenerator(_options.QGramSize);

        _fieldIndexes = new ConcurrentDictionary<string, OptimizedFieldIndex>(StringComparer.OrdinalIgnoreCase);
        _recordStores = new ConcurrentDictionary<Guid, IRecordStore>();
        _rowMetadata = new ConcurrentDictionary<(Guid, int), RowMetadata>();
        _similarityStrategy = QGramSimilarityStrategyFactory.CreateStrategy(_options.SimilarityAlgorithm);

        // Initialize partition locks for parallel deduplication
        //_partitionLocks = new object[PARTITION_COUNT];
        //for (int i = 0; i < PARTITION_COUNT; i++)
        //{
        //    _partitionLocks[i] = new object();
        //}
        _globalProcessedPairs = new ConcurrentDictionary<(Guid, int, Guid, int), byte>();
    }

    #region IProductionQGramIndexer Implementation

    /// <summary>
    /// Index a data source with batch parallel processing
    /// </summary>
    public async Task<IndexingResult> IndexDataSourceAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        DataSourceIndexingConfig config,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(progressTracker);

        var startTime = DateTime.UtcNow;
        var result = new IndexingResult
        {
            DataSourceId = config.DataSourceId,
            DataSourceName = config.DataSourceName,
            IndexedFields = config.FieldsToIndex.ToList()
        };

        // Determine optimal storage based on expected size
        IRecordStore recordStore = config.UseInMemoryStore && config.InMemoryThreshold < 100000
            ? new InMemoryRecordStore(config.InMemoryThreshold)
            : new MemoryMappedRecordStore(config.DataSourceId, _memoryMappedStoreOptions, _logger);

        _recordStores[config.DataSourceId] = recordStore;

        // Initialize field indexes
        foreach (var fieldName in config.FieldsToIndex)
        {
            _fieldIndexes.TryAdd(fieldName, new OptimizedFieldIndex());
        }

        // Process in batches for efficiency
        const int BATCH_SIZE = 5000;
        var batch = new List<(IDictionary<string, object> record, int rowNumber)>(BATCH_SIZE);
        var rowNumber = 0;

        try
        {
            _logger.LogInformation("Starting optimized indexing for {DataSourceName} with {FieldCount} fields",
                config.DataSourceName, config.FieldsToIndex.Count);

            await foreach (var record in records.WithCancellation(cancellationToken))
            {
                batch.Add((record, rowNumber++));

                if (batch.Count >= BATCH_SIZE)
                {
                    await ProcessIndexingBatchAsync(batch, config, recordStore, cancellationToken);
                    result.ProcessedRecords += batch.Count;

                    if (result.ProcessedRecords % 50000 == 0)
                    {
                        await progressTracker.UpdateProgressAsync(result.ProcessedRecords,
                            $"Indexed {result.ProcessedRecords:N0} records from {config.DataSourceName}");
                    }

                    batch.Clear();
                }
            }

            // Process remaining records
            if (batch.Count > 0)
            {
                await ProcessIndexingBatchAsync(batch, config, recordStore, cancellationToken);
                result.ProcessedRecords += batch.Count;
            }

            // Finalize storage
            await recordStore.SwitchToReadOnlyModeAsync();
            var stats = recordStore.GetStatistics();

            result.IndexingDuration = DateTime.UtcNow - startTime;
            result.StorageSizeBytes = stats.TotalSizeBytes;
            result.UsedDiskStorage = stats.StorageType.Contains("Disk") || stats.StorageType.Contains("MemoryMapped");

            _logger.LogInformation("Completed indexing {DataSourceName}: {RecordCount:N0} records in {Duration:F1}s ({Rate:N0} records/sec)",
                config.DataSourceName,
                result.ProcessedRecords,
                result.IndexingDuration.TotalSeconds,
                result.ProcessedRecords / Math.Max(1, result.IndexingDuration.TotalSeconds));

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during indexing for {DataSourceName}", config.DataSourceName);
            throw;
        }
    }

    /// <summary>
    /// Generate candidates from match definitions with optimized processing
    /// </summary>
    public async IAsyncEnumerable<CandidatePair> GenerateCandidatesFromMatchDefinitionsAsync(
        MatchDefinitionCollection matchDefinitions,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(matchDefinitions);

        _logger.LogInformation("Generating candidates from {Count} match definitions",
            matchDefinitions.Definitions.Count);

        // Clear previous processed pairs for new run
        _globalProcessedPairs.Clear();

        var candidateMap = new ConcurrentDictionary<(Guid, int, Guid, int), CandidatePair>();
        var totalCandidates = 0;

        foreach (var definition in matchDefinitions.Definitions)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            _logger.LogDebug("Processing match definition {DefinitionId}: {Name}",
                definition.Id, definition.Name);

            var dataSourcePairs = GetDataSourcePairsFromDefinition(definition);

            foreach (var (sourceId1, sourceId2) in dataSourcePairs)
            {
                if (!_recordStores.TryGetValue(sourceId1, out var store1) ||
                    !_recordStores.TryGetValue(sourceId2, out var store2))
                {
                    _logger.LogWarning("Missing record stores for sources {Source1}, {Source2}",
                        sourceId1, sourceId2);
                    continue;
                }

                await foreach (var candidate in GenerateCandidatesForDefinitionAsync(
                    definition, sourceId1, sourceId2, store1, store2, cancellationToken))
                {
                    var key = GetCandidateKey(candidate);

                    if (candidateMap.TryGetValue(key, out var existingCandidate))
                    {
                        existingCandidate.AddMatchDefinition(definition.Id);
                    }
                    else
                    {
                        candidate.AddMatchDefinition(definition.Id);
                        candidateMap[key] = candidate;
                        totalCandidates++;

                        if (totalCandidates % 10000 == 0)
                        {
                            _logger.LogDebug("Generated {Count} unique candidates so far", totalCandidates);
                        }
                    }
                }
            }
        }

        _logger.LogInformation("Generated {Count} unique candidate pairs", totalCandidates);

        foreach (var candidate in candidateMap.Values)
        {
            yield return candidate;
        }
    }

    /// <summary>
    /// Generate within-source candidates (deduplication)
    /// </summary>
    public async IAsyncEnumerable<CandidatePair> GenerateWithinSourceCandidatesAsync(
        Guid sourceId,
        List<string> fieldNames,
        double minSimilarityThreshold = 0.1,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!_recordStores.TryGetValue(sourceId, out var store))
        {
            _logger.LogError("Record store not found for source {SourceId}", sourceId);
            yield break;
        }

        _globalProcessedPairs.Clear();
        var candidateCount = 0;

        foreach (var fieldName in fieldNames)
        {
            if (!_fieldIndexes.TryGetValue(fieldName, out var fieldIndex))
                continue;

            await foreach (var candidate in ProcessFieldPairOptimizedAsync(
                fieldName, fieldName, sourceId, sourceId, store, store,
                minSimilarityThreshold, null, cancellationToken))
            {
                candidateCount++;
                yield return candidate;

                if (candidateCount % 10000 == 0)
                {
                    _logger.LogDebug("Generated {Count} within-source candidates", candidateCount);
                }
            }
        }

        _logger.LogInformation("Generated {Total} within-source candidates for {SourceId}",
            candidateCount, sourceId);
    }

    /// <summary>
    /// Generate cross-source candidates
    /// </summary>
    public async IAsyncEnumerable<CandidatePair> GenerateCrossSourceCandidatesAsync(
        Guid sourceId1,
        Guid sourceId2,
        List<string> fieldNames,
        double minSimilarityThreshold = 0.1,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!_recordStores.TryGetValue(sourceId1, out var store1) ||
            !_recordStores.TryGetValue(sourceId2, out var store2))
        {
            _logger.LogError("Record stores not found for sources {Source1}, {Source2}", sourceId1, sourceId2);
            yield break;
        }

        _globalProcessedPairs.Clear();
        var candidateCount = 0;

        // Find indexed fields from source2
        var source2IndexedFields = _fieldIndexes.Keys
            .Where(fieldName =>
            {
                if (_fieldIndexes.TryGetValue(fieldName, out var index))
                {
                    return index.GetHashCountForDataSource(sourceId2) > 0;
                }
                return false;
            })
            .ToList();

        if (!source2IndexedFields.Any())
        {
            _logger.LogWarning("No indexed fields found for source {Source2}", sourceId2);
            yield break;
        }

        foreach (var field1Name in fieldNames)
        {
            if (!_fieldIndexes.ContainsKey(field1Name))
                continue;

            foreach (var field2Name in source2IndexedFields)
            {
                var fieldMappings = new Dictionary<string, string> { { field1Name, field2Name } };

                await foreach (var candidate in ProcessFieldPairOptimizedAsync(
                    field1Name, field2Name, sourceId1, sourceId2,
                    store1, store2, minSimilarityThreshold, fieldMappings, cancellationToken))
                {
                    candidateCount++;
                    yield return candidate;

                    if (candidateCount % 10000 == 0)
                    {
                        _logger.LogDebug("Generated {Count} cross-source candidates", candidateCount);
                    }
                }
            }
        }

        _logger.LogInformation("Generated {Total} cross-source candidates", candidateCount);
    }

    public async Task<IDictionary<string, object>> GetRecordAsync(Guid dataSourceId, int rowNumber)
    {
        if (_recordStores.TryGetValue(dataSourceId, out var store))
        {
            return await store.GetRecordAsync(rowNumber);
        }
        return null;
    }

    public async Task<IList<IDictionary<string, object>>> GetRecordsAsync(
        Guid dataSourceId,
        IEnumerable<int> rowNumbers)
    {
        if (_recordStores.TryGetValue(dataSourceId, out var store))
        {
            return await store.GetRecordsAsync(rowNumbers);
        }
        return new List<IDictionary<string, object>>();
    }

    public IndexerStatistics GetStatistics()
    {
        var stats = new IndexerStatistics
        {
            TotalDataSources = _recordStores.Count,
            TotalRecords = _rowMetadata.Count,
            TotalIndexedFields = _fieldIndexes.Count,
            IndexSizeBytes = EstimateIndexSize(),
            DataSources = new List<DataSourceStats>()
        };

        foreach (var store in _recordStores)
        {
            var storeStats = store.Value.GetStatistics();
            stats.DataSources.Add(new DataSourceStats
            {
                DataSourceId = store.Key,
                RecordCount = storeStats.RecordCount,
                StorageSizeBytes = storeStats.TotalSizeBytes,
                StorageType = storeStats.StorageType,
                IsReadOnly = storeStats.IsReadOnly
            });

            stats.TotalStorageSizeBytes += storeStats.TotalSizeBytes;
        }

        return stats;
    }

    public void ClearCaches()
    {
        GC.Collect(2, GCCollectionMode.Forced, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true);
        _logger.LogInformation("Caches cleared and garbage collection completed");
    }

    #endregion

    #region Private Methods

    private async Task ProcessIndexingBatchAsync(
        List<(IDictionary<string, object> record, int rowNumber)> batch,
        DataSourceIndexingConfig config,
        IRecordStore recordStore,
        CancellationToken cancellationToken)
    {
        // Store records first
        var storeTasks = batch.Select(b => recordStore.AddRecordAsync(b.record));
        await Task.WhenAll(storeTasks);

        // Process indexing in parallel
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount,
            CancellationToken = cancellationToken
        };

        await Parallel.ForEachAsync(batch, parallelOptions, async (item, ct) =>
        {
            var metadata = new RowMetadata
            {
                DataSourceId = config.DataSourceId,
                RowNumber = item.rowNumber
            };

            var hashBuffer = _qgramGenerator.RentHashBuffer(1024);
            try
            {
                foreach (var fieldName in config.FieldsToIndex)
                {
                    if (item.record.TryGetValue(fieldName, out var value) &&
                        value is string stringValue &&
                        !string.IsNullOrWhiteSpace(stringValue))
                    {
                        ProcessField(fieldName, stringValue, config.DataSourceId,
                            item.rowNumber, metadata, hashBuffer);
                    }
                }
            }
            finally
            {
                _qgramGenerator.ReturnHashBuffer(hashBuffer);
            }

            _rowMetadata[(config.DataSourceId, item.rowNumber)] = metadata;
            await Task.CompletedTask;
        });
    }

    private void ProcessField(
        string fieldName,
        string fieldValue,
        Guid dataSourceId,
        int rowNumber,
        RowMetadata metadata,
        uint[] hashBuffer)
    {
        _qgramGenerator.GenerateHashes(fieldValue.AsSpan(), hashBuffer, out int hashCount);
        var fieldHashes = new HashSet<uint>(hashCount);
        var fieldIndex = _fieldIndexes[fieldName];

        for (int i = 0; i < hashCount; i++)
        {
            var hash = hashBuffer[i];
            fieldHashes.Add(hash);
            fieldIndex.AddEntry(hash, dataSourceId, rowNumber);
        }

        metadata.FieldHashes[fieldName] = fieldHashes;
    }

    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesForDefinitionAsync(
        MatchLogic.Domain.Entities.MatchDefinition definition,
        Guid sourceId1,
        Guid sourceId2,
        IRecordStore store1,
        IRecordStore store2,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var fieldMappings = GetFieldMappingsForSourcePair(definition, sourceId1, sourceId2);

        if (!fieldMappings.Any())
        {
            _logger.LogWarning("No field mappings found for definition {DefinitionId}", definition.Id);
            yield break;
        }

        // Single criteria - fast path
        if (definition.Criteria.Count == 1)
        {
            var criteria = definition.Criteria.First();
            var minThreshold = GetThresholdForCriteria(criteria);

            await foreach (var candidate in GenerateCandidatesForCriteriaAsync(
                criteria, sourceId1, sourceId2, store1, store2,
                minThreshold, cancellationToken))
            {
                yield return candidate;
            }
            yield break;
        }

        // Multiple criteria - use most selective first
        var orderedCriteria = definition.Criteria
            .OrderByDescending(c => c.MatchingType == MatchingType.Exact ? 2 : 0)
            .ThenByDescending(c => GetThresholdForCriteria(c))
            .ToList();

        var mostSelectiveCriteria = orderedCriteria.First();
        var otherCriteria = orderedCriteria.Skip(1).ToList();
        var mostSelectiveThreshold = GetThresholdForCriteria(mostSelectiveCriteria);

        await foreach (var candidate in GenerateCandidatesForCriteriaAsync(
            mostSelectiveCriteria, sourceId1, sourceId2, store1, store2,
            mostSelectiveThreshold, cancellationToken))
        {
            bool satisfiesAllCriteria = true;

            foreach (var criteria in otherCriteria)
            {
                if (!ValidateCandidateAgainstCriteria(candidate, criteria, sourceId1, sourceId2))
                {
                    satisfiesAllCriteria = false;
                    break;
                }
            }

            if (satisfiesAllCriteria)
            {
                yield return candidate;
            }
        }
    }

    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesForCriteriaAsync(
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2,
        IRecordStore store1,
        IRecordStore store2,
        double minThreshold,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        List<string> source1Fields;
        List<string> source2Fields;

        if (sourceId1 == sourceId2)
        {
            var fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1 && _fieldIndexes.ContainsKey(m.FieldName))
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();

            source1Fields = fields;
            source2Fields = fields;
        }
        else
        {
            source1Fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1 && _fieldIndexes.ContainsKey(m.FieldName))
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();

            source2Fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId2 && _fieldIndexes.ContainsKey(m.FieldName))
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();
        }

        if (!source1Fields.Any() || !source2Fields.Any())
            yield break;

        // Build field mappings
        Dictionary<string, string> fieldMappings = new Dictionary<string, string>();

        if (sourceId1 != sourceId2)
        {
            foreach (var field1 in source1Fields)
            {
                foreach (var field2 in source2Fields)
                {
                    fieldMappings[field1] = field2;
                }
            }
        }
        else
        {
            foreach (var field in source1Fields)
            {
                fieldMappings[field] = field;
            }
        }

        // Process field pairs
        foreach (var field1 in source1Fields)
        {
            foreach (var field2 in source2Fields)
            {
                if (sourceId1 == sourceId2 && field1 != field2)
                    continue; // Skip different fields for same-source

                await foreach (var candidate in ProcessFieldPairOptimizedAsync(
                    field1, field2, sourceId1, sourceId2,
                    store1, store2, minThreshold, fieldMappings, cancellationToken))
                {
                    yield return candidate;
                }
            }
        }
    }

    private async IAsyncEnumerable<CandidatePair> ProcessFieldPairOptimizedAsync(
        string field1Name,
        string field2Name,
        Guid sourceId1,
        Guid sourceId2,
        IRecordStore store1,
        IRecordStore store2,
        double minThreshold,
        Dictionary<string, string> fieldMappings,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!_fieldIndexes.TryGetValue(field1Name, out var fieldIndex1))
            yield break;

        OptimizedFieldIndex fieldIndex2;
        if (field1Name == field2Name)
        {
            fieldIndex2 = fieldIndex1;
        }
        else if (!_fieldIndexes.TryGetValue(field2Name, out fieldIndex2))
        {
            yield break;
        }

        // This is the critical fix - we need to get rows for each source from the appropriate index
        var outputChannel = Channel.CreateBounded<CandidatePair>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        var processingTask = Task.Run(async () =>
        {
            try
            {
                // Get all hashes from source1 in field1
                var source1Hashes = fieldIndex1.GetHashesForDataSource(sourceId1);

                foreach (var hash in source1Hashes)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    // Get rows from source1 for this hash
                    if (!fieldIndex1.TryGetRows(sourceId1, hash, out var rows1))
                        continue;

                    // Get rows from source2 for this hash (might be from different field)
                    List<int> rows2;
                    if (sourceId1 == sourceId2)
                    {
                        rows2 = rows1; // Same source deduplication
                    }
                    else
                    {
                        if (!fieldIndex2.TryGetRows(sourceId2, hash, out rows2))
                            continue;
                    }

                    // Generate pairs based on source relationship
                    await GeneratePairsFromRows(
                        rows1, rows2, sourceId1, sourceId2,
                        store1, store2, minThreshold, fieldMappings,
                        outputChannel.Writer, cancellationToken);
                }
            }
            finally
            {
                outputChannel.Writer.Complete();
            }
        }, cancellationToken);

        await foreach (var candidate in outputChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return candidate;
        }

        await processingTask;
    }
    
    private async Task GeneratePairsFromRows(
List<int> rows1, List<int> rows2,
Guid sourceId1, Guid sourceId2,
IRecordStore store1, IRecordStore store2,
double minThreshold,
Dictionary<string, string> fieldMappings,
ChannelWriter<CandidatePair> writer,
CancellationToken cancellationToken)
    {
        if (sourceId1 == sourceId2)
        {
            // Within-source deduplication
            for (int i = 0; i < rows1.Count; i++)
            {
                for (int j = i + 1; j < rows1.Count; j++)
                {
                    var candidate = CreateCandidateIfValid(
                        sourceId1, rows1[i], sourceId1, rows1[j],
                        store1, store1, minThreshold, fieldMappings);

                    if (candidate != null)
                    {
                        await writer.WriteAsync(candidate, cancellationToken);
                    }
                }
            }
        }
        else
        {
            // Cross-source matching
            foreach (var row1 in rows1)
            {
                foreach (var row2 in rows2)
                {
                    var candidate = CreateCandidateIfValid(
                        sourceId1, row1, sourceId2, row2,
                        store1, store2, minThreshold, fieldMappings);

                    if (candidate != null)
                    {
                        await writer.WriteAsync(candidate, cancellationToken);
                    }
                }
            }
        }
    }

    private async Task ProcessHashBatchAsync(
        uint[] hashes,
        OptimizedFieldIndex fieldIndex1,
        OptimizedFieldIndex fieldIndex2,
        Guid sourceId1,
        Guid sourceId2,
        IRecordStore store1,
        IRecordStore store2,
        double minThreshold,
        Dictionary<string, string> fieldMappings,
        ChannelWriter<CandidatePair> writer,
        CancellationToken cancellationToken)
    {
        foreach (var hash in hashes)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            if (!fieldIndex1.TryGetRows(sourceId1, hash, out var rows1) ||
                !fieldIndex2.TryGetRows(sourceId2, hash, out var rows2))
                continue;

            // Apply bucket size limits
            if (_options.BucketStrategy != BucketOptimizationStrategy.None)
            {
                if (rows1.Count > _options.MaxRecordsPerHashBucket ||
                    rows2.Count > _options.MaxRecordsPerHashBucket)
                {
                    if (_options.BucketStrategy == BucketOptimizationStrategy.Skip)
                        continue;

                    if (_options.BucketStrategy == BucketOptimizationStrategy.Sample)
                    {
                        var sampleSize = (int)(_options.MaxRecordsPerHashBucket * _options.BucketSamplingRate);
                        if (rows1.Count > sampleSize)
                            rows1 = rows1.OrderBy(x => x.GetHashCode()).Take(sampleSize).ToList();
                        if (rows2.Count > sampleSize)
                            rows2 = rows2.OrderBy(x => x.GetHashCode()).Take(sampleSize).ToList();
                    }
                }
            }

            // Generate pairs
            if (sourceId1 == sourceId2)
            {
                // Within-source deduplication
                for (int i = 0; i < rows1.Count - 1; i++)
                {
                    int candidatesForThisRecord = 0;
                    for (int j = i + 1; j < rows1.Count && candidatesForThisRecord < _options.MaxCandidatesPerRecord; j++)
                    {
                        var candidate = CreateCandidateIfValid(
                            sourceId1, rows1[i], sourceId1, rows1[j],
                            store1, store1, minThreshold, fieldMappings);

                        if (candidate != null)
                        {
                            await writer.WriteAsync(candidate, cancellationToken);
                            candidatesForThisRecord++;
                        }
                    }
                }
            }
            else
            {
                // Cross-source matching
                foreach (var row1 in rows1)
                {
                    int candidatesForThisRecord = 0;
                    foreach (var row2 in rows2)
                    {
                        if (candidatesForThisRecord >= _options.MaxCandidatesPerRecord)
                            break;

                        var candidate = CreateCandidateIfValid(
                            sourceId1, row1, sourceId2, row2,
                            store1, store2, minThreshold, fieldMappings);

                        if (candidate != null)
                        {
                            await writer.WriteAsync(candidate, cancellationToken);
                            candidatesForThisRecord++;
                        }
                    }
                }
            }
        }
    }

    private CandidatePair CreateCandidateIfValid(
        Guid sourceId1, int row1,
        Guid sourceId2, int row2,
        IRecordStore store1, IRecordStore store2,
        double minThreshold,
        Dictionary<string, string> fieldMappings)
    {
        // Create canonical key for deduplication
        var key = (sourceId1.CompareTo(sourceId2) < 0 ||
                  (sourceId1 == sourceId2 && row1 < row2))
            ? (sourceId1, row1, sourceId2, row2)
            : (sourceId2, row2, sourceId1, row1);

        // Skip self-pairs
        if (key.Item1 == key.Item3 && key.Item2 == key.Item4)
            return null;

        // Thread-safe deduplication check
        if (!_globalProcessedPairs.TryAdd(key, 0))
            return null;

        // Calculate similarity if threshold is meaningful
        var similarity = 0.0;
        if (minThreshold > 0)
        {
            similarity = CalculateRowSimilarity(
                (sourceId1, row1), (sourceId2, row2), fieldMappings);

            if (similarity < minThreshold)
            {
                _globalProcessedPairs.TryRemove(key, out _);
                return null;
            }
        }

        return new CandidatePair(
            key.Item1, key.Item2, key.Item3, key.Item4,
            store1, store2, similarity);
    }

    private bool ValidateCandidateAgainstCriteria(
        CandidatePair candidate,
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2)
    {
        // Skip validation for numeric and phonetic types (as per your fix)
        if (criteria.DataType == CriteriaDataType.Number ||
            criteria.DataType == CriteriaDataType.Phonetic)
            return true;

        var key1 = (candidate.DataSource1Id, candidate.Row1Number);
        var key2 = (candidate.DataSource2Id, candidate.Row2Number);

        if (!_rowMetadata.TryGetValue(key1, out var metadata1) ||
            !_rowMetadata.TryGetValue(key2, out var metadata2))
        {
            return false;
        }

        var threshold = GetThresholdForCriteria(criteria);

        if (criteria.MatchingType == MatchingType.Exact && threshold >= 0.99)
        {
            return ValidateExactMatch(metadata1, metadata2, criteria, sourceId1, sourceId2);
        }

        return ValidateFuzzyMatch(metadata1, metadata2, criteria, sourceId1, sourceId2, threshold);
    }

    private bool ValidateExactMatch(
        RowMetadata metadata1,
        RowMetadata metadata2,
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2)
    {
        var fieldMappings = BuildFieldMappingsForCriteria(criteria, sourceId1, sourceId2);

        if (!fieldMappings.Any())
            return false;

        foreach (var mapping in fieldMappings)
        {
            if (!metadata1.FieldHashes.TryGetValue(mapping.Key, out var hashes1) ||
                !metadata2.FieldHashes.TryGetValue(mapping.Value, out var hashes2))
            {
                return false;
            }

            double similarity;
            if (hashes1.Count == 0 && hashes2.Count == 0)
            {
                similarity = 1.0;
            }
            else if (hashes1.Count == 0 || hashes2.Count == 0)
            {
                return false;
            }
            else
            {
                similarity = _similarityStrategy.CalculateSimilarity(hashes1, hashes2);
            }

            if (similarity < 0.99)
            {
                return false;
            }
        }

        return true;
    }

    private bool ValidateFuzzyMatch(
        RowMetadata metadata1,
        RowMetadata metadata2,
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2,
        double threshold)
    {
        var fieldMappings = BuildFieldMappingsForCriteria(criteria, sourceId1, sourceId2);

        if (!fieldMappings.Any())
            return false;

        double totalSimilarity = 0;
        int fieldCount = 0;

        foreach (var mapping in fieldMappings)
        {
            if (!metadata1.FieldHashes.TryGetValue(mapping.Key, out var hashes1) ||
                !metadata2.FieldHashes.TryGetValue(mapping.Value, out var hashes2))
            {
                return false;
            }

            if (hashes1.Count == 0 && hashes2.Count == 0)
            {
                totalSimilarity += 1.0;
            }
            else if (hashes1.Count > 0 || hashes2.Count > 0)
            {
                totalSimilarity += _similarityStrategy.CalculateSimilarity(hashes1, hashes2);
            }
            fieldCount++;
        }

        if (fieldCount == 0)
            return false;

        double avgSimilarity = totalSimilarity / fieldCount;
        return avgSimilarity >= threshold;
    }

    private Dictionary<string, string> BuildFieldMappingsForCriteria(
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2)
    {
        var fieldMappings = new Dictionary<string, string>();

        if (sourceId1 == sourceId2)
        {
            var fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1)
                .Select(m => m.FieldName)
                .Distinct();

            foreach (var field in fields)
            {
                if (_fieldIndexes.ContainsKey(field))
                {
                    fieldMappings[field] = field;
                }
            }
        }
        else
        {
            var source1Fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1)
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();

            var source2Fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId2)
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();

            foreach (var field1 in source1Fields)
            {
                foreach (var field2 in source2Fields)
                {
                    if (_fieldIndexes.ContainsKey(field1) &&
                        _fieldIndexes.ContainsKey(field2))
                    {
                        fieldMappings[field1] = field2;
                    }
                }
            }
        }

        return fieldMappings;
    }

    private double CalculateRowSimilarity(
        (Guid sourceId, int row) row1,
        (Guid sourceId, int row) row2,
        Dictionary<string, string> fieldMappings)
    {
        if (!_rowMetadata.TryGetValue(row1, out var metadata1) ||
            !_rowMetadata.TryGetValue(row2, out var metadata2))
            return 0.0;

        var fieldsToCompare = new List<(string, string)>();

        if (fieldMappings?.Any() == true)
        {
            foreach (var mapping in fieldMappings)
            {
                if (metadata1.FieldHashes.ContainsKey(mapping.Key) &&
                    metadata2.FieldHashes.ContainsKey(mapping.Value))
                {
                    fieldsToCompare.Add((mapping.Key, mapping.Value));
                }
            }
        }
        else
        {
            var commonFields = metadata1.FieldHashes.Keys.Intersect(metadata2.FieldHashes.Keys);
            foreach (var field in commonFields)
            {
                fieldsToCompare.Add((field, field));
            }
        }

        if (!fieldsToCompare.Any())
            return 0.0;

        double totalSimilarity = 0;
        foreach (var (field1, field2) in fieldsToCompare)
        {
            var hashes1 = metadata1.FieldHashes[field1];
            var hashes2 = metadata2.FieldHashes[field2];

            if (hashes1.Count == 0 && hashes2.Count == 0)
            {
                totalSimilarity += 1.0;
            }
            else if (hashes1.Count > 0 && hashes2.Count > 0)
            {
                totalSimilarity += _similarityStrategy.CalculateSimilarity(hashes1, hashes2);
            }
        }

        return totalSimilarity / fieldsToCompare.Count;
    }

    private (Guid, int, Guid, int) GetCandidateKey(CandidatePair candidate)
    {
        var row1 = new PairItemRowReference(candidate.DataSource1Id, candidate.Row1Number);
        var row2 = new PairItemRowReference(candidate.DataSource2Id, candidate.Row2Number);

        return row1.CompareTo(row2) <= 0
            ? (candidate.DataSource1Id, candidate.Row1Number, candidate.DataSource2Id, candidate.Row2Number)
            : (candidate.DataSource2Id, candidate.Row2Number, candidate.DataSource1Id, candidate.Row1Number);
    }

    private List<(Guid, Guid)> GetDataSourcePairsFromDefinition(MatchLogic.Domain.Entities.MatchDefinition definition)
    {
        var dataSourceIds = definition.Criteria
            .SelectMany(c => c.FieldMappings)
            .Select(m => m.DataSourceId)
            .Distinct()
            .ToList();

        var pairs = new List<(Guid, Guid)>();

        if (dataSourceIds.Count == 1)
        {
            pairs.Add((dataSourceIds[0], dataSourceIds[0]));
        }
        else if (dataSourceIds.Count == 2)
        {
            pairs.Add((dataSourceIds[0], dataSourceIds[1]));
        }
        else if (dataSourceIds.Count > 2)
        {
            for (int i = 0; i < dataSourceIds.Count; i++)
            {
                for (int j = i; j < dataSourceIds.Count; j++)
                {
                    pairs.Add((dataSourceIds[i], dataSourceIds[j]));
                }
            }
        }

        return pairs;
    }

    private List<FieldMapping> GetFieldMappingsForSourcePair(
        MatchLogic.Domain.Entities.MatchDefinition definition,
        Guid sourceId1,
        Guid sourceId2)
    {
        var mappings = new List<FieldMapping>();

        foreach (var criteria in definition.Criteria)
        {
            var source1Mappings = criteria.FieldMappings.Where(m => m.DataSourceId == sourceId1);
            var source2Mappings = criteria.FieldMappings.Where(m => m.DataSourceId == sourceId2);

            mappings.AddRange(source1Mappings);
            if (sourceId1 != sourceId2)
            {
                mappings.AddRange(source2Mappings);
            }
        }

        return mappings.Distinct().ToList();
    }

    private double GetThresholdForCriteria(MatchCriteria criteria)
    {
        if (criteria.MatchingType == MatchingType.Exact && criteria.DataType != CriteriaDataType.Phonetic)
            return 0.99;

        if (criteria.Arguments.TryGetValue(ArgsValue.FastLevel, out var thresholdStr) &&
            double.TryParse(thresholdStr, out var threshold))
        {
            return threshold;
        }

        return criteria.DataType switch
        {
            CriteriaDataType.Text => 0.7,
            CriteriaDataType.Number => -0.3,
            CriteriaDataType.Phonetic => -0.1,
            _ => 0.7
        };
    }

    private long EstimateIndexSize()
    {
        long size = 0;
        foreach (var fieldIndex in _fieldIndexes)
        {
            size += fieldIndex.Key.Length * 2;
            size += 100000; // Estimate for index structure
        }
        return size;
    }

    #endregion

    #region IDisposable

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            foreach (var index in _fieldIndexes.Values)
            {
                index?.Dispose();
            }

            foreach (var store in _recordStores.Values)
            {
                store?.Dispose();
            }

            _qgramGenerator?.Dispose();
            _fieldIndexes.Clear();
            _recordStores.Clear();
            _rowMetadata.Clear();
            _globalProcessedPairs.Clear();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during disposal");
        }
    }
    public Interfaces.LiveSearch.QGramIndexData BuildIndexDataForPersistence(Guid projectId)
    {
        throw new NotImplementedException();
    }

    public void LoadIndexDataFromPersistence(Interfaces.LiveSearch.QGramIndexData indexData)
    {
        throw new NotImplementedException();
    }

    public Task<List<CandidatePair>> GenerateCandidatesForSingleRecordAsync(Guid projectId, Guid newRecordDataSourceId, IDictionary<string, object> newRecord, MatchDefinitionCollection matchDefinitions, double minSimilarityThreshold = 0.1, int maxCandidates = 1000, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public void RegisterRecordStore(Guid dataSourceId, IRecordStore store)
    {
        throw new NotImplementedException();
    }

    public IRecordStore GetRecordStore(Guid dataSourceId)
    {
        throw new NotImplementedException();
    }

    #endregion
}