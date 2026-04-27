using MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.LiveSearch;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;

/// <summary>
/// SIMPLIFIED FIX: Uses Guid.Empty convention for cross-datasource blocks
/// Key insight: When DataSourceId = Guid.Empty, the block is cross-datasource
/// This maintains single BlockKey struct and avoids compilation issues
/// </summary>
public class ProductionQGramIndexerDME : IProductionQGramIndexer, IDisposable
{
    private readonly QGramIndexerWithBlockingOptions _options;
    private readonly ILogger<ProductionQGramIndexerDME> _logger;
    private readonly QGramGenerator _qgramGenerator;
    private readonly IQGramSimilarityStrategy _similarityStrategy;

    // Record storage
    private readonly ConcurrentDictionary<Guid, IRecordStore> _recordStores = new();
    private readonly ConcurrentDictionary<(Guid, int), RowMetadataDME> _rowMetadata = new();

    // Blocking strategy per definition
    private readonly Dictionary<Guid, BlockingStrategy> _definitionBlockingStrategies = new();

    // Global per-field inverted index and DF
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<uint, PostingList>> _globalFieldIndex
        = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<uint, int>> _globalFieldDf
        = new(StringComparer.OrdinalIgnoreCase);

    private MatchDefinitionCollection _matchDefinitions;
    private bool _sealedGlobalIndex;
    private bool _disposed;

    // Guards the lazy first-hit init in GenerateCandidatesForSingleRecordAsync (singleton hit
    // concurrently by all live-search requests on a query node).
    private readonly object _initLock = new();

    // OPTIMIZED: Sharded pair tracking for less contention
    private const int SHARD_COUNT = 64;
    private readonly ConcurrentDictionary<Guid, ProcessedPairs64[]> _seenPairsByDefinition = new();

    public ProductionQGramIndexerDME(
        IOptions<QGramIndexerWithBlockingOptions> options,
        ILogger<ProductionQGramIndexerDME> logger)
    {
        _options = PerformanceTunedOptionsFactory.CreateBalanced();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _qgramGenerator = new QGramGenerator(_options.QGramSize);
        _similarityStrategy = QGramSimilarityStrategyFactory.CreateStrategy(_options.SimilarityAlgorithm);
    }

    #region Public API

    public void InitializeBlockingConfiguration(MatchDefinitionCollection matchDefinitions)
    {
        _matchDefinitions = matchDefinitions ?? throw new ArgumentNullException(nameof(matchDefinitions));

        _definitionBlockingStrategies.Clear();
        _seenPairsByDefinition.Clear();

        if (!_options.EnableBlocking)
        {
            _logger.LogInformation("Blocking disabled; will use global inverted index only.");
            return;
        }

        foreach (var def in matchDefinitions.Definitions)
        {
            var strategy = DetermineBlockingStrategy(def);
            _definitionBlockingStrategies[def.Id] = strategy;

            // Initialize sharded trackers
            var shards = new ProcessedPairs64[SHARD_COUNT];
            for (int i = 0; i < SHARD_COUNT; i++)
                shards[i] = new ProcessedPairs64();
            _seenPairsByDefinition[def.Id] = shards;

            if (_options.LogBlockingDecisions)
            {
                _logger.LogInformation("Def {DefId}: UseBlocking={Use}, Strategy={Strategy}, Fields=[{Fields}]",
                    def.Id, strategy.UseBlocking, strategy.StrategyType,
                    string.Join(", ", strategy.BlockingFields.Select(x =>
                        $"{x.DataSourceId.ToString("N").Substring(0, 8)}:{x.FieldName}")));
            }
        }
    }

    public async Task<IndexingResult> IndexDataSourceAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        DataSourceIndexingConfig config,
        IStepProgressTracker progressTracker,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(progressTracker);

        var start = DateTime.UtcNow;
        var result = new IndexingResult
        {
            DataSourceId = config.DataSourceId,
            DataSourceName = config.DataSourceName,
            IndexedFields = config.FieldsToIndex.ToList()
        };

        IRecordStore recordStore = config.UseInMemoryStore
                                       ? new InMemoryRecordStore(config.InMemoryThreshold)
                : new MemoryMappedRecordStore(config.DataSourceId, new MemoryMappedStoreOptions(), _logger);

        _recordStores[config.DataSourceId] = recordStore;

        var hashBuffer = _qgramGenerator.RentHashBuffer(1024);
        int rowNumber = 0;

        try
        {
            await foreach (var record in records.WithCancellation(cancellationToken))
            {
                await recordStore.AddRecordAsync(record);

                var meta = new RowMetadataDME
                {
                    DataSourceId = config.DataSourceId,
                    RowNumber = rowNumber
                };

                foreach (var field in config.FieldsToIndex)
                {
                    if (!record.TryGetValue(field, out var raw)) continue;
                    var text = Convert.ToString(raw) ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(text)) continue;

                    _qgramGenerator.GenerateHashes(System.Text.RegularExpressions.Regex.Replace(text.ToUpper(), "[^0-9a-zA-Z]", "").AsSpan(), hashBuffer, out int count);
                    var set = new HashSet<uint>();
                    for (int i = 0; i < count; i++) set.Add(hashBuffer[i]);
                    if (set.Count > 0) meta.FieldHashes[field] = set;

                    if (_options.EnableBlocking && IsFieldUsedForBlocking(config.DataSourceId, field))
                    {
                        meta.BlockingValues[field] = NormalizeForBlocking(text);
                    }

                    if (_matchDefinitions != null && set.Count > 0)
                    {
                        var fldIdx = _globalFieldIndex.GetOrAdd(field, _ => new());
                        var fldDf = _globalFieldDf.GetOrAdd(field, _ => new());

                        foreach (var g in set)
                        {
                            var pl = fldIdx.GetOrAdd(g, _ => new PostingList());
                            pl.Add(config.DataSourceId, rowNumber);
                            fldDf.AddOrUpdate(g, 1, (_, v) => v + 1);
                        }
                    }
                }

                _rowMetadata[(config.DataSourceId, rowNumber)] = meta;
                rowNumber++;
                result.ProcessedRecords++;

                if (rowNumber % _options.IndexSaveFrequency == 0)
                {
                    await progressTracker.UpdateProgressAsync(rowNumber, $"Indexed {rowNumber:N0} {config.DataSourceName}");
                }
            }

            await recordStore.SwitchToReadOnlyModeAsync();

            //if (!_sealedGlobalIndex)
            //{
            //    foreach (var fld in _globalFieldIndex.Values)
            //        foreach (var pl in fld.Values) pl.Seal();
            //    _sealedGlobalIndex = true;
            //}

            var stats = recordStore.GetStatistics();
            result.IndexingDuration = DateTime.UtcNow - start;
            result.StorageSizeBytes = stats.TotalSizeBytes;

            _logger.LogInformation("Indexed {Records:N0} rows from {Name} in {Sec:F1}s",
                result.ProcessedRecords, config.DataSourceName, result.IndexingDuration.TotalSeconds);

            return result;
        }
        finally
        {
            _qgramGenerator.ReturnHashBuffer(hashBuffer);
        }
    }

    /// <summary>
    /// Seals all PostingLists after ALL datasources have been indexed.
    /// MUST be called after indexing all datasources and BEFORE generating candidates.
    /// </summary>
    public void SealGlobalIndex()
    {
        if (_sealedGlobalIndex)
        {
            _logger.LogWarning("Global index already sealed");
            return;
        }

        _logger.LogInformation("Sealing global index with {FieldCount} fields", _globalFieldIndex.Count);

        foreach (var fld in _globalFieldIndex.Values)
        {
            foreach (var pl in fld.Values)
            {
                pl.Seal();
            }
        }

        _sealedGlobalIndex = true;

        _logger.LogInformation("Global index sealed successfully");
    }
    public async IAsyncEnumerable<CandidatePair> GenerateCandidatesFromMatchDefinitionsAsync(
        MatchDefinitionCollection matchDefinitions,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_matchDefinitions == null)
            InitializeBlockingConfiguration(matchDefinitions);

        // Clear per-definition sharded trackers
        foreach (var shards in _seenPairsByDefinition.Values)
            foreach (var shard in shards)
                shard.Clear();

        _logger.LogInformation("=== Candidate Generation Started: {N} definitions ===",
            matchDefinitions.Definitions.Count);

        var overallStart = DateTime.UtcNow;
        long totalCandidates = 0;

        foreach (var def in matchDefinitions.Definitions)
        {
            var defStart = DateTime.UtcNow;
            long defCandidates = 0;

            _logger.LogInformation("Processing Definition: {DefId} - {DefName}", def.Id, def.Name);

            var pairs = GetDataSourcePairsFromDefinition(def);
            _definitionBlockingStrategies.TryGetValue(def.Id, out var strategy);

            foreach (var (s1, s2) in pairs)
            {
                if (!_recordStores.TryGetValue(s1, out var store1) ||
                    !_recordStores.TryGetValue(s2, out var store2))
                {
                    _logger.LogWarning("  Missing stores for {S1} or {S2}", s1, s2);
                    continue;
                }

                //  FIX: Get fuzzy criteria (with their field mappings)
                var fuzzyCriteriaList = def.Criteria
                    .Where(c => c.MatchingType == MatchingType.Fuzzy && c.DataType == CriteriaDataType.Text)
                    .Where(c => c.FieldMappings.Any(m => m.DataSourceId == s1 || m.DataSourceId == s2))
                    .ToList();

                if (fuzzyCriteriaList.Count == 0)
                {
                    await foreach (var c in GenerateExactOnlyCandidatesAsync(def, s1, s2, store1, store2, cancellationToken))
                    {
                        totalCandidates++;
                        defCandidates++;
                        yield return c;
                    }
                    continue;
                }

                foreach (var fuzzyCriteria in fuzzyCriteriaList)
                {
                    long fieldCandidates = 0;

                    //  FIX: Get field mappings for this criteria (filter to s1/s2)
                    var relevantMappings = fuzzyCriteria.FieldMappings
                        .Where(m => m.DataSourceId == s1 || m.DataSourceId == s2)
                        .ToList();

                    if (strategy?.UseBlocking == true)
                    {
                        await foreach (var c in GenerateCandidatesWithBlockingAsync(
                            def, strategy, relevantMappings, s1, s2, store1, store2, cancellationToken))
                        {
                            totalCandidates++;
                            defCandidates++;
                            fieldCandidates++;
                            yield return c;
                        }
                    }
                    else
                    {
                        await foreach (var c in GenerateCandidatesWithGlobalProgressiveAsync(
                            def, relevantMappings, s1, s2, store1, store2, cancellationToken))
                        {
                            totalCandidates++;
                            defCandidates++;
                            fieldCandidates++;
                            yield return c;
                        }
                    }

                    if (_options.LogBlockingDecisions && fieldCandidates > 0)
                    {
                        _logger.LogDebug("Generated {Count} candidates for criteria (DS {S1} / {S2})",
                            fieldCandidates, s1.ToString().Substring(0, 8), s2.ToString().Substring(0, 8));
                    }
                }
            }

            _logger.LogInformation("Definition {DefId} completed: {Count:N0} candidates in {Seconds:F1}s",
                def.Id, defCandidates, (DateTime.UtcNow - defStart).TotalSeconds);
        }

        var overallDuration = DateTime.UtcNow - overallStart;
        _logger.LogInformation(
            "=== Candidate Generation Completed ===\n" +
            "  Total Candidates: {Total:N0}\n" +
            "  Total Duration: {Duration:F1}s\n" +
            "  Throughput: {Throughput:N0} candidates/sec",
            totalCandidates,
            overallDuration.TotalSeconds,
            totalCandidates / Math.Max(1, overallDuration.TotalSeconds));
    }

    public async Task<IDictionary<string, object>> GetRecordAsync(Guid dataSourceId, int rowNumber)
    {
        if (_recordStores.TryGetValue(dataSourceId, out var store))
            return await store.GetRecordAsync(rowNumber);
        return null;
    }

    public async Task<IList<IDictionary<string, object>>> GetRecordsAsync(Guid dataSourceId, IEnumerable<int> rowNumbers)
    {
        if (_recordStores.TryGetValue(dataSourceId, out var store))
            return await store.GetRecordsAsync(rowNumbers);
        return new List<IDictionary<string, object>>();
    }

    public IndexerStatistics GetStatistics()
    {
        var stats = new IndexerStatistics
        {
            TotalDataSources = _recordStores.Count,
            TotalRecords = _rowMetadata.Count,
            TotalIndexedFields = _globalFieldIndex.Count,
            DataSources = new List<DataSourceStats>()
        };

        foreach (var kv in _recordStores)
        {
            var s = kv.Value.GetStatistics();
            stats.DataSources.Add(new DataSourceStats
            {
                DataSourceId = kv.Key,
                RecordCount = s.RecordCount,
                StorageSizeBytes = s.TotalSizeBytes,
                StorageType = s.StorageType,
                IsReadOnly = s.IsReadOnly
            });
            stats.TotalStorageSizeBytes += s.TotalSizeBytes;
        }
        return stats;
    }

    public void ClearCaches()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        _logger.LogInformation("GC forced; caches cleared");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            foreach (var store in _recordStores.Values) store.Dispose();
            _qgramGenerator?.Dispose();
            _recordStores.Clear();
            _rowMetadata.Clear();
            foreach (var shards in _seenPairsByDefinition.Values)
                foreach (var shard in shards)
                    shard.Clear();
            _seenPairsByDefinition.Clear();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Dispose error");
        }
    }

    public IAsyncEnumerable<CandidatePair> GenerateWithinSourceCandidatesAsync(Guid sourceId, List<string> fieldNames, double minSimilarityThreshold = 0.1, CancellationToken cancellationToken = default)
        => throw new NotImplementedException();
    public IAsyncEnumerable<CandidatePair> GenerateCrossSourceCandidatesAsync(Guid sourceId1, Guid sourceId2, List<string> fieldNames, double minSimilarityThreshold = 0.1, CancellationToken cancellationToken = default)
        => throw new NotImplementedException();

    #endregion

    #region Blocking strategies

    private BlockingStrategy DetermineBlockingStrategy(Domain.Entities.MatchDefinition definition)
    {
        var s = new BlockingStrategy { DefinitionId = definition.Id };

        var exact = definition.Criteria
            .Where(c => c.MatchingType == MatchingType.Exact &&
                        (c.DataType == CriteriaDataType.Text || c.DataType == CriteriaDataType.Number))
            .ToList();

        if (!exact.Any())
        {
            s.UseBlocking = false;
            s.StrategyType = BlockingStrategyType.GlobalHashIndexing;
            return s;
        }

        foreach (var c in exact)
            foreach (var m in c.FieldMappings)
                s.BlockingFields.Add((m.DataSourceId, m.FieldName));

        if (exact.Count >= 1)
        {
            s.UseBlocking = true;
            s.StrategyType = exact.Count == 1
                ? BlockingStrategyType.SingleFieldBlocking
                : BlockingStrategyType.MultiFieldBlocking;
        }
        else
        {
            s.UseBlocking = false;
            s.StrategyType = BlockingStrategyType.GlobalHashIndexing;
        }

        return s;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsFieldUsedForBlocking(Guid dataSourceId, string fieldName)
        => _definitionBlockingStrategies.Values.Any(s =>
               s.BlockingFields.Any(f => f.DataSourceId == dataSourceId &&
                                         f.FieldName.Equals(fieldName, StringComparison.OrdinalIgnoreCase)));

    private string NormalizeForBlocking(string value)
    {
        if (string.IsNullOrWhiteSpace(value)) return string.Empty;
        var sb = new System.Text.StringBuilder(value.Length);
        foreach (var ch in value)
            if (char.IsLetterOrDigit(ch)) sb.Append(char.ToUpperInvariant(ch));
        if (sb.Length == 0) return value.Trim().ToUpperInvariant();
        return sb.ToString();
    }

    private List<(Guid, Guid)> GetDataSourcePairsFromDefinition(Domain.Entities.MatchDefinition definition)
    {
        var ids = definition.Criteria
            .SelectMany(c => c.FieldMappings)
            .Select(m => m.DataSourceId)
            .Distinct()
            .ToList();

        var pairs = new List<(Guid, Guid)>();
        if (ids.Count == 1) pairs.Add((ids[0], ids[0]));
        else if (ids.Count == 2) pairs.Add((ids[0], ids[1]));
        else
        {
            for (int i = 0; i < ids.Count; i++)
                for (int j = i; j < ids.Count; j++)
                    pairs.Add((ids[i], ids[j]));
        }
        return pairs;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Hash32(string s)
    {
        const uint FNV_OFFSET = 2166136261, FNV_PRIME = 16777619;
        uint h = FNV_OFFSET;
        foreach (var ch in s.AsSpan()) { h ^= ch; h *= FNV_PRIME; }
        return h;
    }

    #endregion

    #region FAST Candidate Generation

    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesWithGlobalProgressiveAsync(
        Domain.Entities.MatchDefinition def,
        List<FieldMapping> fieldMappings,
        Guid s1, Guid s2,
        IRecordStore store1, IRecordStore store2,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var rows = new List<(Guid ds, int row)>(_rowMetadata.Count);
        foreach (var kv in _rowMetadata)
            if (kv.Key.Item1 == s1 || kv.Key.Item1 == s2)
                rows.Add((kv.Key.Item1, kv.Key.Item2));

        await foreach (var c in GenerateCandidatesProgressiveAsync(
            def, fieldMappings, rows, s1, s2, store1, store2, ct))
            yield return c;
    }

    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesWithBlockingAsync(
        Domain.Entities.MatchDefinition def,
        BlockingStrategy strategy,
        List<FieldMapping> fieldMappings,
        Guid s1, Guid s2,
        IRecordStore store1, IRecordStore store2,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // SIMPLIFIED FIX: Single method, Guid.Empty convention
        var blocks = BuildBlocks(s1, s2, strategy);

        if (_options.LogBlockDistribution && blocks.Count > 0)
        {
            _logger.LogInformation(
                "Blocking: DefId={DefId}, Scenario={Scenario}, Blocks={Count}, AvgSize={Avg:F1}, MaxSize={Max}",
                def.Id, s1 == s2 ? "Within-Source" : "Cross-Source", blocks.Count,
                blocks.Average(b => b.Value.Count), blocks.Max(b => b.Value.Count));
        }

        // OPTIMIZED: Larger channel with bounded mode for backpressure
        var outCh = Channel.CreateBounded<CandidatePair>(new BoundedChannelOptions(_options.CandidateChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        var pTask = Task.Run(async () =>
        {
            try
            {
                // OPTIMIZED: Higher parallelism for block processing
                await Parallel.ForEachAsync(
                    blocks,
                    new ParallelOptions { MaxDegreeOfParallelism = _options.MaxParallelism, CancellationToken = ct },
                    async (block, token) =>
                    {
                        await foreach (var c in GenerateCandidatesProgressiveAsync(
                            def, fieldMappings, block.Value, s1, s2, store1, store2, token))
                        {
                            await outCh.Writer.WriteAsync(c, token);
                        }
                    });
            }
            finally { outCh.Writer.Complete(); }
        }, ct);

        await foreach (var c in outCh.Reader.ReadAllAsync(ct))
            yield return c;

        await pTask;
    }

    /// <summary>
    /// FAST: Hybrid approach - smart cutoff instead of full Cartesian
    /// </summary>
    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesProgressiveAsync(
        Domain.Entities.MatchDefinition def,
        List<FieldMapping> fieldMappings,
        List<(Guid ds, int row)> rowsInBlock,
        Guid s1, Guid s2,
        IRecordStore store1, IRecordStore store2,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int size = rowsInBlock.Count;

        // TIER 1: Hybrid - use sampling for medium blocks to save time
        if (size <= _options.FullPairCutoff)  // Lower cutoff for speed
        {
            await foreach (var cp in SmartCartesianAsync(
                rowsInBlock, fieldMappings, def, s1, s2, store1, store2, ct))
                yield return cp;
            yield break;
        }

        // TIER 2: Progressive with larger windows for speed
        int dynWindow = Math.Min(_options.SortedWindow, size / 10); // Adaptive but capped

        //  FIX: Get the first field name to check in global index
        var firstFieldName = fieldMappings.FirstOrDefault()?.FieldName ?? string.Empty;

        if (string.IsNullOrEmpty(firstFieldName) ||
            !_globalFieldIndex.TryGetValue(firstFieldName, out var fldIdx) ||
            !_globalFieldDf.TryGetValue(firstFieldName, out var fldDf))
        {
            await foreach (var cp in FastSNBAsync(
                rowsInBlock, fieldMappings, def, s1, s2, store1, store2, dynWindow, ct))
                yield return cp;
            yield break;
        }

        var seeds = ChooseRareGramsSample(rowsInBlock, firstFieldName, fldIdx, fldDf,
            Math.Min(150, _options.RareTopK), Math.Min(200, size));

        var covered = new HashSet<(Guid, int)>(capacity: Math.Min(size, 1 << 16));
        var blockSet = new HashSet<(Guid, int)>(rowsInBlock);

        if (seeds.Count > 0)
        {
            foreach (var g in seeds)
            {
                if (!fldIdx.TryGetValue(g, out var pl)) continue;

                var sub = new List<(Guid ds, int row)>();
                foreach (var item in pl.Items)
                {
                    if (blockSet.Contains(item))
                    {
                        sub.Add(item);
                        covered.Add(item);
                        if (sub.Count >= _options.MaxBlockSize) break;
                    }
                }

                if (sub.Count < 2) continue;

                await foreach (var cp in FastSNBAsync(
                    sub, fieldMappings, def, s1, s2, store1, store2, dynWindow, ct))
                    yield return cp;
            }
        }

        // Tier 3: Safety net - only if significant uncovered rows
        if (covered.Count < size * 0.85)  // Only if <85% coverage
        {
            var leftover = rowsInBlock.Where(r => !covered.Contains(r)).ToList();
            if (leftover.Count > 0)
            {
                int safetyWindow = Math.Min(300, leftover.Count / 3);

                await foreach (var cp in FastSNBAsync(
                    leftover, fieldMappings, def, s1, s2, store1, store2, safetyWindow, ct))
                    yield return cp;
            }
        }
    }

    /// <summary>
    /// FAST: Smart Cartesian - batched validation to reduce async overhead
    /// </summary>
    private async IAsyncEnumerable<CandidatePair> SmartCartesianAsync(
        List<(Guid ds, int row)> rows,
        List<FieldMapping> fieldMappings,
        Domain.Entities.MatchDefinition def,
        Guid s1, Guid s2,
        IRecordStore store1, IRecordStore store2,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var keyed = rows
            .Select(t =>
            {
                var fld = GetFieldName(fieldMappings, t.ds);  //  Get correct field for this DS

                if (!string.IsNullOrEmpty(fld) &&
                    _rowMetadata.TryGetValue((t.ds, t.row), out var m))
                {
                    if (m.BlockingValues.TryGetValue(fld, out var norm)) return (t.ds, t.row, norm);
                    if (m.FieldHashes.TryGetValue(fld, out var set)) return (t.ds, t.row, set.Count.ToString("D8"));
                }
                return (t.ds, t.row, string.Empty);
            })
            .OrderBy(x => x.Item3, StringComparer.Ordinal)
            .ToList();

        // OPTIMIZED: Batch candidate creation
        var batch = new List<(int i, int j)>(100);

        for (int i = 0; i < keyed.Count; i++)
        {
            if (ct.IsCancellationRequested) yield break;

            var a = keyed[i];

            for (int j = i + 1; j < keyed.Count; j++)
            {
                var b = keyed[j];

                if (s1 == s2)
                {
                    if (a.ds != s1 || b.ds != s2) continue;
                    if (a.row == b.row) continue;
                }
                else
                {
                    if (!((a.ds == s1 && b.ds == s2) || (a.ds == s2 && b.ds == s1))) continue;
                }

                var fldA = GetFieldName(fieldMappings, a.ds);
                var fldB = GetFieldName(fieldMappings, b.ds);
                if (!QuickOverlap((a.ds, a.row, a.Item3), (b.ds, b.row, b.Item3), fldA, fldB)) continue;

                batch.Add((i, j));

                if (batch.Count >= 100)
                {
                    // Validate batch
                    foreach (var (bi, bj) in batch)
                    {
                        var aDs = keyed[bi].ds;
                        var aRow = keyed[bi].row;
                        var bDs = keyed[bj].ds;
                        var bRow = keyed[bj].row;

                        //  FIX: Ensure correct datasource order (s1, s2)
                        CandidatePair cand;
                        if (aDs == s1 && bDs == s2)
                        {
                            cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
                        }
                        else if (aDs == s2 && bDs == s1)
                        {
                            // Swap to maintain s1, s2 order
                            cand = await CreateCandidateIfValidAsync(bDs, bRow, aDs, aRow, store1, store2, def);
                        }
                        else
                        {
                            // Both from same datasource (deduplication)
                            cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
                        }

                        if (cand != null) yield return cand;
                    }
                    batch.Clear();
                }
            }
        }

        // Process remaining batch
        foreach (var (bi, bj) in batch)
        {
            var aDs = keyed[bi].ds;
            var aRow = keyed[bi].row;
            var bDs = keyed[bj].ds;
            var bRow = keyed[bj].row;

            //  FIX: Ensure correct datasource order (s1, s2)
            CandidatePair cand;
            if (aDs == s1 && bDs == s2)
            {
                cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
            }
            else if (aDs == s2 && bDs == s1)
            {
                // Swap to maintain s1, s2 order
                cand = await CreateCandidateIfValidAsync(bDs, bRow, aDs, aRow, store1, store2, def);
            }
            else
            {
                // Both from same datasource (deduplication)
                cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
            }

            if (cand != null) yield return cand;
        }
    }

    /// <summary>
    /// FAST SNB: Reduced logging, batch processing
    /// </summary>
    private async IAsyncEnumerable<CandidatePair> FastSNBAsync(
        List<(Guid ds, int row)> rows,
        List<FieldMapping> fieldMappings,
        Domain.Entities.MatchDefinition def,
        Guid s1, Guid s2,
        IRecordStore store1, IRecordStore store2,
        int w,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var keyed = new List<(Guid ds, int row, string norm, int cnt)>(rows.Count);

        foreach (var t in rows)
        {
            var fld = GetFieldName(fieldMappings, t.ds);  //  Get correct field

            if (!string.IsNullOrEmpty(fld) &&
                _rowMetadata.TryGetValue((t.ds, t.row), out var m))
            {
                if (m.BlockingValues.TryGetValue(fld, out var norm) && !string.IsNullOrEmpty(norm))
                {
                    keyed.Add((t.ds, t.row, norm, 0));
                    continue;
                }
                if (m.FieldHashes.TryGetValue(fld, out var set))
                {
                    keyed.Add((t.ds, t.row, null, set.Count));
                    continue;
                }
            }
            keyed.Add((t.ds, t.row, null, 0));
        }

        keyed.Sort(static (a, b) =>
        {
            bool an = a.norm != null, bn = b.norm != null;
            if (an && bn) return string.Compare(a.norm, b.norm, StringComparison.Ordinal);
            if (an ^ bn) return an ? -1 : 1;
            return a.cnt.CompareTo(b.cnt);
        });

        w = Math.Max(2, Math.Min(w, keyed.Count));

        // OPTIMIZED: Batch validation
        var batch = new List<(int i, int j)>(50);

        for (int i = 0; i < keyed.Count; i++)
        {
            int end = Math.Min(keyed.Count, i + w);
            for (int j = i + 1; j < end; j++)
            {
                if (ct.IsCancellationRequested) yield break;

                var a = keyed[i]; var b = keyed[j];

                if (s1 == s2)
                {
                    if (a.ds != s1 || b.ds != s2) continue;
                    if (a.row == b.row) continue;
                }
                else
                {
                    if (!((a.ds == s1 && b.ds == s2) || (a.ds == s2 && b.ds == s1))) continue;
                }

                var fldA = GetFieldName(fieldMappings, a.ds);
                var fldB = GetFieldName(fieldMappings, b.ds);
                if (!QuickOverlap((a.ds, a.row, a.norm ?? string.Empty),
                                  (b.ds, b.row, b.norm ?? string.Empty), fldA, fldB)) continue;

                batch.Add((i, j));

                if (batch.Count >= 50)
                {
                    foreach (var (bi, bj) in batch)
                    {
                        var aDs = keyed[bi].ds;
                        var aRow = keyed[bi].row;
                        var bDs = keyed[bj].ds;
                        var bRow = keyed[bj].row;

                        //  FIX: Ensure correct datasource order (s1, s2)
                        CandidatePair cand;
                        if (aDs == s1 && bDs == s2)
                        {
                            cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
                        }
                        else if (aDs == s2 && bDs == s1)
                        {
                            // Swap to maintain s1, s2 order
                            cand = await CreateCandidateIfValidAsync(bDs, bRow, aDs, aRow, store1, store2, def);
                        }
                        else
                        {
                            // Both from same datasource (deduplication)
                            cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
                        }

                        if (cand != null) yield return cand;
                    }
                    batch.Clear();
                }
            }
        }

        foreach (var (bi, bj) in batch)
        {
            var aDs = keyed[bi].ds;
            var aRow = keyed[bi].row;
            var bDs = keyed[bj].ds;
            var bRow = keyed[bj].row;

            //  FIX: Ensure correct datasource order (s1, s2)
            CandidatePair cand;
            if (aDs == s1 && bDs == s2)
            {
                cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
            }
            else if (aDs == s2 && bDs == s1)
            {
                // Swap to maintain s1, s2 order
                cand = await CreateCandidateIfValidAsync(bDs, bRow, aDs, aRow, store1, store2, def);
            }
            else
            {
                // Both from same datasource (deduplication)
                cand = await CreateCandidateIfValidAsync(aDs, aRow, bDs, bRow, store1, store2, def);
            }

            if (cand != null) yield return cand;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool QuickOverlap((Guid ds, int row, string key) a, (Guid ds, int row, string key) b, string fieldA, string fieldB)
    {
        if (string.IsNullOrEmpty(fieldA) || string.IsNullOrEmpty(fieldB)) return false;

        if (!_rowMetadata.TryGetValue((a.ds, a.row), out var m1) ||
            !_rowMetadata.TryGetValue((b.ds, b.row), out var m2)) return false;
        if (!m1.FieldHashes.TryGetValue(fieldA, out var g1) ||
            !m2.FieldHashes.TryGetValue(fieldB, out var g2)) return false;

        foreach (var g in g1) if (g2.Contains(g)) return true;
        return false;
    }

    /// <summary>
    /// Gets field name for a datasource from field mappings
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GetFieldName(List<FieldMapping> mappings, Guid dataSourceId)
    {
        var fieldName = mappings.FirstOrDefault(m => m.DataSourceId == dataSourceId)?.FieldName ?? string.Empty;

        if (_options.LogBlockingDecisions && !string.IsNullOrEmpty(fieldName))
        {
            _logger.LogTrace("GetFieldName: DS={DS}, Field={Field}",
                dataSourceId.ToString().Substring(0, 8), fieldName);
        }

        return fieldName;
    }

    /// <summary>
    /// SIMPLIFIED FIX: Single BuildBlocks method using Guid.Empty convention
    /// Convention: When s1 != s2, use Guid.Empty as BlockKey DataSourceId to indicate cross-datasource blocks
    /// This allows records from different sources to share the same block while maintaining single struct type
    /// </summary>
    private Dictionary<BlockKey, List<(Guid ds, int row)>> BuildBlocks(
        Guid s1,
        Guid s2,
        BlockingStrategy strategy)
    {
        var blocks = new Dictionary<BlockKey, List<(Guid, int)>>();
        bool isCrossSource = s1 != s2;
        var allowedSources = new HashSet<Guid> { s1, s2 };

        // Build field mapping structure for cross-datasource scenarios
        Dictionary<int, List<(Guid DataSourceId, string FieldName)>> fieldMappings = null;
        if (isCrossSource)
        {
            fieldMappings = BuildFieldMappingStructure(strategy.BlockingFields);
        }

        foreach (var kv in _rowMetadata)
        {
            var meta = kv.Value;

            // Filter to only allowed datasources
            if (!allowedSources.Contains(meta.DataSourceId)) continue;

            BlockKey key;

            if (isCrossSource)
            {
                // CROSS-SOURCE: Use Guid.Empty and resolve field mappings
                var blockingValues = ExtractBlockingValuesForDataSource(
                    meta, meta.DataSourceId, fieldMappings);

                if (blockingValues == null || blockingValues.Count == 0) continue;

                // Create key with Guid.Empty to indicate cross-datasource block
                key = CreateBlockKey(Guid.Empty, blockingValues);
            }
            else
            {
                // WITHIN-SOURCE: Use actual DataSourceId (original logic)
                int need = 0;
                foreach (var f in strategy.BlockingFields)
                    if (f.DataSourceId == meta.DataSourceId) need++;
                if (need == 0) continue;

                if (need <= 3)
                {
                    string p1 = null, p2 = null, p3 = null;
                    int got = 0;

                    foreach (var (ds, field) in strategy.BlockingFields)
                    {
                        if (ds != meta.DataSourceId) continue;
                        if (!meta.BlockingValues.TryGetValue(field, out var v)) { got = -1; break; }
                        if (got == 0) p1 = v;
                        else if (got == 1) p2 = v;
                        else p3 = v;
                        got++;
                    }
                    if (got != need) continue;

                    key = new BlockKey(
                        meta.DataSourceId,  // Use actual DataSourceId for within-source
                        Hash32(p1 ?? string.Empty),
                        need >= 2 ? Hash32(p2 ?? string.Empty) : 0u,
                        need >= 3 ? Hash32(p3 ?? string.Empty) : 0u
                    );
                }
                else
                {
                    uint agg = 0u;
                    int got = 0;
                    foreach (var (ds, field) in strategy.BlockingFields)
                    {
                        if (ds != meta.DataSourceId) continue;
                        if (!meta.BlockingValues.TryGetValue(field, out var v)) { got = -1; break; }
                        unchecked { agg = (agg * 16777619u) ^ Hash32(v); }
                        got++;
                    }
                    if (got != need)
                        continue;

                    key = new BlockKey(meta.DataSourceId, agg, 0, 0);
                }
            }

            if (!blocks.TryGetValue(key, out var list))
                blocks[key] = list = new List<(Guid, int)>(4);
            list.Add((meta.DataSourceId, meta.RowNumber));
        }

        return blocks;
    }

    /// <summary>
    /// Groups blocking fields by their logical criteria position for cross-datasource mapping
    /// </summary>
    private Dictionary<int, List<(Guid DataSourceId, string FieldName)>> BuildFieldMappingStructure(
        List<(Guid DataSourceId, string FieldName)> blockingFields)
    {
        var result = new Dictionary<int, List<(Guid, string)>>();

        var criteriaIndex = 0;
        var processedFields = new HashSet<(Guid, string)>();

        for (int i = 0; i < blockingFields.Count; i++)
        {
            var field = blockingFields[i];
            if (processedFields.Contains(field)) continue;

            var criteriaFields = new List<(Guid, string)> { field };
            processedFields.Add(field);

            // Check if there are corresponding fields from other datasources
            for (int j = i + 1; j < blockingFields.Count; j++)
            {
                var otherField = blockingFields[j];
                if (processedFields.Contains(otherField)) continue;

                // If different datasource, assume same logical field (same criteria)
                if (otherField.DataSourceId != field.DataSourceId)
                {
                    criteriaFields.Add(otherField);
                    processedFields.Add(otherField);
                }
            }

            result[criteriaIndex++] = criteriaFields;
        }

        return result;
    }

    /// <summary>
    /// Extracts blocking values from a record for the specified datasource with field mapping resolution
    /// </summary>
    private List<string> ExtractBlockingValuesForDataSource(
        RowMetadataDME meta,
        Guid dataSourceId,
        Dictionary<int, List<(Guid DataSourceId, string FieldName)>> fieldMappings)
    {
        var values = new List<string>();

        foreach (var criteriaIndex in fieldMappings.Keys.OrderBy(k => k))
        {
            var fieldsForCriteria = fieldMappings[criteriaIndex];

            // Find the field(s) that belong to this datasource
            var relevantField = fieldsForCriteria.FirstOrDefault(f => f.DataSourceId == dataSourceId);

            if (relevantField == default)
            {
                return null; // This datasource doesn't have a field for this criteria
            }

            // Get the normalized blocking value
            if (!meta.BlockingValues.TryGetValue(relevantField.FieldName, out var value))
            {
                return null; // Missing required field value
            }

            values.Add(value);
        }

        return values;
    }

    /// <summary>
    /// Creates a BlockKey from normalized field values
    /// </summary>
    private BlockKey CreateBlockKey(Guid dataSourceId, List<string> values)
    {
        if (values.Count == 0) return default;

        uint k1 = Hash32(values[0]);
        uint k2 = values.Count > 1 ? Hash32(values[1]) : 0u;
        uint k3 = values.Count > 2 ? Hash32(values[2]) : 0u;

        // If more than 3 fields, combine remaining into aggregate hash
        if (values.Count > 3)
        {
            uint agg = k3;
            for (int i = 3; i < values.Count; i++)
            {
                unchecked { agg = (agg * 16777619u) ^ Hash32(values[i]); }
            }
            return new BlockKey(dataSourceId, k1, k2, agg);
        }

        return new BlockKey(dataSourceId, k1, k2, k3);
    }

    /// <summary>
    /// Generate candidates for exact-only matches (no fuzzy criteria)
    /// </summary>
    private async IAsyncEnumerable<CandidatePair> GenerateExactOnlyCandidatesAsync(
Domain.Entities.MatchDefinition def,
Guid s1, Guid s2,
IRecordStore store1, IRecordStore store2,
[EnumeratorCancellation] CancellationToken ct)
    {
        var fields = def.Criteria
            .Where(c => c.MatchingType == MatchingType.Exact)
            .SelectMany(c => c.FieldMappings)
            .Select(m => (m.DataSourceId, m.FieldName))
            .Distinct()
            .ToList();

        if (fields.Count == 0) yield break;

        var strategy = new BlockingStrategy
        {
            DefinitionId = def.Id,
            UseBlocking = true,
            StrategyType = fields.Count == 1
                ? BlockingStrategyType.SingleFieldBlocking
                : BlockingStrategyType.MultiFieldBlocking,
            BlockingFields = fields
        };

        var blocks = BuildBlocks(s1, s2, strategy);

        _logger.LogDebug(
            "Exact-only definition {DefId}: {BlockCount} blocks for {S1}<->{S2}",
            def.Id, blocks.Count,
            s1.ToString("N").Substring(0, 8),
            s2.ToString("N").Substring(0, 8));

        if (!_seenPairsByDefinition.TryGetValue(def.Id, out var shards))
        {
            _logger.LogWarning("No shard tracker for definition {DefId}", def.Id);
            yield break;
        }

        long pairsGenerated = 0;
        bool isSameSource = (s1 == s2);

        foreach (var blockKvp in blocks)
        {
            if (ct.IsCancellationRequested) yield break;

            var block = blockKvp.Value;
            if (block.Count < 2) continue;

            List<(Guid ds, int row)> list1;
            List<(Guid ds, int row)> list2;

            if (isSameSource)
            {
                // Deduplication: same source for both sides
                list1 = block;
                list2 = block;
            }
            else
            {
                // Cross-source: separate by datasource
                list1 = block.Where(r => r.ds == s1).ToList();
                list2 = block.Where(r => r.ds == s2).ToList();

                if (list1.Count == 0 || list2.Count == 0)
                    continue;
            }

            // Generate ALL pairs - no cap during generation
            // Evidence: Legacy generates all pairs, maxMatchesForOneRow applied in final phase
            for (int i = 0; i < list1.Count; i++)
            {
                if (ct.IsCancellationRequested) yield break;

                var rec1 = list1[i];
                int jStart = isSameSource ? i + 1 : 0;

                for (int j = jStart; j < list2.Count; j++)
                {
                    var rec2 = list2[j];

                    // Skip self-pair
                    if (rec1.ds == rec2.ds && rec1.row == rec2.row)
                        continue;

                    // Deduplication via sharded tracking
                    int shardIndex = (int)((uint)HashCode.Combine(rec1.ds, rec1.row, rec2.ds, rec2.row) % SHARD_COUNT);
                    if (!shards[shardIndex].TryAddNormalized(rec1.ds, rec1.row, rec2.ds, rec2.row))
                        continue;

                    // LEAN: Only store references, QuickSimilarity = 1.0 signals exact match
                    // NO record loading here - will be lazy loaded in grouping phase if needed
                    var cand = new CandidatePair(rec1.ds, rec1.row, rec2.ds, rec2.row, store1, store2, 1.0);
                    cand.AddMatchDefinition(def.Id);

                    pairsGenerated++;
                    yield return cand;
                }
            }
        }

        _logger.LogInformation(
            "Exact-only definition {DefId}: Generated {Count:N0} pairs from {Blocks} blocks",
            def.Id, pairsGenerated, blocks.Count);
    }

    #endregion

    #region Candidate validation

    private async Task<CandidatePair> CreateCandidateIfValidAsync(
        Guid ds1, int row1, Guid ds2, int row2,
        IRecordStore store1, IRecordStore store2,
        Domain.Entities.MatchDefinition def)
    {
        if (ds1 == ds2 && row1 == row2) return null;

        //  NEW: Validate that all criteria have field mappings for both datasources
        // This prevents issues when definitions have mappings for 3+ datasources
        foreach (var criterion in def.Criteria)
        {
            var hasDs1Mapping = criterion.FieldMappings.Any(fm => fm.DataSourceId == ds1);
            var hasDs2Mapping = criterion.FieldMappings.Any(fm => fm.DataSourceId == ds2);

            if (!hasDs1Mapping || !hasDs2Mapping)
            {
                if (_options.LogBlockingDecisions)
                {
                    _logger.LogTrace(
                        "Skipping pair ({DS1}-{Row1}, {DS2}-{Row2}): Criterion {CriterionId} missing mapping for DS1 or DS2",
                        ds1.ToString().Substring(0, 8), row1,
                        ds2.ToString().Substring(0, 8), row2,
                        criterion.Id);
                }
                return null; // Skip this pair - criteria not applicable
            }
        }

        // OPTIMIZED: Sharded pair tracking
        if (!_seenPairsByDefinition.TryGetValue(def.Id, out var shards))
            return null;

        var shardIndex = (int)((uint)(ds1.GetHashCode() ^ row1 ^ ds2.GetHashCode() ^ row2) % SHARD_COUNT);
        if (!shards[shardIndex].TryAddNormalized(ds1, row1, ds2, row2)) return null;

        if (!await ValidateAllCriteriaAsync(ds1, row1, ds2, row2, def))
        {
            shards[shardIndex].TryRemoveNormalized(ds1, row1, ds2, row2);
            return null;
        }

        var sim = CalculateQuickSimilarity(ds1, row1, ds2, row2, def);
        var cand = new CandidatePair(ds1, row1, ds2, row2, store1, store2, sim);
        cand.AddMatchDefinition(def.Id);
        return cand;
    }

    private async Task<bool> ValidateAllCriteriaAsync(
        Guid ds1, int row1, Guid ds2, int row2,
        Domain.Entities.MatchDefinition def)
    {
        if (!_rowMetadata.TryGetValue((ds1, row1), out var m1) ||
            !_rowMetadata.TryGetValue((ds2, row2), out var m2))
            return false;

        foreach (var c in def.Criteria)
        {
            var thr = GetThreshold(c);

            if (c.MatchingType == MatchingType.Exact)
            {
                if (!ValidateExactCriteria(m1, m2, c, ds1, ds2, thr))
                    return false;
            }
            else
            {
                if (!ValidateFuzzyCriteria(m1, m2, c, ds1, ds2, thr))
                    return false;
            }
        }
        await Task.CompletedTask;
        return true;
    }

    private bool ValidateExactCriteria(
        RowMetadataDME m1, RowMetadataDME m2, MatchCriteria c,
        Guid ds1, Guid ds2, double thr)
    {
        if (c.DataType == CriteriaDataType.Phonetic || c.DataType == CriteriaDataType.Number) return true;

        var f1 = c.FieldMappings.Where(f => f.DataSourceId == ds1).Select(f => f.FieldName);
        var f2 = c.FieldMappings.Where(f => f.DataSourceId == ds2).Select(f => f.FieldName);

        foreach (var a in f1)
        {
            foreach (var b in f2)
            {
                if (m1.BlockingValues.TryGetValue(a, out var v1) &&
                    m2.BlockingValues.TryGetValue(b, out var v2))
                {
                    if (!string.Equals(v1, v2, StringComparison.Ordinal)) return false;
                }
                else return false;
            }
        }
        return true;
    }

    private bool ValidateFuzzyCriteria(
        RowMetadataDME m1, RowMetadataDME m2, MatchCriteria c,
        Guid ds1, Guid ds2, double thr)
    {
        if (c.DataType != CriteriaDataType.Text) return true;

        var f1 = c.FieldMappings.Where(f => f.DataSourceId == ds1).Select(f => f.FieldName);
        var f2 = c.FieldMappings.Where(f => f.DataSourceId == ds2).Select(f => f.FieldName);

        double sum = 0; int cnt = 0;
        foreach (var a in f1)
        {
            foreach (var b in f2)
            {
                if (m1.FieldHashes.TryGetValue(a, out var h1) &&
                    m2.FieldHashes.TryGetValue(b, out var h2))
                {
                    var s = _similarityStrategy.CalculateSimilarity(h1, h2);
                    sum += s; cnt++;
                }
            }
        }

        if (cnt == 0) return false;
        return (sum / cnt) >= thr;
    }

    private double CalculateQuickSimilarity(Guid ds1, int row1, Guid ds2, int row2, Domain.Entities.MatchDefinition def)
    {
        if (!_rowMetadata.TryGetValue((ds1, row1), out var m1) ||
            !_rowMetadata.TryGetValue((ds2, row2), out var m2)) return 0;

        var all1 = m1.FieldHashes.Values.SelectMany(x => x).ToHashSet();
        var all2 = m2.FieldHashes.Values.SelectMany(x => x).ToHashSet();
        if (all1.Count == 0 || all2.Count == 0) return 0;

        return _similarityStrategy.CalculateSimilarity(all1, all2);
    }

    private double GetThreshold(MatchCriteria c)
    {
        if (c.MatchingType == MatchingType.Exact && c.DataType != CriteriaDataType.Phonetic) return 0.99;
        if (c.Arguments.TryGetValue(ArgsValue.FastLevel, out var s) && double.TryParse(s, out var t)) return t;

        return c.DataType switch
        {
            CriteriaDataType.Text => 0.7,
            _ => 0.0
        };
    }

    private List<uint> ChooseRareGramsSample(
        List<(Guid ds, int row)> rows,
        string fld,
        IReadOnlyDictionary<uint, PostingList> idx,
        IReadOnlyDictionary<uint, int> df,
        int k,
        int sampleCap)
    {
        var present = new HashSet<uint>();
        int sample = Math.Min(sampleCap, rows.Count);

        for (int i = 0; i < sample; i++)
        {
            var (ds, row) = rows[i];
            if (!_rowMetadata.TryGetValue((ds, row), out var meta)) continue;
            if (!meta.FieldHashes.TryGetValue(fld, out var grams)) continue;

            foreach (var g in grams)
            {
                if (idx.ContainsKey(g)) present.Add(g);
            }
        }

        if (present.Count == 0) return new List<uint>(0);

        var scored = new List<(uint g, int d)>(present.Count);
        foreach (var g in present)
        {
            if (!df.TryGetValue(g, out var d)) continue;
            if (d > rows.Count) continue;
            scored.Add((g, d));
        }

        if (scored.Count == 0) return new List<uint>(0);

        scored.Sort((a, b) => a.d.CompareTo(b.d));
        int take = Math.Min(k, scored.Count);

        var result = new List<uint>(take);
        for (int i = 0; i < take; i++) result.Add(scored[i].g);
        return result;
    }

    #endregion

    #region Live Search

    /// <summary>
    /// NEW METHOD: Extract index data for persistence (used by Live Search)
    /// This doesn't modify existing batch logic - just exposes the internal index
    /// </summary>
    public QGramIndexData BuildIndexDataForPersistence(Guid projectId)
    {
        _logger.LogInformation("Extracting index data for persistence (Project: {ProjectId})", projectId);

        var indexData = new QGramIndexData
        {
            ProjectId = projectId,
            CreatedAt = DateTime.UtcNow,
            GlobalFieldIndex = new Dictionary<string, Dictionary<uint, List<PostingEntry>>>(),
            RowMetadata = new Dictionary<(Guid, int), MatchLogic.Application.Interfaces.LiveSearch.RowMetadataLight>(),
            DataSourceStats = new Dictionary<Guid, MatchLogic.Application.Interfaces.LiveSearch.DataSourceStats>()
        };

        // Convert internal _globalFieldIndex to serializable format
        foreach (var field in _globalFieldIndex)
        {
            var fieldIndex = new Dictionary<uint, List<PostingEntry>>();

            foreach (var qgramEntry in field.Value)
            {
                var postings = new List<PostingEntry>();
                var items = qgramEntry.Value.Items; // Get sealed items

                foreach (var (ds, row) in items)
                {
                    postings.Add(new PostingEntry(ds, row));
                }

                fieldIndex[qgramEntry.Key] = postings;
            }

            indexData.GlobalFieldIndex[field.Key] = fieldIndex;
        }

        // Convert internal _rowMetadata to serializable format
        foreach (var meta in _rowMetadata)
        {
            var key = $"{meta.Key.Item1:N}_{meta.Key.Item2}";
            indexData.RowMetadata[meta.Key] = new RowMetadataLight
            {
                DataSourceId = meta.Value.DataSourceId,
                RowNumber = meta.Value.RowNumber,
                FieldHashes = meta.Value.FieldHashes.ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value),
                BlockingValues = meta.Value.BlockingValues.ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value)
            };
        }

        // Build data source stats
        var dataSourceGroups = _rowMetadata.GroupBy(m => m.Value.DataSourceId);
        foreach (var group in dataSourceGroups)
        {
            var dsId = group.Key;
            var recordCount = group.Count();
            var indexedFields = group.First().Value.FieldHashes.Keys.ToList();

            indexData.DataSourceStats[dsId] = new MatchLogic.Application.Interfaces.LiveSearch.DataSourceStats
            {
                DataSourceId = dsId,
                RecordCount = recordCount,
                IndexedFields = indexedFields
            };
        }

        indexData.TotalRecords = _rowMetadata.Count;

        _logger.LogInformation(
            "Index data extracted: {Records} records, {Fields} fields",
            indexData.TotalRecords,
            indexData.GlobalFieldIndex.Count);

        return indexData;
    }

    /// <summary>
    /// NEW METHOD: Load persisted index data back into memory (used by Live Search query nodes)
    /// This populates the internal structures from persisted data
    /// </summary>
    public void LoadIndexDataFromPersistence(QGramIndexData indexData)
    {
        _logger.LogInformation("Loading index data from persistence");

        // Clear existing data
        _globalFieldIndex.Clear();
        _globalFieldDf.Clear();
        _rowMetadata.Clear();

        // Load GlobalFieldIndex
        foreach (var field in indexData.GlobalFieldIndex)
        {
            var fieldIndex = new ConcurrentDictionary<uint, PostingList>();

            foreach (var qgramEntry in field.Value)
            {
                var postingList = new PostingList(qgramEntry.Value.Count);

                foreach (var posting in qgramEntry.Value)
                {
                    postingList.Add(posting.DataSourceId, posting.RowNumber);
                }

                postingList.Seal();
                fieldIndex[qgramEntry.Key] = postingList;
            }

            _globalFieldIndex[field.Key] = fieldIndex;
        }

        // Load RowMetadata
        foreach (var meta in indexData.RowMetadata)
        {
            var rowMeta = new RowMetadataDME
            {
                DataSourceId = meta.Value.DataSourceId,
                RowNumber = meta.Value.RowNumber,
                FieldHashes = meta.Value.FieldHashes.ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value),
                BlockingValues = meta.Value.BlockingValues.ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value)
            };

            _rowMetadata[(meta.Value.DataSourceId, meta.Value.RowNumber)] = rowMeta;
        }

        // Build DF (document frequency) for similarity calculations
        foreach (var field in _globalFieldIndex)
        {
            var dfDict = new ConcurrentDictionary<uint, int>();

            foreach (var qgramEntry in field.Value)
            {
                dfDict[qgramEntry.Key] = qgramEntry.Value.Items.Length;
            }

            _globalFieldDf[field.Key] = dfDict;
        }

        _sealedGlobalIndex = true;

        _logger.LogInformation(
            "Index data loaded: {Records} records, {Fields} fields",
            _rowMetadata.Count,
            _globalFieldIndex.Count);
    }

    /// <summary>
    /// NEW METHOD: Generate candidates for a single new record (Live Search)
    /// Uses existing candidate generation logic but for one record against the indexed corpus
    /// </summary>
    public async Task<List<CandidatePair>> GenerateCandidatesForSingleRecordAsync(
        Guid projectId,
        Guid newRecordDataSourceId,
        IDictionary<string, object> newRecord,
        MatchDefinitionCollection matchDefinitions,
        double minSimilarityThreshold = 0.1,
        int maxCandidates = 1000,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Generating candidates for single record (DataSource: {DataSourceId})",
            newRecordDataSourceId);

        // Initialize blocking if needed. This Singleton is hit concurrently by every live
        // search request on a query node, so guard the lazy init against the first-request
        // race (double-checked lock pattern).
        if (_matchDefinitions == null)
        {
            lock (_initLock)
            {
                if (_matchDefinitions == null)
                    InitializeBlockingConfiguration(matchDefinitions);
            }
        }

        // Create temporary store for the new record
        var tempStore = new InMemoryRecordStore(1);
        await tempStore.AddRecordAsync(newRecord);
        await tempStore.SwitchToReadOnlyModeAsync();

        // Create temporary metadata for the new record
        var newRecordRowNumber = 0;
        var newRecordMeta = new RowMetadataDME
        {
            DataSourceId = newRecordDataSourceId,
            RowNumber = newRecordRowNumber
        };

        // Extract q-grams from the new record (reuse existing logic)
        var hashBuffer = _qgramGenerator.RentHashBuffer(1024);
        try
        {
            foreach (var field in newRecord.Keys)
            {
                if (!newRecord.TryGetValue(field, out var raw)) continue;
                var text = Convert.ToString(raw) ?? string.Empty;
                if (string.IsNullOrWhiteSpace(text)) continue;

                _qgramGenerator.GenerateHashes(
                    System.Text.RegularExpressions.Regex.Replace(text.ToUpper(), "[^0-9a-zA-Z]", "").AsSpan(),
                    hashBuffer,
                    out int count);

                var set = new HashSet<uint>();
                for (int i = 0; i < count; i++)
                    set.Add(hashBuffer[i]);

                if (set.Count > 0)
                    newRecordMeta.FieldHashes[field] = set;

                if (_options.EnableBlocking && IsFieldUsedForBlocking(newRecordDataSourceId, field))
                {
                    newRecordMeta.BlockingValues[field] = NormalizeForBlocking(text);
                }
            }
        }
        finally
        {
            _qgramGenerator.ReturnHashBuffer(hashBuffer);
        }

        // Find candidate records using q-gram similarity
        var candidates = new List<CandidatePair>();
        var candidateScores = new Dictionary<(Guid ds, int row), double>();

        // For each field in the new record, query the inverted index
        foreach (var fieldName in newRecordMeta.FieldHashes.Keys)
        {
            if (!_globalFieldIndex.TryGetValue(fieldName, out var fieldIndex))
                continue;

            var newRecordQGrams = newRecordMeta.FieldHashes[fieldName];
            if (!_globalFieldDf.TryGetValue(fieldName, out var fieldDf))
                continue;

            // Get candidate records from posting lists
            var candidateRefs = new HashSet<(Guid ds, int row)>();
            foreach (var qgram in newRecordQGrams)
            {
                if (!fieldIndex.TryGetValue(qgram, out var postingList))
                    continue;

                foreach (var (ds, row) in postingList.Items)
                {
                    candidateRefs.Add((ds, row));
                }
            }

            // Calculate similarity for each candidate
            foreach (var (ds, row) in candidateRefs)
            {
                if (!_rowMetadata.TryGetValue((ds, row), out var candidateMeta))
                    continue;

                if (!candidateMeta.FieldHashes.TryGetValue(fieldName, out var candidateQGrams))
                    continue;

                // Calculate Jaccard similarity (reuse existing strategy)
                var similarity = _similarityStrategy.CalculateSimilarity(
                    newRecordQGrams,
                    candidateQGrams);

                if (similarity >= minSimilarityThreshold)
                {
                    var key = (ds, row);
                    if (!candidateScores.ContainsKey(key))
                        candidateScores[key] = 0;

                    candidateScores[key] = Math.Max(candidateScores[key], similarity);
                }
            }
        }

        // Create CandidatePair objects for top candidates
        var topCandidates = candidateScores
            .OrderByDescending(kvp => kvp.Value)
            .Take(maxCandidates)
            .ToList();

        foreach (var (candidateRef, score) in topCandidates)
        {
            var (targetDs, targetRow) = candidateRef;

            if (!_recordStores.TryGetValue(targetDs, out var targetStore))
                continue;

            var pair = new CandidatePair(
                newRecordDataSourceId,
                newRecordRowNumber,
                targetDs,
                targetRow,
                tempStore,
                targetStore);

            pair.AddMatchDefinitions(matchDefinitions.Definitions.Select(d => d.Id));

            candidates.Add(pair);
        }

        _logger.LogInformation(
            "Generated {Count} candidates for single record",
            candidates.Count);

        return candidates;
    }

    /// <summary>
    /// NEW METHOD: Register an already-built record store (for Live Search)
    /// Allows external code to provide record stores without re-indexing
    /// </summary>
    public void RegisterRecordStore(Guid dataSourceId, IRecordStore store)
    {
        _recordStores[dataSourceId] = store;
        _logger.LogDebug("Registered record store for DataSource {DataSourceId}", dataSourceId);
    }

    /// <summary>
    /// NEW METHOD: Get existing record store (for Live Search)
    /// </summary>
    public IRecordStore GetRecordStore(Guid dataSourceId)
    {
        _recordStores.TryGetValue(dataSourceId, out var store);
        return store;
    }

    #endregion
}

#region Support types

public class BlockingStrategy
{
    public Guid DefinitionId { get; set; }
    public bool UseBlocking { get; set; }
    public BlockingStrategyType StrategyType { get; set; }
    public List<(Guid DataSourceId, string FieldName)> BlockingFields { get; set; } = new();
}

public enum BlockingStrategyType
{
    GlobalHashIndexing,
    SingleFieldBlocking,
    MultiFieldBlocking
}

public class RowMetadataDME
{
    public Guid DataSourceId { get; set; }
    public int RowNumber { get; set; }
    public Dictionary<string, HashSet<uint>> FieldHashes { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, string> BlockingValues { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public int RecordId { get; set; }
    public uint[] NGrams { get; set; } = Array.Empty<uint>();
    public uint[] PhoneticGrams { get; set; } = Array.Empty<uint>();

    public void ClearAndReturnPools()
    {
        if (NGrams.Length > 0) ArrayPool<uint>.Shared.Return(NGrams, clearArray: true);
        if (PhoneticGrams.Length > 0) ArrayPool<uint>.Shared.Return(PhoneticGrams, clearArray: true);
        NGrams = Array.Empty<uint>();
        PhoneticGrams = Array.Empty<uint>();
    }
}

/// <summary>
/// SIMPLIFIED: Single BlockKey struct
/// Convention: DataSourceId = Guid.Empty indicates cross-datasource block
/// </summary>
internal readonly struct BlockKey : IEquatable<BlockKey>
{
    public readonly Guid DataSourceId;
    public readonly uint K1, K2, K3;

    public BlockKey(Guid ds, uint k1, uint k2 = 0u, uint k3 = 0u)
    { DataSourceId = ds; K1 = k1; K2 = k2; K3 = k3; }

    public bool Equals(BlockKey other)
        => DataSourceId == other.DataSourceId && K1 == other.K1 && K2 == other.K2 && K3 == other.K3;

    public override bool Equals(object? obj) => obj is BlockKey o && Equals(o);

    public override int GetHashCode() => HashCode.Combine(DataSourceId, K1, K2, K3);
}

internal sealed class PostingList
{
    private (Guid ds, int row)[] _buf;
    private int _count;
    private volatile (Guid ds, int row)[] _sealed;
    private readonly object _lock = new();

    public PostingList(int initialCapacity = 64)
    {
        _buf = ArrayPool<(Guid, int)>.Shared.Rent(initialCapacity);
        _count = 0;
    }

    public void Add(Guid ds, int row)
    {
        if (_sealed != null) throw new InvalidOperationException("PostingList sealed");
        lock (_lock)
        {
            if (_count == _buf.Length)
            {
                var bigger = ArrayPool<(Guid, int)>.Shared.Rent(_buf.Length * 2);
                Array.Copy(_buf, 0, bigger, 0, _count);
                ArrayPool<(Guid, int)>.Shared.Return(_buf, clearArray: false);
                _buf = bigger;
            }
            _buf[_count++] = (ds, row);
        }
    }

    public void Seal()
    {
        if (_sealed != null) return;
        lock (_lock)
        {
            var arr = new (Guid ds, int row)[_count];
            Array.Copy(_buf, 0, arr, 0, _count);
            ArrayPool<(Guid, int)>.Shared.Return(_buf, clearArray: false);
            _buf = null!;
            _sealed = arr;
        }
    }

    public (Guid ds, int row)[] Items => _sealed ?? Array.Empty<(Guid, int)>();
}

internal sealed class ProcessedPairs64
{
    private readonly ConcurrentDictionary<ulong, byte> _seen = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong MakeKey(Guid ds1, int row1, Guid ds2, int row2)
    {
        bool swap = ds1.CompareTo(ds2) > 0 || (ds1 == ds2 && row1 > row2);
        var aDs = swap ? ds2 : ds1; var aRow = swap ? row2 : row1;
        var bDs = swap ? ds1 : ds2; var bRow = swap ? row1 : row2;

        unchecked
        {
            uint ah = (uint)aDs.GetHashCode();
            uint bh = (uint)bDs.GetHashCode();
            uint lo = (uint)HashCode.Combine(bRow, bh);
            uint hi = (uint)HashCode.Combine(aRow, ah);
            return ((ulong)hi << 32) | lo;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAddNormalized(Guid ds1, int row1, Guid ds2, int row2)
        => _seen.TryAdd(MakeKey(ds1, row1, ds2, row2), 0);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRemoveNormalized(Guid ds1, int row1, Guid ds2, int row2)
        => _seen.TryRemove(MakeKey(ds1, row1, ds2, row2), out _);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(Guid ds1, int row1, Guid ds2, int row2)
        => _seen.ContainsKey(MakeKey(ds1, row1, ds2, row2));

    public void Clear() => _seen.Clear();
}

#endregion
