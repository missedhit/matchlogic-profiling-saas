using MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;

/// <summary>
/// Enhanced blocking index structure for definition-aware processing
/// </summary>
public class BlockingIndex
{
    public Guid DefinitionId { get; set; } 
    public bool UseBlocking { get; set; }
    public BlockingScheme Scheme { get; set; }

    // Store which fields from which sources are used for blocking
    public List<(Guid DataSourceId, string FieldName)> BlockingFieldMappings { get; set; } = new();

    // Blocks now use composite key
    public ConcurrentDictionary<CompositeBlockKey, HashSet<PairItemRowReference>> Blocks { get; set; } = new();

    // Cross-source block value mapping: blockValue -> list of composite keys with that value
    public ConcurrentDictionary<string, List<CompositeBlockKey>> BlockValueIndex { get; set; } = new();
}

/// <summary>
/// Composite key for blocking that considers data source, field name, and block value
/// </summary>
public class CompositeBlockKey : IEquatable<CompositeBlockKey>
{
    public Guid DataSourceId { get; set; }
    public string FieldName { get; set; }
    public string BlockValue { get; set; }

    public CompositeBlockKey(Guid dataSourceId, string fieldName, string blockValue)
    {
        DataSourceId = dataSourceId;
        FieldName = fieldName;
        BlockValue = blockValue;
    }

    public override bool Equals(object obj) => Equals(obj as CompositeBlockKey);

    public bool Equals(CompositeBlockKey other)
    {
        if (other == null) return false;
        return DataSourceId == other.DataSourceId &&
               FieldName == other.FieldName &&
               BlockValue == other.BlockValue;
    }

    public override int GetHashCode() => HashCode.Combine(DataSourceId, FieldName, BlockValue);

    public override string ToString() => $"{DataSourceId}_{FieldName}_{BlockValue}";
}

public enum BlockingScheme
{
    None,           // No blocking (fuzzy with special types)
    SingleField,    // Single exact field blocking
    MultiField,     // Multiple exact fields combined
    MultiScheme     // Multiple blocking schemes (first/middle/last q-gram)
}

public partial class ProductionQGramIndexerWithBlocking : IProductionQGramIndexer
{
    private readonly QGramIndexerWithBlockingOptions _options;
    private readonly MemoryMappedStoreOptions _memoryMappedStoreOptions;
    private readonly ILogger<ProductionQGramIndexerWithBlocking> _logger;
    private readonly QGramGenerator _qgramGenerator;
    private readonly ConcurrentDictionary<(Guid DefinitionId, Guid DataSourceId, int RowNumber),
        Dictionary<string, string>> _multiFieldCache = new();

    // Standard inverted index for fields
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>>> _globalInvertedIndex;

    // Definition-specific blocking indexes
    private readonly ConcurrentDictionary<Guid, BlockingIndex> _blockingIndexes;

    // Record storage and metadata
    private readonly ConcurrentDictionary<Guid, IRecordStore> _recordStores;
    private readonly ConcurrentDictionary<(Guid, int), RowMetadata> _rowMetadata;
    private readonly IQGramSimilarityStrategy _similarityStrategy;

    // Match definitions for blocking configuration
    private MatchDefinitionCollection _matchDefinitions;

    private bool _disposed;

    public ProductionQGramIndexerWithBlocking(IOptions<QGramIndexerWithBlockingOptions> options, ILogger<ProductionQGramIndexerWithBlocking> logger)
    {
        _options = options.Value ?? new QGramIndexerWithBlockingOptions();
        _memoryMappedStoreOptions = new MemoryMappedStoreOptions();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _qgramGenerator = new QGramGenerator(_options.QGramSize);

        _globalInvertedIndex = new ConcurrentDictionary<string, ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>>>();
        _blockingIndexes = new ConcurrentDictionary<Guid, BlockingIndex>();
        _recordStores = new ConcurrentDictionary<Guid, IRecordStore>();
        _rowMetadata = new ConcurrentDictionary<(Guid, int), RowMetadata>();

        _similarityStrategy = QGramSimilarityStrategyFactory.CreateStrategy(_options.SimilarityAlgorithm);
    }

    /// <summary>
    /// Initialize blocking configuration from match definitions
    /// </summary>
    public void InitializeBlockingConfiguration(MatchDefinitionCollection matchDefinitions)
    {
        _matchDefinitions = matchDefinitions;

        foreach (var definition in matchDefinitions.Definitions)
        {
            var blockingIndex = AnalyzeDefinitionForBlocking(definition);
            _blockingIndexes[definition.Id] = blockingIndex;

            _logger.LogInformation("Definition {DefinitionId} blocking: {UseBlocking}, Scheme: {Scheme}, Fields: {Fields}",
                definition.Id,
                blockingIndex.UseBlocking,
                blockingIndex.Scheme,
                string.Join(", ", blockingIndex.BlockingFieldMappings.Select(e => e.FieldName)));
        }
    }

    /// <summary>
    /// Analyze a definition to determine optimal blocking strategy
    /// </summary>
    private BlockingIndex AnalyzeDefinitionForBlocking(MatchLogic.Domain.Entities.MatchDefinition definition)
    {
        var blockingIndex = new BlockingIndex
        {
            DefinitionId = definition.Id,
            UseBlocking = false,
            Scheme = BlockingScheme.None
        };

        // Find exact criteria that can be used for blocking
        var blockableCriteria = definition.Criteria
            .Where(c => IsBlockableExactCriteria(c))
            .ToList();

        if (blockableCriteria.Any())
        {
            blockingIndex.UseBlocking = true;

            // Collect field mappings from blockable criteria
            foreach (var criteria in blockableCriteria)
            {
                foreach (var fieldMapping in criteria.FieldMappings)
                {
                    blockingIndex.BlockingFieldMappings.Add(
                        (fieldMapping.DataSourceId, fieldMapping.FieldName));
                }
            }

            // Determine blocking scheme
            if (blockableCriteria.Count > 1 && _options.EnableMultiFieldBlocking)
            {
                blockingIndex.Scheme = BlockingScheme.MultiField;

                // NEW: Validate configuration
                var dataSources = definition.Criteria
                    .SelectMany(c => c.FieldMappings)
                    .Select(m => m.DataSourceId)
                    .Distinct()
                    .ToList();

                foreach (var criteria in blockableCriteria)
                {
                    var sourcesInCriteria = criteria.FieldMappings
                        .Select(m => m.DataSourceId)
                        .Distinct()
                        .ToHashSet();

                    if (!dataSources.All(ds => sourcesInCriteria.Contains(ds)))
                    {
                        _logger.LogWarning(
                            "Multi-field blocking may be ineffective: Criteria {CriteriaId} doesn't have fields from all data sources",
                            criteria.Id);
                    }
                }
            }
            else
            {
                blockingIndex.Scheme = BlockingScheme.SingleField;
            }
        }
        else if (_options.EnableMultiSchemeBlocking)
        {
            // For pure fuzzy text matching, use multi-scheme
            var fuzzyTextCriteria = definition.Criteria
                .Where(c => c.MatchingType == MatchingType.Fuzzy &&
                           c.DataType == CriteriaDataType.Text)
                .ToList();

            if (fuzzyTextCriteria.Any())
            {
                blockingIndex.UseBlocking = true;
                blockingIndex.Scheme = BlockingScheme.MultiScheme;

                foreach (var criteria in fuzzyTextCriteria)
                {
                    foreach (var fieldMapping in criteria.FieldMappings)
                    {
                        blockingIndex.BlockingFieldMappings.Add(
                            (fieldMapping.DataSourceId, fieldMapping.FieldName));
                    }
                }
            }
        }

        _logger.LogInformation("Definition {DefinitionId} blocking: {UseBlocking}, Scheme: {Scheme}, Mappings: {Count}",
            definition.Id,
            blockingIndex.UseBlocking,
            blockingIndex.Scheme,
            blockingIndex.BlockingFieldMappings.Count);

        return blockingIndex;
    }

    /// <summary>
    /// Check if a criteria can be used for blocking
    /// </summary>
    private bool IsBlockableExactCriteria(MatchCriteria criteria)
    {
        // Exact text matching only (exclude phonetic and numeric)
        return criteria.MatchingType == MatchingType.Exact &&
               (criteria.DataType == CriteriaDataType.Text || criteria.DataType == CriteriaDataType.Number);
    }

    /// <summary>
    /// Enhanced indexing with blocking-aware processing
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

        // Initialize record store
        IRecordStore recordStore = config.UseInMemoryStore
            ? new InMemoryRecordStore(config.InMemoryThreshold)
            : new MemoryMappedRecordStore(config.DataSourceId, _memoryMappedStoreOptions, _logger);

        _recordStores[config.DataSourceId] = recordStore;

        // Initialize field indexes
        foreach (var fieldName in config.FieldsToIndex)
        {
            _globalInvertedIndex.TryAdd(fieldName, new ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>>());
        }

        var hashBuffer = _qgramGenerator.RentHashBuffer(1024);
        var rowNumber = 0;

        try
        {
            _logger.LogInformation("Starting blocking-aware indexing for {DataSourceName} with {FieldCount} fields",
                config.DataSourceName, config.FieldsToIndex.Count);

            await foreach (var record in records.WithCancellation(cancellationToken))
            {
                try
                {
                    // Store the record
                    await recordStore.AddRecordAsync(record);

                    var rowRef = new PairItemRowReference(config.DataSourceId, rowNumber);
                    var metadata = new RowMetadata
                    {
                        DataSourceId = config.DataSourceId,
                        RowNumber = rowNumber
                    };

                    // Process standard indexing
                    foreach (var fieldName in config.FieldsToIndex)
                    {                            
                        if (record.TryGetValue(fieldName, out var value))
                        {
                            string parseValue = Convert.ToString(value) ?? "";
                            if (string.IsNullOrWhiteSpace(parseValue))
                                continue;
                            ProcessField(fieldName, parseValue, rowRef, metadata, hashBuffer);

                            // Also update blocking indexes if configured
                            if (_matchDefinitions != null)
                            {
                                UpdateBlockingIndexes(fieldName, parseValue, rowRef, config.DataSourceId);
                            }
                        }
                    }

                    _rowMetadata[(config.DataSourceId, rowNumber)] = metadata;
                    rowNumber++;
                    result.ProcessedRecords++;

                    if (rowNumber % 10000 == 0)
                    {
                        await progressTracker.UpdateProgressAsync(rowNumber,
                            $"Indexed {rowNumber:N0} records from {config.DataSourceName}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing record {RowNumber}", rowNumber);
                }
            }

            FlushMultiFieldCache();

            await recordStore.SwitchToReadOnlyModeAsync();
            var stats = recordStore.GetStatistics();

            result.IndexingDuration = DateTime.UtcNow - startTime;
            result.StorageSizeBytes = stats.TotalSizeBytes;
            result.UsedDiskStorage = stats.StorageType.Contains("Disk") || stats.StorageType.Contains("MemoryMapped");

            _logger.LogInformation("Completed indexing {DataSourceName}: {RecordCount:N0} records in {Duration:F1}s",
                config.DataSourceName, result.ProcessedRecords, result.IndexingDuration.TotalSeconds);

            // Log blocking statistics
            LogBlockingStatistics();

            return result;
        }
        finally
        {
            _qgramGenerator.ReturnHashBuffer(hashBuffer);
        }
    }

    /// <summary>
    /// Update blocking indexes based on field values
    /// </summary>
    /// <summary>
    private void UpdateBlockingIndexes(string fieldName, string fieldValue,
        PairItemRowReference rowRef, Guid dataSourceId)
    {
        foreach (var blockingIndex in _blockingIndexes.Values)
        {
            if (!blockingIndex.UseBlocking)
                continue;

            // Check if this specific datasource+field combination is used for blocking
            var isRelevant = blockingIndex.BlockingFieldMappings
                .Any(mapping => mapping.DataSourceId == dataSourceId &&
                               mapping.FieldName == fieldName);

            if (!isRelevant)
                continue;

            switch (blockingIndex.Scheme)
            {
                case BlockingScheme.SingleField:
                    var isExactField = IsFieldUsedForExactMatching(
                        blockingIndex.DefinitionId, dataSourceId, fieldName);

                    string blockValue = isExactField
                        ? NormalizeForBlocking(fieldValue)
                        : GenerateBlockValue(fieldValue, BlockingPosition.First);

                    var blockKey = new CompositeBlockKey(dataSourceId, fieldName, blockValue);
                    AddToBlock(blockingIndex, blockKey, blockValue, rowRef);
                    break;

                case BlockingScheme.MultiField:
                    // NEW: Defer processing until all fields are available
                    StoreFieldValueForMultiFieldBlocking(
                        blockingIndex, dataSourceId, fieldName, fieldValue, rowRef);
                    break;

                case BlockingScheme.MultiScheme:
                    // Keep existing q-gram logic
                    var firstValue = GenerateBlockValue(fieldValue, BlockingPosition.First);
                    var middleValue = GenerateBlockValue(fieldValue, BlockingPosition.Middle);
                    var lastValue = GenerateBlockValue(fieldValue, BlockingPosition.Last);

                    var firstKey = new CompositeBlockKey(dataSourceId, fieldName, $"F_{firstValue}");
                    var middleKey = new CompositeBlockKey(dataSourceId, fieldName, $"M_{middleValue}");
                    var lastKey = new CompositeBlockKey(dataSourceId, fieldName, $"L_{lastValue}");

                    AddToBlock(blockingIndex, firstKey, $"F_{firstValue}", rowRef);
                    AddToBlock(blockingIndex, middleKey, $"M_{middleValue}", rowRef);
                    AddToBlock(blockingIndex, lastKey, $"L_{lastValue}", rowRef);
                    break;
            }
        }
    }

    private void StoreFieldValueForMultiFieldBlocking(
        BlockingIndex blockingIndex,
        Guid dataSourceId,
        string fieldName,
        string fieldValue,
        PairItemRowReference rowRef)
    {
        var key = (blockingIndex.DefinitionId, dataSourceId, rowRef.RowNumber);
        var fieldValues = _multiFieldCache.GetOrAdd(key, _ => new Dictionary<string, string>());

        lock (fieldValues)
        {
            fieldValues[fieldName] = fieldValue;

            // Check if we have all required fields for this definition+source
            var requiredFields = blockingIndex.BlockingFieldMappings
                .Where(m => m.DataSourceId == dataSourceId)
                .Select(m => m.FieldName)
                .ToHashSet();

            // If all fields are present, create composite key
            if (requiredFields.All(f => fieldValues.ContainsKey(f)))
            {
                CreateCompositeBlockKey(blockingIndex, dataSourceId, fieldValues, rowRef);

                // Clean up cache
                _multiFieldCache.TryRemove(key, out _);
            }
        }
    }

    private void CreateCompositeBlockKey(
        BlockingIndex blockingIndex,
        Guid dataSourceId,
        Dictionary<string, string> fieldValues,
        PairItemRowReference rowRef)
    {
        // Get definition for consistent field ordering across sources
        var definition = _matchDefinitions.Definitions.FirstOrDefault(d => d.Id == blockingIndex.DefinitionId);
        if (definition == null) return;

        // Order fields by their criteria position (not alphabetically)
        var orderedFields = new List<string>();
        foreach (var criteria in definition.Criteria)
        {
            // Only include exact text fields used for blocking
            if (!IsBlockableExactCriteria(criteria))
                continue;

            var fieldMapping = criteria.FieldMappings
                .FirstOrDefault(fm => fm.DataSourceId == dataSourceId && fieldValues.ContainsKey(fm.FieldName));

            if (fieldMapping != null && !orderedFields.Contains(fieldMapping.FieldName))
            {
                orderedFields.Add(fieldMapping.FieldName);
            }
        }

        if (!orderedFields.Any()) return;

        // Create composite blocking value using consistent ordering
        var compositeValue = string.Join("+", orderedFields.Select(f =>
        {
            var value = fieldValues[f];
            return NormalizeForBlocking(value); // All exact fields
        }));

        // Use consistent field name for composite blocks
        var compositeFieldName = $"COMPOSITE_{orderedFields.Count}FIELDS";

        var blockKey = new CompositeBlockKey(dataSourceId, compositeFieldName, compositeValue);
        AddToBlock(blockingIndex, blockKey, compositeValue, rowRef);
    }

    private void FlushMultiFieldCache()
    {
        foreach (var kvp in _multiFieldCache)
        {
            var (defId, dataSourceId, rowNumber) = kvp.Key;
            var fieldValues = kvp.Value;

            if (_blockingIndexes.TryGetValue(defId, out var blockingIndex))
            {
                var rowRef = new PairItemRowReference(dataSourceId, rowNumber);
                CreateCompositeBlockKey(blockingIndex, dataSourceId, fieldValues, rowRef);
            }
        }

        _multiFieldCache.Clear();
    }

    /// <summary>
    /// Check if a field is used for exact matching in a definition
    /// </summary>
    private bool IsFieldUsedForExactMatching(Guid definitionId, Guid dataSourceId, string fieldName)
    {
        var definition = _matchDefinitions.Definitions.FirstOrDefault(d => d.Id == definitionId);
        if (definition == null)
            return false;

        return definition.Criteria.Any(c =>
            c.MatchingType == MatchingType.Exact &&
            (c.DataType == CriteriaDataType.Text || c.DataType == CriteriaDataType.Number) &&
            c.FieldMappings.Any(fm =>
                fm.DataSourceId == dataSourceId &&
                fm.FieldName == fieldName));
    }

    /// <summary>
    /// Generate blocking key from field value
    /// </summary>
    private string GenerateBlockValue(string value, BlockingPosition position)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "EMPTY";

        var normalizedValue = NormalizeForBlocking(value);

        if (normalizedValue.Length < _options.QGramSize)
            return normalizedValue.ToUpperInvariant();

        string qgram;
        switch (position)
        {
            case BlockingPosition.First:
                qgram = normalizedValue.Substring(0, _options.QGramSize);
                break;
            case BlockingPosition.Middle:
                var midStart = (normalizedValue.Length - _options.QGramSize) / 2;
                qgram = normalizedValue.Substring(midStart, _options.QGramSize);
                break;
            case BlockingPosition.Last:
                qgram = normalizedValue.Substring(normalizedValue.Length - _options.QGramSize);
                break;
            default:
                qgram = normalizedValue.Substring(0, _options.QGramSize);
                break;
        }

        return qgram.ToUpperInvariant();
    }

    /// <summary>
    /// Normalize string for blocking (remove spaces, special chars, etc.)
    /// </summary>
    private string NormalizeForBlocking(string value)
    {
        var normalized = new StringBuilder();
        foreach (char c in value)
        {
            if (char.IsLetterOrDigit(c))
                normalized.Append(char.ToUpperInvariant(c));
        }
        return normalized.Length > 0 ? normalized.ToString() : value.Trim().ToUpperInvariant();
    }

    /// <summary>
    /// Add record to a block
    /// </summary>
    private void AddToBlock(BlockingIndex blockingIndex, CompositeBlockKey blockKey,
        string blockValue, PairItemRowReference rowRef)
    {
        // Add to specific block
        var block = blockingIndex.Blocks.GetOrAdd(blockKey, _ => new HashSet<PairItemRowReference>());
        lock (block)
        {
            block.Add(rowRef);
        }

        // Also maintain block value index for cross-source matching
        var valueIndex = blockingIndex.BlockValueIndex.GetOrAdd(blockValue, _ => new List<CompositeBlockKey>());
        lock (valueIndex)
        {
            if (!valueIndex.Any(k => k.Equals(blockKey)))
            {
                valueIndex.Add(blockKey);
            }
        }
    }

    /// <summary>
    /// Enhanced candidate generation with blocking
    /// </summary>
    public async IAsyncEnumerable<CandidatePair> GenerateCandidatesFromMatchDefinitionsAsync(
        MatchDefinitionCollection matchDefinitions,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(matchDefinitions);

        // Initialize blocking configuration if not already done
        if (_matchDefinitions == null)
        {
            InitializeBlockingConfiguration(matchDefinitions);
        }

        _logger.LogInformation("Generating candidates from {Count} match definitions with blocking optimization",
            matchDefinitions.Definitions.Count);

        // Track unique candidates across all definitions
        var globalCandidateMap = new ConcurrentDictionary<(Guid, int, Guid, int), CandidatePair>();
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

                // Use blocking if configured for this definition
                IAsyncEnumerable<CandidatePair> candidates;

                if (_blockingIndexes.TryGetValue(definition.Id, out var blockingIndex) &&
                    blockingIndex.UseBlocking)
                {
                    candidates = GenerateCandidatesWithBlockingAsync(
                        definition, blockingIndex, sourceId1, sourceId2, store1, store2, cancellationToken);
                }
                else
                {
                    candidates = GenerateCandidatesForDefinitionAsync(
                        definition, sourceId1, sourceId2, store1, store2, cancellationToken);
                }

                // Merge candidates from this definition
                await foreach (var candidate in candidates)
                {
                    var key = GetCandidateKey(candidate);

                    if (globalCandidateMap.TryGetValue(key, out var existingCandidate))
                    {
                        // Merge match definition IDs
                        existingCandidate.AddMatchDefinition(definition.Id);
                    }
                    else
                    {
                        candidate.AddMatchDefinition(definition.Id);
                        globalCandidateMap[key] = candidate;
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

        foreach (var candidate in globalCandidateMap.Values)
        {
            yield return candidate;
        }
    }

    /// <summary>
    /// Generate candidates using blocking strategy
    /// </summary>
    // Replace the existing sequential processing section with:
    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesWithBlockingAsync(
        MatchLogic.Domain.Entities.MatchDefinition definition,
        BlockingIndex blockingIndex,
        Guid sourceId1,
        Guid sourceId2,
        IRecordStore store1,
        IRecordStore store2,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _logger.LogInformation("Using blocking strategy");
        var processedPairs = new ConcurrentDictionary<(Guid, int, Guid, int), byte>();

        // Pre-compute all block pairs
        var blockPairs = PrecomputeBlockPairs(blockingIndex, sourceId1, sourceId2);

        _logger.LogInformation("Processing {Count} block pairs for definition {DefId}",
            blockPairs.Count, definition.Id);

        // Simple parallel processing without batching
        var outputChannel = Channel.CreateUnbounded<CandidatePair>();

        var processingTask = Task.Run(async () =>
        {
            await Parallel.ForEachAsync(blockPairs,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = _options.MaxParallelism,
                    CancellationToken = cancellationToken
                },
                async (blockPair, ct) =>
                {
                    var source1Records = blockPair.Item1.ToList();
                    var source2Records = sourceId1 == sourceId2 ? source1Records : blockPair.Item2.ToList();

                    await foreach (var candidate in GenerateCandidatesWithinBlockAsync(
                        definition, source1Records, source2Records, sourceId1, sourceId2,
                        store1, store2, processedPairs, ct))
                    {
                        await outputChannel.Writer.WriteAsync(candidate, ct);
                    }
                });

            outputChannel.Writer.Complete();
        }, cancellationToken);

        await foreach (var candidate in outputChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return candidate;
        }

        await processingTask;
    }

    // Add this method
    private List<(HashSet<PairItemRowReference>, HashSet<PairItemRowReference>)> PrecomputeBlockPairs(
        BlockingIndex blockingIndex,
        Guid sourceId1,
        Guid sourceId2)
    {
        var blockPairs = new List<(HashSet<PairItemRowReference>, HashSet<PairItemRowReference>)>();
        var skippedBlocks = 0;
        var skippedRecords = 0;

        if (sourceId1 == sourceId2)
        {
            // Deduplication: just get all blocks for this source
            foreach (var kvp in blockingIndex.Blocks.Where(b => b.Key.DataSourceId == sourceId1))
            {
                var blockSize = kvp.Value.Count;                    
                if (blockSize > _options.MaxBlockSize)
                {
                    skippedBlocks++;
                    skippedRecords += blockSize;

                    _logger.LogWarning(
                        "Block exceeds max size: {BlockKey} with {Size:N0} records (max: {Max:N0})",
                        kvp.Key, blockSize, _options.MaxBlockSize);

                    if (_options.BucketStrategy == BucketOptimizationStrategy.Skip)
                    {
                        _logger.LogInformation("SKIPPING block {BlockKey}", kvp.Key);
                        continue; // Skip this block entirely
                    }
                    else if (_options.BucketStrategy == BucketOptimizationStrategy.Sample)
                    {
                        // Sample the block
                        var sampleSize = Math.Min(
                            _options.MaxBlockSize,
                            (int)(blockSize * _options.BucketSamplingRate));

                        var sampledRecords = kvp.Value
                            .OrderBy(x => Guid.NewGuid())
                            .Take(sampleSize)
                            .ToHashSet();

                        _logger.LogInformation(
                            "SAMPLED block {BlockKey} from {Original:N0} to {Sample:N0} records",
                            kvp.Key, blockSize, sampledRecords.Count);

                        blockPairs.Add((sampledRecords, sampledRecords));
                    }
                    else
                    {
                        _logger.LogInformation(
                            "PROCESSING oversized block {BlockKey} (strategy: None)",
                            kvp.Key);
                        blockPairs.Add((kvp.Value, kvp.Value));
                    }
                }
                else
                {
                    // Block is within limits, add it
                    blockPairs.Add((kvp.Value, kvp.Value));
                }
            }
        }
        else
        {
            // Cross-source: pre-compute matching pairs
            var source1BlocksByValue = blockingIndex.Blocks
                .Where(b => b.Key.DataSourceId == sourceId1)
                .GroupBy(b => b.Key.BlockValue)
                .ToDictionary(g => g.Key, g => g.Select(x => x.Value).ToList());

            var source2BlocksByValue = blockingIndex.Blocks
                .Where(b => b.Key.DataSourceId == sourceId2)
                .GroupBy(b => b.Key.BlockValue)
                .ToDictionary(g => g.Key, g => g.Select(x => x.Value).ToList());

            foreach (var kvp in source1BlocksByValue)
            {
                if (source2BlocksByValue.TryGetValue(kvp.Key, out var source2BlockList))
                {
                    foreach (var s1Block in kvp.Value)
                    {
                        foreach (var s2Block in source2BlockList)
                        {
                            var totalPairs = (long)s1Block.Count * s2Block.Count;

                            // Check if total pairs exceed limit
                            var maxPairs = (long)_options.MaxBlockSize * _options.MaxBlockSize;

                            if (totalPairs > maxPairs)
                            {
                                skippedBlocks++;
                                skippedRecords += s1Block.Count + s2Block.Count;

                                _logger.LogWarning(
                                    "Block pair exceeds max: {Size1:N0} x {Size2:N0} = {Total:N0} pairs (max: {Max:N0})",
                                    s1Block.Count, s2Block.Count, totalPairs, maxPairs);

                                // Apply strategy
                                if (_options.BucketStrategy == BucketOptimizationStrategy.Skip)
                                {
                                    _logger.LogInformation(
                                        "SKIPPING block pair {Value}", kvp.Key);
                                    continue; // Skip this block pair entirely
                                }
                                else if (_options.BucketStrategy == BucketOptimizationStrategy.Sample)
                                {
                                    var maxPerBlock = (int)Math.Sqrt(maxPairs);

                                    var sample1 = s1Block
                                        .OrderBy(x => Guid.NewGuid())
                                        .Take(maxPerBlock)
                                        .ToHashSet();

                                    var sample2 = s2Block
                                        .OrderBy(x => Guid.NewGuid())
                                        .Take(maxPerBlock)
                                        .ToHashSet();

                                    _logger.LogInformation(
                                        "SAMPLED block pair from {O1:N0}x{O2:N0} to {S1:N0}x{S2:N0}",
                                        s1Block.Count, s2Block.Count, sample1.Count, sample2.Count);

                                    blockPairs.Add((sample1, sample2));
                                }
                                else
                                {
                                    _logger.LogInformation(
                                        "PROCESSING oversized block pair {Value} (strategy: None)",
                                        kvp.Key);
                                    blockPairs.Add((s1Block, s2Block));
                                }
                            }
                            else
                            {
                                // Block pair is within limits, add it
                                blockPairs.Add((s1Block, s2Block));
                            }
                        }
                    }
                }
            }
        }

        if (skippedBlocks > 0)
        {
            _logger.LogWarning(
                "Block filtering summary: Skipped {Blocks} blocks, {Records:N0} records. Processing {Valid} blocks.",
                skippedBlocks, skippedRecords, blockPairs.Count);
        }
        else
        {
            _logger.LogInformation(
                "All blocks within limits. Processing {Count} block pairs.", blockPairs.Count);
        }

        return blockPairs;
    }

    /// <summary>
    /// Generate candidates within a single block
    /// </summary>
    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesWithinBlockAsync(
        MatchLogic.Domain.Entities.MatchDefinition definition,
        List<PairItemRowReference> source1Records,
        List<PairItemRowReference> source2Records,
        Guid sourceId1,
        Guid sourceId2,
        IRecordStore store1,
        IRecordStore store2,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const int BATCH_SIZE = 100;
        var candidateBatch = new List<CandidatePair>(BATCH_SIZE);

        // Within-source (deduplication)
        if (sourceId1 == sourceId2)
        {
            for (int i = 0; i < source1Records.Count; i++)
            {
                int candidatesForThisRecord = 0; 

                for (int j = i + 1; j < source1Records.Count; j++)
                {
                    if (candidatesForThisRecord >= _options.MaxCandidatesPerRecord)
                    {
                        break; // Stop generating for this record
                    }
                    candidatesForThisRecord++;
                    var candidate = await CreateAndValidateCandidateAsync(
                        source1Records[i], source1Records[j],
                        store1, store1, definition, processedPairs);

                    if (candidate != null)
                    {
                        candidateBatch.Add(candidate);
                        

                        if (candidateBatch.Count >= BATCH_SIZE)
                        {
                            foreach (var c in candidateBatch)
                                yield return c;
                            candidateBatch.Clear();
                        }
                    }
                }
            }
        }
        else
        {
            // Cross-source matching
            foreach (var record1 in source1Records)
            {
                int candidatesForThisRecord = 0; 

                foreach (var record2 in source2Records)
                {
                    if (candidatesForThisRecord >= _options.MaxCandidatesPerRecord)
                    {
                        break; // Stop generating for this record
                    }
                    candidatesForThisRecord++;
                    var candidate = await CreateAndValidateCandidateAsync(
                        record1, record2, store1, store2,
                        definition, processedPairs);

                    if (candidate != null)
                    {
                        candidateBatch.Add(candidate);
                        

                        if (candidateBatch.Count >= BATCH_SIZE)
                        {
                            foreach (var c in candidateBatch)
                                yield return c;
                            candidateBatch.Clear();
                        }
                    }
                }
            }
        }

        // Yield remaining candidates
        foreach (var c in candidateBatch)
            yield return c;
    }

    /// <summary>
    /// Create and validate a candidate against all criteria
    /// </summary>
    private async Task<CandidatePair> CreateAndValidateCandidateAsync(
        PairItemRowReference row1,
        PairItemRowReference row2,
        IRecordStore store1,
        IRecordStore store2,
        MatchLogic.Domain.Entities.MatchDefinition definition,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs)
    {
        // Create canonical key
        var key = row1.CompareTo(row2) <= 0
            ? (row1.DataSourceId, row1.RowNumber, row2.DataSourceId, row2.RowNumber)
            : (row2.DataSourceId, row2.RowNumber, row1.DataSourceId, row1.RowNumber);

        // Skip self-pairs and already processed
        if (key.Item1 == key.Item3 && key.Item2 == key.Item4)
            return null;

        if (!processedPairs.TryAdd(key, 0))
            return null;

        // Get blocking index for this definition
        BlockingIndex blockingIndex = null;
        _blockingIndexes?.TryGetValue(definition.Id, out blockingIndex);

        // Validate only non-blocking criteria
        // Validate only non-blocking criteria
        foreach (var criteria in definition.Criteria)
        {
            // NEW: Check if ALL fields from this criteria are covered by blocking
            bool isFullyCoveredByBlocking = false;

            if (blockingIndex != null &&
                blockingIndex.UseBlocking &&
                blockingIndex.Scheme == BlockingScheme.MultiField)
            {
                // Get all fields from this criteria for this source pair
                var criteriaFields = criteria.FieldMappings
                    .Where(fm => fm.DataSourceId == row1.DataSourceId || fm.DataSourceId == row2.DataSourceId)
                    .Select(fm => (fm.DataSourceId, fm.FieldName))
                    .ToHashSet();

                // Check if ALL criteria fields are in the blocking index
                var blockingFields = blockingIndex.BlockingFieldMappings
                    .Select(bf => (bf.DataSourceId, bf.FieldName))
                    .ToHashSet();

                isFullyCoveredByBlocking = criteriaFields.All(cf => blockingFields.Contains(cf));
            }
            else if (blockingIndex != null && blockingIndex.UseBlocking)
            {
                // For SingleField or MultiScheme, use existing logic
                isFullyCoveredByBlocking = blockingIndex.BlockingFieldMappings
                    .Any(bf => criteria.FieldMappings
                        .Any(fm => fm.DataSourceId == bf.DataSourceId &&
                                  fm.FieldName == bf.FieldName));
            }

            // Skip validation only if fully covered by blocking AND it's exact text matching
            if (isFullyCoveredByBlocking &&
                criteria.MatchingType == MatchingType.Exact &&
                (criteria.DataType == CriteriaDataType.Text || criteria.DataType == CriteriaDataType.Number))
            {
                continue;
            }

            if (!await ValidateCriteriaAsync(row1, row2, criteria, store1, store2))
            {
                processedPairs.TryRemove(key, out _);
                return null;
            }
        }

        // Calculate similarity (excluding blocking fields if needed)
        var similarity = CalculateRowSimilarity(row1, row2, null);

        return new CandidatePair(
            row1.DataSourceId, row1.RowNumber,
            row2.DataSourceId, row2.RowNumber,
            store1, store2, similarity);
    }

    /// <summary>
    /// Validate a specific criteria (async for potential I/O)
    /// </summary>
    private async Task<bool> ValidateCriteriaAsync(
        PairItemRowReference row1,
        PairItemRowReference row2,
        MatchCriteria criteria,
        IRecordStore store1,
        IRecordStore store2)
    {
        // For numeric and phonetic types, defer validation to comparison service
        if ((criteria.DataType == CriteriaDataType.Number && criteria.MatchingType == MatchingType.Fuzzy)||
            criteria.DataType == CriteriaDataType.Phonetic)
        {
            return true;
        }

        // Get metadata for text validation
        var key1 = (row1.DataSourceId, row1.RowNumber);
        var key2 = (row2.DataSourceId, row2.RowNumber);

        if (!_rowMetadata.TryGetValue(key1, out var metadata1) ||
            !_rowMetadata.TryGetValue(key2, out var metadata2))
        {
            return false;
        }

        var threshold = GetThresholdForCriteria(criteria);

        if (criteria.MatchingType == MatchingType.Exact && threshold >= 0.99)
        {
            return ValidateExactMatch(metadata1, metadata2, criteria,
                row1.DataSourceId, row2.DataSourceId);
        }

        return ValidateFuzzyMatch(metadata1, metadata2, criteria,
            row1.DataSourceId, row2.DataSourceId, threshold);
    }

    /// <summary>
    /// Log blocking statistics for monitoring
    /// </summary>
    private void LogBlockingStatistics()
    {
        foreach (var kvp in _blockingIndexes)
        {
            var index = kvp.Value;
            if (!index.UseBlocking)
                continue;

            var blockSizes = index.Blocks.Values.Select(b => b.Count).ToList();
            if (!blockSizes.Any())
                continue;

            _logger.LogInformation(
                "Blocking statistics for definition {DefId}: " +
                "Blocks: {BlockCount}, " +
                "Avg size: {AvgSize:F1}, " +
                "Min: {Min}, Max: {Max}, " +
                "Median: {Median}",
                index.DefinitionId,
                blockSizes.Count,
                blockSizes.Average(),
                blockSizes.Min(),
                blockSizes.Max(),
                blockSizes.OrderBy(x => x).Skip(blockSizes.Count / 2).First());
        }
    }

    private void ProcessField(string fieldName, string fieldValue, PairItemRowReference rowRef,
        RowMetadata metadata, uint[] hashBuffer)
    {
        _qgramGenerator.GenerateHashes(fieldValue.AsSpan(), hashBuffer, out int hashCount);
        var fieldHashes = new HashSet<uint>();

        for (int i = 0; i < hashCount; i++)
        {
            var hash = hashBuffer[i];
            fieldHashes.Add(hash);

            var fieldIndex = _globalInvertedIndex[fieldName];
            var rowBag = fieldIndex.GetOrAdd(hash, _ => new ConcurrentBag<PairItemRowReference>());
            rowBag.Add(rowRef);
        }

        metadata.FieldHashes[fieldName] = fieldHashes;
    }

    /// <summary>
    /// Validate exact match criteria - optimized for quick rejection
    /// </summary>
    private bool ValidateExactMatch(
        RowMetadata metadata1,
        RowMetadata metadata2,
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2)
    {
        if (criteria.DataType == CriteriaDataType.Phonetic)
            return true;

        var fieldMappings = BuildFieldMappingsForCriteria(criteria, sourceId1, sourceId2);

        if (!fieldMappings.Any())
            return false;

        // For exact match, ALL fields must have near-perfect similarity
        foreach (var mapping in fieldMappings)
        {
            if (!metadata1.FieldHashes.TryGetValue(mapping.Key, out var hashes1) ||
                !metadata2.FieldHashes.TryGetValue(mapping.Value, out var hashes2))
            {
                return false;  // Field missing - fail immediately
            }

            // Calculate similarity for this field
            double similarity;
            if (hashes1.Count == 0 && hashes2.Count == 0)
            {
                similarity = 1.0;
            }
            else if (hashes1.Count == 0 || hashes2.Count == 0)
            {
                return false;  // One empty, one not - not exact match
            }
            else
            {
                similarity = _similarityStrategy.CalculateSimilarity(hashes1, hashes2);
            }

            if (similarity < 0.99)  // Exact match threshold
            {
                return false;  // Fail fast on first non-match
            }
        }

        return true;
    }

    /// <summary>
    /// Validate fuzzy match criteria
    /// </summary>
    private bool ValidateFuzzyMatch(
        RowMetadata metadata1,
        RowMetadata metadata2,
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2,
        double threshold)
    {
        if (criteria.DataType == CriteriaDataType.Number || criteria.DataType == CriteriaDataType.Phonetic)
            return true;

        var fieldMappings = BuildFieldMappingsForCriteria(criteria, sourceId1, sourceId2);

        if (!fieldMappings.Any())
            return false;

        // Calculate average similarity across all fields
        double totalSimilarity = 0;
        int fieldCount = 0;

        foreach (var mapping in fieldMappings)
        {
            if (!metadata1.FieldHashes.TryGetValue(mapping.Key, out var hashes1) ||
                !metadata2.FieldHashes.TryGetValue(mapping.Value, out var hashes2))
            {
                return false;  // Required field missing
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
            // Deduplication: map fields to themselves
            var fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1)
                .Select(m => m.FieldName)
                .Distinct();

            foreach (var field in fields)
            {
                if (_globalInvertedIndex.ContainsKey(field))
                {
                    fieldMappings[field] = field;
                }
            }
        }
        else
        {
            // Cross-source: map fields between sources
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
                    if (_globalInvertedIndex.ContainsKey(field1) &&
                        _globalInvertedIndex.ContainsKey(field2))
                    {
                        fieldMappings[field1] = field2;
                    }
                }
            }
        }

        return fieldMappings;
    }

    private double CalculateRowSimilarity(
        PairItemRowReference row1,
        PairItemRowReference row2,
        Dictionary<string, string> fieldMappings = null) // Optional field mappings for cross-source
    {
        if (!_rowMetadata.TryGetValue((row1.DataSourceId, row1.RowNumber), out var metadata1) ||
            !_rowMetadata.TryGetValue((row2.DataSourceId, row2.RowNumber), out var metadata2))
        {
            return 0.0;
        }

        // If field mappings provided (cross-source), use them
        // Otherwise, use common fields (same-source)
        var fieldsToCompare = new List<(string field1, string field2)>();

        if (fieldMappings != null && fieldMappings.Any())
        {
            // Cross-source: use field mappings
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
            // Same-source: use common fields
            var commonFields = metadata1.FieldHashes.Keys.Intersect(metadata2.FieldHashes.Keys);
            foreach (var field in commonFields)
            {
                fieldsToCompare.Add((field, field));
            }
        }

        if (!fieldsToCompare.Any())
            return 0.0;

        double totalSimilarity = 0;
        int fieldCount = 0;

        foreach (var (field1, field2) in fieldsToCompare)
        {
            var hashes1 = metadata1.FieldHashes[field1];
            var hashes2 = metadata2.FieldHashes[field2];

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

        return fieldCount > 0 ? totalSimilarity / fieldCount : 0.0;
    }

    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesForDefinitionAsync(
        MatchLogic.Domain.Entities.MatchDefinition definition,
        Guid sourceId1, Guid sourceId2,
        IRecordStore store1, IRecordStore store2,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var processedPairs = new ConcurrentDictionary<(Guid, int, Guid, int), byte>();
        var fieldMappings = GetFieldMappingsForSourcePair(definition, sourceId1, sourceId2);

        if (!fieldMappings.Any())
        {
            _logger.LogWarning("No field mappings found for definition {DefinitionId} between sources {Source1} and {Source2}",
                definition.Id, sourceId1, sourceId2);
            yield break;
        }

        // If only one criteria, use existing fast path
        if (definition.Criteria.Count == 1)
        {
            var criteria = definition.Criteria.First();
            var criteriaFieldMappings = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1 || m.DataSourceId == sourceId2)
                .ToList();

            // Validation logic
            if (sourceId1 == sourceId2)
            {
                if (criteriaFieldMappings.Count < 1) yield break;
            }
            else
            {
                var hasSource1 = criteriaFieldMappings.Any(m => m.DataSourceId == sourceId1);
                var hasSource2 = criteriaFieldMappings.Any(m => m.DataSourceId == sourceId2);
                if (!hasSource1 || !hasSource2) yield break;
            }

            var minThreshold = GetThresholdForCriteria(criteria);

            await foreach (var candidate in GenerateCandidatesForCriteriaAsync(
                criteria, sourceId1, sourceId2, store1, store2, minThreshold, processedPairs, cancellationToken))
            {
                yield return candidate;
            }
            yield break;
        }

        // For multiple criteria, order them by selectivity (most restrictive first)
        var orderedCriteria = definition.Criteria
            .OrderByDescending(c => c.MatchingType == MatchingType.Exact ? 2 : 0)  // Exact match first
            .ThenByDescending(c => GetThresholdForCriteria(c))  // Then highest threshold
            .ToList();

        var mostSelectiveCriteria = orderedCriteria.First();
        var otherCriteria = orderedCriteria.Skip(1).ToList();

        // Validate the most selective criteria has proper field mappings
        var mostSelectiveFieldMappings = mostSelectiveCriteria.FieldMappings
            .Where(m => m.DataSourceId == sourceId1 || m.DataSourceId == sourceId2)
            .ToList();

        if (sourceId1 == sourceId2)
        {
            if (mostSelectiveFieldMappings.Count < 1) yield break;
        }
        else
        {
            var hasSource1 = mostSelectiveFieldMappings.Any(m => m.DataSourceId == sourceId1);
            var hasSource2 = mostSelectiveFieldMappings.Any(m => m.DataSourceId == sourceId2);
            if (!hasSource1 || !hasSource2) yield break;
        }

        var mostSelectiveThreshold = GetThresholdForCriteria(mostSelectiveCriteria);

        _logger.LogDebug("Processing with most selective criteria first: {CriteriaType} with threshold {Threshold}",
            mostSelectiveCriteria.MatchingType, mostSelectiveThreshold);

        // Generate candidates from the most selective criteria
        await foreach (var candidate in GenerateCandidatesForCriteriaAsync(
            mostSelectiveCriteria, sourceId1, sourceId2, store1, store2,
            mostSelectiveThreshold, processedPairs, cancellationToken))
        {
            // Now validate against other criteria in order of selectivity
            bool satisfiesAllCriteria = true;

            foreach (var criteria in otherCriteria)
            {
                if (!ValidateCandidateAgainstCriteria(candidate, criteria, sourceId1, sourceId2))
                {
                    satisfiesAllCriteria = false;
                    break;  // Fail fast - no need to check remaining criteria
                }
            }

            if (satisfiesAllCriteria)
            {
                yield return candidate;
            }
        }
    }

    private double GetThresholdForCriteria(MatchCriteria criteria)
    {
        if (criteria.MatchingType == MatchingType.Exact && criteria.DataType != CriteriaDataType.Phonetic)
            return 0.99; // Near-perfect for exact matching

        // For fuzzy matching, try to get threshold from arguments
        if (criteria.Arguments.TryGetValue(ArgsValue.FastLevel, out var thresholdStr) &&
            double.TryParse(thresholdStr, out var threshold))
        {
            return threshold;
        }

        // Default thresholds based on data type
        return criteria.DataType switch
        {
            CriteriaDataType.Text => 0.7,
            CriteriaDataType.Number => -0.3,
            CriteriaDataType.Phonetic => -0.1,
            _ => 0.7
        };
    }

    private List<FieldMapping> GetFieldMappingsForSourcePair(MatchLogic.Domain.Entities.MatchDefinition definition, Guid sourceId1, Guid sourceId2)
    {
        var mappings = new List<FieldMapping>();

        foreach (var criteria in definition.Criteria)
        {
            var source1Mappings = criteria.FieldMappings.Where(m => m.DataSourceId == sourceId1);
            var source2Mappings = criteria.FieldMappings.Where(m => m.DataSourceId == sourceId2);

            mappings.AddRange(source1Mappings);
            if (sourceId1 != sourceId2) // Avoid duplicates for within-source matching
            {
                mappings.AddRange(source2Mappings);
            }
        }

        return mappings.Distinct().ToList();
    }

    private (Guid, int, Guid, int) GetCandidateKey(CandidatePair candidate)
    {
        // Ensure consistent ordering
        var row1 = new PairItemRowReference(candidate.DataSource1Id, candidate.Row1Number);
        var row2 = new PairItemRowReference(candidate.DataSource2Id, candidate.Row2Number);

        return row1.CompareTo(row2) <= 0
            ? (candidate.DataSource1Id, candidate.Row1Number, candidate.DataSource2Id, candidate.Row2Number)
            : (candidate.DataSource2Id, candidate.Row2Number, candidate.DataSource1Id, candidate.Row1Number);
    }

    private async IAsyncEnumerable<CandidatePair> GenerateCandidatesForCriteriaAsync(
        MatchCriteria criteria,
        Guid sourceId1, Guid sourceId2,
        IRecordStore store1, IRecordStore store2,
        double minThreshold,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Get field names for each source from the criteria's field mappings
        List<string> source1Fields;
        List<string> source2Fields;

        if (sourceId1 == sourceId2)
        {
            // Deduplication: use the same fields for both source1 and source2
            var fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1 && _globalInvertedIndex.ContainsKey(m.FieldName))
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();

            source1Fields = fields;
            source2Fields = fields; // Same fields for deduplication
        }
        else
        {
            // Cross-source: get fields for each source separately
            source1Fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId1 && _globalInvertedIndex.ContainsKey(m.FieldName))
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();

            source2Fields = criteria.FieldMappings
                .Where(m => m.DataSourceId == sourceId2 && _globalInvertedIndex.ContainsKey(m.FieldName))
                .Select(m => m.FieldName)
                .Distinct()
                .ToList();
        }

        if (!source1Fields.Any() || !source2Fields.Any())
            yield break;

        // For exact matching, we don't need similarity check since q-gram intersection is sufficient
        var effectiveThreshold = minThreshold;//criteria.MatchingType == MatchingType.Exact ? 0.0 : minThreshold;

        // Build field mappings for both cross-source and deduplication
        Dictionary<string, string> fieldMappings = new Dictionary<string, string>();

        if (sourceId1 != sourceId2)
        {
            // Cross-source: map each field from source1 to each field from source2
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
            // Deduplication: map fields to themselves (only for criteria fields)
            foreach (var field in source1Fields)
            {
                fieldMappings[field] = field;
            }
        }

        // For cross-source matching, we need to compare each field from source1 
        // with each field from source2 based on the criteria
        foreach (var field1 in source1Fields)
        {
            foreach (var field2 in source2Fields)
            {
                if (sourceId1 == sourceId2 && field1 == field2)
                {
                    // Same source with same field
                    var fieldIndex = _globalInvertedIndex[field1];
                    await foreach (var candidate in ProcessFieldPairForCandidatesAsync(
                        fieldIndex, fieldIndex, field1, field2, sourceId1, sourceId2,
                        store1, store2, effectiveThreshold, processedPairs, fieldMappings, cancellationToken))
                    {
                        yield return candidate;
                    }
                }
                else
                {
                    // Different fields or different sources
                    var fieldIndex1 = _globalInvertedIndex[field1];
                    var fieldIndex2 = _globalInvertedIndex[field2];

                    await foreach (var candidate in ProcessFieldPairForCandidatesAsync(
                        fieldIndex1, fieldIndex2, field1, field2, sourceId1, sourceId2,
                        store1, store2, effectiveThreshold, processedPairs, fieldMappings, cancellationToken))
                    {
                        yield return candidate;
                    }
                }
            }
        }
    }

    private bool ValidateCandidateAgainstCriteria(
        CandidatePair candidate,
        MatchCriteria criteria,
        Guid sourceId1,
        Guid sourceId2)
    {
        // Get the row metadata
        var key1 = (candidate.DataSource1Id, candidate.Row1Number);
        var key2 = (candidate.DataSource2Id, candidate.Row2Number);

        if (!_rowMetadata.TryGetValue(key1, out var metadata1) ||
            !_rowMetadata.TryGetValue(key2, out var metadata2))
        {
            return false;
        }

        // Get threshold for this criteria
        var threshold = GetThresholdForCriteria(criteria);

        // For exact matching with high threshold, we can do early rejection
        if (criteria.MatchingType == MatchingType.Exact && threshold >= 0.99)
        {
            // For exact match, even one field mismatch means failure
            return ValidateExactMatch(metadata1, metadata2, criteria, sourceId1, sourceId2);
        }

        // For fuzzy matching, calculate similarity
        return ValidateFuzzyMatch(metadata1, metadata2, criteria, sourceId1, sourceId2, threshold);
    }

    private async IAsyncEnumerable<CandidatePair> ProcessFieldPairForCandidatesAsync(
        ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>> fieldIndex1,
        ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>> fieldIndex2,
        string field1Name, string field2Name,
        Guid sourceId1, Guid sourceId2,
        IRecordStore store1, IRecordStore store2,
        double minThreshold,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs,
        Dictionary<string, string> fieldMappings,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Use channel for parallel processing output
        var outputChannel = Channel.CreateBounded<CandidatePair>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        // Process in parallel
        var processingTask = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(_options.MaxParallelism);
            var tasks = new List<Task>();

            // Use smaller index for iteration efficiency
            var (smallerIndex, largerIndex) = fieldIndex1.Count <= fieldIndex2.Count
                ? (fieldIndex1, fieldIndex2)
                : (fieldIndex2, fieldIndex1);

            int processedHashes = 0;
            int totalCandidates = 0;

            try
            {
                foreach (var kvp in smallerIndex)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    var hash = kvp.Key;

                    // Check if hash exists in the larger index (Fix 2: no ToList())
                    if (!largerIndex.ContainsKey(hash))
                        continue;

                    await semaphore.WaitAsync(cancellationToken);

                    var localHash = hash; // Capture for closure
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            if (fieldIndex1.TryGetValue(localHash, out var rows1) &&
                                fieldIndex2.TryGetValue(localHash, out var rows2))
                            {
                                var filteredRows1 = rows1.Where(r => r.DataSourceId == sourceId1).ToList();
                                var filteredRows2 = rows2.Where(r => r.DataSourceId == sourceId2).ToList();

                                // Generate pairs
                                if (sourceId1 == sourceId2)
                                {
                                    // Within-source: avoid duplicates and self-pairs
                                    for (int i = 0; i < filteredRows1.Count; i++)
                                    {
                                        for (int j = i + 1; j < filteredRows1.Count; j++)
                                        {
                                            var candidate = CreateCandidateIfValid(
                                                filteredRows1[i], filteredRows1[j],
                                                store1, store1, minThreshold, processedPairs, null);

                                            if (candidate != null)
                                            {
                                                await outputChannel.Writer.WriteAsync(candidate, cancellationToken);
                                                Interlocked.Increment(ref totalCandidates);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    // Cross-source
                                    foreach (var row1 in filteredRows1)
                                    {
                                        foreach (var row2 in filteredRows2)
                                        {
                                            var candidate = CreateCandidateIfValid(
                                                row1, row2, store1, store2,
                                                minThreshold, processedPairs, fieldMappings);

                                            if (candidate != null)
                                            {
                                                await outputChannel.Writer.WriteAsync(candidate, cancellationToken);
                                                Interlocked.Increment(ref totalCandidates);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, cancellationToken));

                    Interlocked.Increment(ref processedHashes);

                    // Clean up completed tasks periodically
                    if (tasks.Count >= _options.MaxParallelism * 2)
                    {
                        tasks.RemoveAll(t => t.IsCompleted);
                    }

                    // Log progress
                    if (processedHashes % 1000 == 0)
                    {
                        _logger.LogDebug("Processed {Hashes} hashes, {Candidates} candidates generated",
                            processedHashes, totalCandidates);
                    }
                }

                await Task.WhenAll(tasks);
            }
            finally
            {
                outputChannel.Writer.Complete();
                semaphore.Dispose();
            }

            _logger.LogDebug("Completed processing {Hashes} hashes, total {Candidates} candidates",
                processedHashes, totalCandidates);
        }, cancellationToken);

        // Stream results as they're generated
        await foreach (var candidate in outputChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return candidate;
        }

        await processingTask;
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
            // Single data source - within-source matching (deduplication)
            pairs.Add((dataSourceIds[0], dataSourceIds[0]));
        }
        else if (dataSourceIds.Count == 2)
        {
            // Two data sources - cross-source matching
            pairs.Add((dataSourceIds[0], dataSourceIds[1]));
        }
        else if (dataSourceIds.Count > 2)
        {
            // Multiple data sources - generate all pairs
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

    private CandidatePair CreateCandidateIfValid(
        PairItemRowReference row1, PairItemRowReference row2,
        IRecordStore store1, IRecordStore store2,
        double minSimilarityThreshold,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs,
        Dictionary<string, string> fieldMappings = null)
    {
        // Create consistent ordering key
        var key = row1.CompareTo(row2) <= 0
            ? (row1.DataSourceId, row1.RowNumber, row2.DataSourceId, row2.RowNumber)
            : (row2.DataSourceId, row2.RowNumber, row1.DataSourceId, row1.RowNumber);

        // Skip if same record (self-pair)
        if (key.Item1 == key.Item3 && key.Item2 == key.Item4)
            return null;

        // Skip if already processed
        if (!processedPairs.TryAdd(key, 0))
            return null;

        // Calculate similarity ONLY if threshold is meaningful
        var similarity = 0.0;
        if (minSimilarityThreshold > 0)
        {
            similarity = CalculateRowSimilarity(row1, row2, fieldMappings);
            if (similarity < minSimilarityThreshold)
            {
                // Remove from processed since we're not creating the candidate
                processedPairs.TryRemove(key, out _);
                return null;
            }
        }

        return new CandidatePair(
            row1.DataSourceId, row1.RowNumber,
            row2.DataSourceId, row2.RowNumber,
            store1, store2, similarity);
    }

    public async Task<IDictionary<string, object>> GetRecordAsync(Guid dataSourceId, int rowNumber)
    {
        if (_recordStores.TryGetValue(dataSourceId, out var store))
        {
            return await store.GetRecordAsync(rowNumber);
        }
        return null;
    }

    /// <summary>
    /// Get multiple records efficiently
    /// </summary>
    public async Task<IList<IDictionary<string, object>>> GetRecordsAsync(
        Guid dataSourceId, IEnumerable<int> rowNumbers)
    {
        if (_recordStores.TryGetValue(dataSourceId, out var store))
        {
            return await store.GetRecordsAsync(rowNumbers);
        }
        return new List<IDictionary<string, object>>();
    }

    /// <summary>
    /// Generate within-source candidate pairs for deduplication
    /// </summary>
    public async IAsyncEnumerable<CandidatePair> GenerateWithinSourceCandidatesAsync(
        Guid sourceId, List<string> fieldNames,
        double minSimilarityThreshold = 0.1,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!_recordStores.TryGetValue(sourceId, out var store))
        {
            _logger.LogError("Record store not found for source {SourceId}", sourceId);
            yield break;
        }

        var processedPairs = new ConcurrentDictionary<(Guid, int, Guid, int), byte>();
        var candidateCount = 0;

        foreach (var fieldName in fieldNames)
        {
            if (!_globalInvertedIndex.TryGetValue(fieldName, out var fieldIndex))
                continue;

            await foreach (var candidate in ProcessFieldForCandidatesAsync(
                fieldIndex, sourceId, sourceId, store, store,
                minSimilarityThreshold, processedPairs, cancellationToken))
            {
                candidateCount++;
                yield return candidate;

                if (candidateCount % 10000 == 0)
                {
                    _logger.LogDebug("Generated {Count} within-source candidates for field {Field}", candidateCount, fieldName);
                }
            }
        }

        _logger.LogInformation("Generated {Total} within-source candidates for {SourceId}",
            candidateCount, sourceId);
    }

    /// <summary>
    /// Legacy method: Generate cross-source candidates by field names (for backward compatibility)
    /// </summary>
    /// <summary>
    /// Legacy method: Generate cross-source candidates by field names (for backward compatibility)
    /// </summary>
    public async IAsyncEnumerable<CandidatePair> GenerateCrossSourceCandidatesAsync(
        Guid sourceId1, Guid sourceId2, List<string> fieldNames,
        double minSimilarityThreshold = 0.1,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!_recordStores.TryGetValue(sourceId1, out var store1) ||
            !_recordStores.TryGetValue(sourceId2, out var store2))
        {
            _logger.LogError("Record stores not found for sources {Source1}, {Source2}", sourceId1, sourceId2);
            yield break;
        }

        var processedPairs = new ConcurrentDictionary<(Guid, int, Guid, int), byte>();
        var candidateCount = 0;

        _logger.LogInformation("Generating cross-source candidates between {Source1} and {Source2} using {FieldCount} fields",
            sourceId1, sourceId2, fieldNames.Count);

        // For legacy method, we need to find which fields from source2 might correspond to the given fields
        // We'll check all indexed fields from source2 and compare with fields from source1
        var source2IndexedFields = _globalInvertedIndex.Keys
            .Where(fieldName =>
            {
                // Check if this field has any entries from source2
                if (_globalInvertedIndex.TryGetValue(fieldName, out var index))
                {
                    return index.Values.Any(bag => bag.Any(r => r.DataSourceId == sourceId2));
                }
                return false;
            })
            .ToList();

        // If no fields specified or found for source2, we can't proceed
        if (!source2IndexedFields.Any())
        {
            _logger.LogWarning("No indexed fields found for source {Source2}", sourceId2);
            yield break;
        }

        // For each field from source1, try to match with ALL fields from source2
        // This is a brute-force approach for the legacy method
        foreach (var field1Name in fieldNames)
        {
            if (!_globalInvertedIndex.TryGetValue(field1Name, out var field1Index))
            {
                _logger.LogWarning("No index found for field {FieldName} in source1", field1Name);
                continue;
            }

            // Check if field1Index has entries from source1
            var hasSource1Entries = field1Index.Values.Any(bag => bag.Any(r => r.DataSourceId == sourceId1));
            if (!hasSource1Entries)
            {
                _logger.LogWarning("Field {FieldName} has no entries from source {Source1}", field1Name, sourceId1);
                continue;
            }

            // Try matching with each field from source2
            foreach (var field2Name in source2IndexedFields)
            {
                if (!_globalInvertedIndex.TryGetValue(field2Name, out var field2Index))
                    continue;

                _logger.LogDebug("Comparing field {Field1} from source1 with field {Field2} from source2",
                    field1Name, field2Name);

                // Create field mappings for similarity calculation
                var fieldMappings = new Dictionary<string, string> { { field1Name, field2Name } };

                // Process this field pair
                await foreach (var candidate in ProcessFieldPairForCandidatesAsync(
                    field1Index, field2Index, field1Name, field2Name, sourceId1, sourceId2,
                    store1, store2, minSimilarityThreshold, processedPairs, fieldMappings, cancellationToken))
                {
                    candidateCount++;
                    yield return candidate;

                    if (candidateCount % 10000 == 0)
                    {
                        _logger.LogDebug("Generated {Count} candidates for fields {Field1}-{Field2}",
                            candidateCount, field1Name, field2Name);
                    }
                }
            }
        }

        _logger.LogInformation("Generated {Total} cross-source candidates between {Source1} and {Source2}",
            candidateCount, sourceId1, sourceId2);
    }

    /// <summary>
    /// Process a specific field index to generate candidates
    /// </summary>
    private async IAsyncEnumerable<CandidatePair> ProcessFieldForCandidatesAsync(
        ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>> fieldIndex,
        Guid sourceId1, Guid sourceId2,
        IRecordStore store1, IRecordStore store2,
        double minSimilarityThreshold,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<CandidatePair>(new BoundedChannelOptions(_options.CandidateChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        var processingTask = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(_options.MaxParallelism);
            var tasks = new List<Task>();

            try
            {
                foreach (var hashBucket in fieldIndex)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    await semaphore.WaitAsync(cancellationToken);

                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            var candidates = ProcessHashBucket(hashBucket.Value, sourceId1, sourceId2,
                                store1, store2, minSimilarityThreshold, processedPairs);

                            foreach (var candidate in candidates)
                            {
                                await channel.Writer.WriteAsync(candidate, cancellationToken);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error processing hash bucket");
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, cancellationToken));

                    // Limit concurrent tasks to prevent memory issues
                    if (tasks.Count >= _options.MaxParallelism * 2)
                    {
                        var completedTask = await Task.WhenAny(tasks);
                        tasks.Remove(completedTask);
                    }
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in candidate generation processing");
            }
            finally
            {
                channel.Writer.Complete();
                semaphore.Dispose();
            }
        }, cancellationToken);

        await foreach (var candidate in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return candidate;
        }

        await processingTask;
    }

    private List<CandidatePair> ProcessHashBucket(
        ConcurrentBag<PairItemRowReference> rows,
        Guid sourceId1, Guid sourceId2,
        IRecordStore store1, IRecordStore store2,
        double minSimilarityThreshold,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs)
    {
        var candidates = new List<CandidatePair>();
        var rowList = rows.ToList();

        var source1Rows = rowList.Where(r => r.DataSourceId == sourceId1).ToList();
        var source2Rows = rowList.Where(r => r.DataSourceId == sourceId2).ToList();

        // Apply bucket optimization strategy only for VERY large buckets
        if (_options.BucketStrategy != BucketOptimizationStrategy.None)
        {
            bool shouldOptimize = false;

            // Check if optimization is needed
            if (sourceId1 == sourceId2 && source1Rows.Count > _options.MaxRecordsPerHashBucket)
            {
                shouldOptimize = true;
            }
            else if (sourceId1 != sourceId2 &&
                    (source1Rows.Count > _options.MaxRecordsPerHashBucket ||
                     source2Rows.Count > _options.MaxRecordsPerHashBucket))
            {
                shouldOptimize = true;
            }

            if (shouldOptimize)
            {
                switch (_options.BucketStrategy)
                {
                    case BucketOptimizationStrategy.Skip:
                        _logger.LogWarning("Skipping oversized bucket: source1={Count1}, source2={Count2}",
                            source1Rows.Count, source2Rows.Count);
                        return candidates; // Return empty list

                    case BucketOptimizationStrategy.Sample:
                        if (source1Rows.Count > _options.MaxRecordsPerHashBucket)
                        {
                            var sampleSize1 = Math.Max(100, (int)(source1Rows.Count * _options.BucketSamplingRate));
                            _logger.LogDebug("Sampling source1 bucket from {Original} to {Sample} records",
                                source1Rows.Count, sampleSize1);
                            source1Rows = source1Rows.OrderBy(x => Guid.NewGuid()).Take(sampleSize1).ToList();
                        }

                        if (sourceId1 != sourceId2 && source2Rows.Count > _options.MaxRecordsPerHashBucket)
                        {
                            var sampleSize2 = Math.Max(100, (int)(source2Rows.Count * _options.BucketSamplingRate));
                            _logger.LogDebug("Sampling source2 bucket from {Original} to {Sample} records",
                                source2Rows.Count, sampleSize2);
                            source2Rows = source2Rows.OrderBy(x => Guid.NewGuid()).Take(sampleSize2).ToList();
                        }
                        break;
                }
            }
        }

        // Determine field mappings for cross-source scenario
        Dictionary<string, string> fieldMappings = null;
        if (sourceId1 != sourceId2)
        {
            // For legacy methods, we don't have field mappings
            // This is acceptable for backward compatibility
        }

        if (sourceId1 == sourceId2)
        {
            // Within-source pairs (deduplication)
            for (int i = 0; i < source1Rows.Count; i++)
            {
                int candidatesForThisRecord = 0;

                for (int j = i + 1; j < source1Rows.Count; j++)
                {
                    candidatesForThisRecord++;
                    var candidate = CreateCandidateIfValid(source1Rows[i], source1Rows[j],
                        store1, store1, minSimilarityThreshold, processedPairs, null);

                    if (candidate != null)
                    {
                        candidates.Add(candidate);
                       

                        // Limit candidates per record within this bucket
                        if (candidatesForThisRecord >= _options.MaxCandidatesPerRecord)
                            break;
                    }
                }
            }
        }
        else
        {
            // Cross-source pairs
            foreach (var row1 in source1Rows)
            {
                int candidatesForThisRecord = 0;

                foreach (var row2 in source2Rows)
                {
                    candidatesForThisRecord++;
                    var candidate = CreateCandidateIfValid(row1, row2,
                        store1, store2, minSimilarityThreshold, processedPairs, fieldMappings);

                    if (candidate != null)
                    {
                        candidates.Add(candidate);
                        

                        // Limit candidates per record within this bucket
                        if (candidatesForThisRecord >= _options.MaxCandidatesPerRecord)
                            break;
                    }
                }
            }
        }

        return candidates;
    }

    /// <summary>
    /// Get comprehensive statistics about the indexer
    /// </summary>
    public IndexerStatistics GetStatistics()
    {
        var stats = new IndexerStatistics
        {
            TotalDataSources = _recordStores.Count,
            TotalRecords = _rowMetadata.Count,
            TotalIndexedFields = _globalInvertedIndex.Count,
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
        // In a production system, we might implement selective cache clearing
        // For now, this is a placeholder for future optimization
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        _logger.LogInformation("Caches cleared and garbage collection completed");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            foreach (var store in _recordStores.Values)
            {
                store.Dispose();
            }

            _qgramGenerator?.Dispose();
            _globalInvertedIndex.Clear();
            _recordStores.Clear();
            _rowMetadata.Clear();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during indexer disposal");
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


    private long EstimateIndexSize()
    {
        long size = 0;
        foreach (var field in _globalInvertedIndex)
        {
            size += field.Key.Length * 2; // Field name
            size += field.Value.Count * (4 + 16); // Hash + average PairItemRowReference size
        }
        return size;
    }

    private enum BlockingPosition
    {
        First,
        Middle,
        Last
    }
}