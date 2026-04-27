using MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.DataProfiling;
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

public partial class ProductionQGramIndexer : IProductionQGramIndexer
{
    private readonly QGramIndexerOptions _options;
    private readonly MemoryMappedStoreOptions _memoryMappedStoreOptions;
    private readonly ILogger<ProductionQGramIndexer> _logger;
    private readonly QGramGenerator _qgramGenerator;
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>>> _globalInvertedIndex;
    private readonly ConcurrentDictionary<Guid, IRecordStore> _recordStores;
    private readonly ConcurrentDictionary<(Guid, int), RowMetadata> _rowMetadata;
    private readonly IQGramSimilarityStrategy _similarityStrategy;
    private bool _disposed;

    public ProductionQGramIndexer(IOptions<QGramIndexerOptions> options, ILogger<ProductionQGramIndexer> logger)
    {
        _options = options.Value ?? new QGramIndexerOptions();
        _memoryMappedStoreOptions = new MemoryMappedStoreOptions();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _qgramGenerator = new QGramGenerator(_options.QGramSize);
        _globalInvertedIndex = new ConcurrentDictionary<string, ConcurrentDictionary<uint, ConcurrentBag<PairItemRowReference>>>();
        _recordStores = new ConcurrentDictionary<Guid, IRecordStore>();
        _rowMetadata = new ConcurrentDictionary<(Guid, int), RowMetadata>();

        _similarityStrategy = QGramSimilarityStrategyFactory.CreateStrategy(_options.SimilarityAlgorithm);
    }

    /// <summary>
    /// Index a data source with comprehensive error handling and optimization
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
            _logger.LogInformation("Starting indexing for {DataSourceName} with {FieldCount} fields",
                config.DataSourceName, config.FieldsToIndex.Count);

            await foreach (var record in records.WithCancellation(cancellationToken))
            {
                try
                {
                    // Store the record
                    await recordStore.AddRecordAsync(record);

                    // Create row reference
                    var rowRef = new PairItemRowReference(config.DataSourceId, rowNumber);
                    var metadata = new RowMetadata
                    {
                        DataSourceId = config.DataSourceId,
                        RowNumber = rowNumber
                    };

                    // Process each field
                    foreach (var fieldName in config.FieldsToIndex)
                    {
                        if (record.TryGetValue(fieldName, out var value) &&
                            value is string stringValue &&
                            !string.IsNullOrWhiteSpace(stringValue))
                        {
                            ProcessField(fieldName, stringValue, rowRef, metadata, hashBuffer);
                        }
                    }

                    // Store metadata
                    _rowMetadata[(config.DataSourceId, rowNumber)] = metadata;

                    rowNumber++;
                    result.ProcessedRecords++;

                    // Switch to disk storage if threshold exceeded
                    if (config.UseInMemoryStore && rowNumber >= config.InMemoryThreshold)
                    {
                        _logger.LogWarning("Record count {Count} exceeded threshold {Threshold}. Consider using disk storage.",
                            rowNumber, config.InMemoryThreshold);
                    }

                    // Progress update
                    if (rowNumber % 10000 == 0)
                    {
                        await progressTracker.UpdateProgressAsync(rowNumber,
                            $"Indexed {rowNumber:N0} records from {config.DataSourceName}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing record {RowNumber} from {DataSourceName}",
                        rowNumber, config.DataSourceName);
                    // Continue processing other records
                }
            }

            // Finalize storage
            await recordStore.SwitchToReadOnlyModeAsync();
            var stats = recordStore.GetStatistics();

            result.IndexingDuration = DateTime.UtcNow - startTime;
            result.StorageSizeBytes = stats.TotalSizeBytes;
            result.UsedDiskStorage = stats.StorageType.Contains("Disk");

            _logger.LogInformation("Completed indexing {DataSourceName}: {RecordCount:N0} records in {Duration}",
                config.DataSourceName, result.ProcessedRecords, result.IndexingDuration);

            return result;
        }
        finally
        {
            _qgramGenerator.ReturnHashBuffer(hashBuffer);
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
    /// Generate candidate pairs based on match definitions (the primary method)
    /// </summary>
    public async IAsyncEnumerable<CandidatePair> GenerateCandidatesFromMatchDefinitionsAsync(
        MatchDefinitionCollection matchDefinitions,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(matchDefinitions);

        _logger.LogInformation("Generating candidates from {Count} match definitions", matchDefinitions.Definitions.Count);

        // Group candidates by record pair to merge match definition IDs
        var candidateMap = new ConcurrentDictionary<(Guid, int, Guid, int), CandidatePair>();
        var totalCandidates = 0;

        foreach (var definition in matchDefinitions.Definitions)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            _logger.LogDebug("Processing match definition {DefinitionId}: {Name}", definition.Id, definition.Name);

            // Get data source pairs for this definition
            var dataSourcePairs = GetDataSourcePairsFromDefinition(definition);

            foreach (var (sourceId1, sourceId2) in dataSourcePairs)
            {
                if (!_recordStores.TryGetValue(sourceId1, out var store1) ||
                    !_recordStores.TryGetValue(sourceId2, out var store2))
                {
                    _logger.LogWarning("Skipping definition {DefinitionId}: missing record stores for sources {Source1}, {Source2}",
                        definition.Id, sourceId1, sourceId2);
                    continue;
                }

                // Generate candidates for this definition and data source pair
                await foreach (var candidate in GenerateCandidatesForDefinitionAsync(
                    definition, sourceId1, sourceId2, store1, store2, cancellationToken))
                {
                    var key = GetCandidateKey(candidate);

                    if (candidateMap.TryGetValue(key, out var existingCandidate))
                    {
                        // Merge match definition IDs
                        existingCandidate.AddMatchDefinition(definition.Id);
                    }
                    else
                    {
                        // New candidate
                        candidate.AddMatchDefinition(definition.Id);
                        candidateMap[key] = candidate;
                        totalCandidates++;
                    }
                }
            }
        }

        _logger.LogInformation("Generated {Count} unique candidate pairs from match definitions", totalCandidates);

        // Return all unique candidates
        foreach (var candidate in candidateMap.Values)
        {
            yield return candidate;
        }
    }

    /// <summary>
    /// Generate candidates for a specific match definition and data source pair
    /// </summary>
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

    /// <summary>
    /// Generate candidates for a specific criteria within a match definition
    /// </summary>
    /// 

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

    /*private async IAsyncEnumerable<CandidatePair> GenerateCandidatesForCriteriaAsync(
        MatchCriteria criteria,
        Guid sourceId1, Guid sourceId2,
        IRecordStore store1, IRecordStore store2,
        double minThreshold,
        ConcurrentDictionary<(Guid, int, Guid, int), byte> processedPairs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Get field names for this criteria that are indexed
        var source1Fields = criteria.FieldMappings
            .Where(m => m.DataSourceId == sourceId1 && _globalInvertedIndex.ContainsKey(m.FieldName))
            .Select(m => m.FieldName)
            .Distinct()
            .ToList();

        var source2Fields = criteria.FieldMappings
            .Where(m => m.DataSourceId == sourceId2 && _globalInvertedIndex.ContainsKey(m.FieldName))
            .Select(m => m.FieldName)
            .Distinct()
            .ToList();

        if (!source1Fields.Any() || !source2Fields.Any())
            yield break;

        // For each field combination in this criteria
        foreach (var field1 in source1Fields)
        {
            foreach (var field2 in source2Fields)
            {
                // Skip if it's the same field and same source (for within-source matching)
                if (sourceId1 == sourceId2 && field1 == field2)
                    continue;

                var fieldIndex1 = _globalInvertedIndex[field1];
                var fieldIndex2 = sourceId1 == sourceId2 ? fieldIndex1 : _globalInvertedIndex[field2];

                await foreach (var candidate in ProcessFieldPairForCandidatesAsync(
                    fieldIndex1, fieldIndex2, field1, field2, sourceId1, sourceId2,
                    store1, store2, minThreshold, processedPairs, cancellationToken))
                {
                    yield return candidate;
                }
            }
        }
    }*/

    /// <summary>
    /// Process a pair of field indexes to find candidate pairs
    /// </summary>
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
        var outputChannel = Channel.CreateBounded<CandidatePair>(new BoundedChannelOptions(10000)
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
                // FIX: Handle same-index case properly
                if (ReferenceEquals(fieldIndex1, fieldIndex2))
                {
                    // Same index - process each hash bucket for within-source matching
                    foreach (var kvp in fieldIndex1)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        var hash = kvp.Key;
                        await semaphore.WaitAsync(cancellationToken);

                        tasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                var rows = kvp.Value.Where(r => r.DataSourceId == sourceId1).ToList();

                                // Generate within-source pairs from this hash bucket
                                if (sourceId1 == sourceId2)
                                {
                                    for (int i = 0; i < rows.Count; i++)
                                    {
                                        for (int j = i + 1; j < rows.Count; j++)
                                        {
                                            var candidate = CreateCandidateIfValid(
                                                rows[i], rows[j],
                                                store1, store1, minThreshold, processedPairs, fieldMappings);

                                            if (candidate != null)
                                            {
                                                await outputChannel.Writer.WriteAsync(candidate, cancellationToken);
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
                    }
                }
                else
                {
                    // Different indexes - find common hashes (existing logic)
                    var (smallerIndex, largerIndex) = fieldIndex1.Count <= fieldIndex2.Count
                        ? (fieldIndex1, fieldIndex2)
                        : (fieldIndex2, fieldIndex1);

                    foreach (var kvp in smallerIndex)
                    {
                        if (!largerIndex.ContainsKey(kvp.Key))
                            continue;

                        // Process matching hash buckets...
                    }
                }

                await Task.WhenAll(tasks);
            }
            finally
            {
                outputChannel.Writer.Complete();
                semaphore.Dispose();
            }
        }, cancellationToken);

        await foreach (var candidate in outputChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return candidate;
        }

        await processingTask;
    }

    /// <summary>
    /// Get data source pairs from a match definition
    /// </summary>
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

    /// <summary>
    /// Get field mappings for a specific source pair from a match definition
    /// </summary>
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

    /// <summary>
    /// Get similarity threshold for a criteria
    /// </summary>
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

    /// <summary>
    /// Validate if a candidate pair satisfies a specific criteria
    /// Uses fail-fast approach for efficiency
    /// </summary>
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

    /// <summary>
    /// Build field mappings for a specific criteria
    /// </summary>
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

    /// <summary>
    /// Create a unique key for a candidate pair
    /// </summary>
    private (Guid, int, Guid, int) GetCandidateKey(CandidatePair candidate)
    {
        // Ensure consistent ordering
        var row1 = new PairItemRowReference(candidate.DataSource1Id, candidate.Row1Number);
        var row2 = new PairItemRowReference(candidate.DataSource2Id, candidate.Row2Number);

        return row1.CompareTo(row2) <= 0
            ? (candidate.DataSource1Id, candidate.Row1Number, candidate.DataSource2Id, candidate.Row2Number)
            : (candidate.DataSource2Id, candidate.Row2Number, candidate.DataSource1Id, candidate.Row1Number);
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

    /// <summary>
    /// Process a single hash bucket to find candidate pairs
    /// </summary>
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
                    var candidate = CreateCandidateIfValid(source1Rows[i], source1Rows[j],
                        store1, store1, minSimilarityThreshold, processedPairs, null);

                    if (candidate != null)
                    {
                        candidates.Add(candidate);
                        candidatesForThisRecord++;

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
                    var candidate = CreateCandidateIfValid(row1, row2,
                        store1, store2, minSimilarityThreshold, processedPairs, fieldMappings);

                    if (candidate != null)
                    {
                        candidates.Add(candidate);
                        candidatesForThisRecord++;

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
    /// Create a candidate pair if it meets all criteria
    /// </summary>
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

    /// <summary>
    /// Calculate similarity between two rows based on q-gram overlap
    /// </summary>
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

    /// <summary>
    /// Get record by data source and row number
    /// </summary>
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

    /// <summary>
    /// Clear cached data to free memory
    /// </summary>
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
}
