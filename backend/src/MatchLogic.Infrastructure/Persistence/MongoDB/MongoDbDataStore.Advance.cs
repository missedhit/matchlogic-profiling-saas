using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence.MongoDB;

/// <summary>
/// Partial class containing advanced operations: sorting, filtering, and sampling
/// </summary>
public partial class MongoDbDataStore
{
    #region Sorting and Filtering

    private static readonly List<string> MetaFieldsName = new()
    {
        "GroupId",
        "PairId",
        "FinalScore",
        "WeightedScore"
    };

    #region Main Query Method

    /// <summary>
    /// Get paged data with comprehensive filtering, sorting, and band support
    /// </summary>
    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)>
        GetPagedJobWithSortingAndFilteringDataAsync(
            string collectionName,
            int pageNumber,
            int pageSize,
            string filterText = null,
            string sortColumn = null,
            bool ascending = true,
            string filters = "")
    {
        try
        {
            var collection = GetCollection(collectionName);
            var filterBuilder = Builders<BsonDocument>.Filter;
            var combinedFilters = new List<FilterDefinition<BsonDocument>>();

            // 1. Text search filter
            if (!string.IsNullOrWhiteSpace(filterText))
            {
                var searchConditions = await BuildEnhancedSearchConditionsAsync(
                    collection, filterText, collectionName);

                if (searchConditions.Any())
                {
                    combinedFilters.Add(filterBuilder.Or(searchConditions));
                }
            }

            // 2. FieldScores level filters (existing filters parameter)
            if (!string.IsNullOrWhiteSpace(filters))
            {
                var levelFilter = BuildFilterQuery(filters);
                if (levelFilter != FilterDefinition<BsonDocument>.Empty)
                {
                    combinedFilters.Add(levelFilter);
                }
            }

            // Combine all filters with AND
            var finalFilter = combinedFilters.Any()
                ? filterBuilder.And(combinedFilters)
                : FilterDefinition<BsonDocument>.Empty;

            // Get total count with filter
            var totalCount = (int)await collection.CountDocumentsAsync(finalFilter);

            // Build find query
            var findFluent = collection.Find(finalFilter);

            // Apply sorting
            if (!string.IsNullOrWhiteSpace(sortColumn) && IsValidSortField(sortColumn))
            {
                var sortField = GetSortColumn(sortColumn, collectionName);
                var sortDefinition = ascending
                    ? Builders<BsonDocument>.Sort.Ascending(sortField)
                    : Builders<BsonDocument>.Sort.Descending(sortField);
                findFluent = findFluent.Sort(sortDefinition);
            }

            // Apply pagination
            var documents = await findFluent
                .Skip((pageNumber - 1) * pageSize)
                .Limit(pageSize)
                .ToListAsync();

            var data = documents.Select(MongoDbBsonConverter.ConvertBsonDocumentToDictionary).ToList();

            _logger.LogInformation(
                "Query Results: Collection={Collection}, Page={Page}, Results={Count}/{Total}, " +
                "TextFilter={HasText}, FieldFilters={HasFields}",
                collectionName, pageNumber, data.Count, totalCount,
                !string.IsNullOrEmpty(filterText), !string.IsNullOrEmpty(filters));

            return (data, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching paged data for collection {Collection}", collectionName);
            throw;
        }
    }

    #endregion

    #region Enhanced Search (Dynamic Fields)

    /// <summary>
    /// Build search conditions for all field types dynamically
    /// </summary>
    private async Task<List<FilterDefinition<BsonDocument>>> BuildEnhancedSearchConditionsAsync(
        IMongoCollection<BsonDocument> collection,
        string searchText,
        string collectionName)
    {
        var conditions = new List<FilterDefinition<BsonDocument>>();
        var filterBuilder = Builders<BsonDocument>.Filter;

        // FIXED: Use FilterDefinition<BsonDocument>.Empty instead of just "Empty"
        var sampleDoc = await collection.Find(FilterDefinition<BsonDocument>.Empty)
            .Limit(1)
            .FirstOrDefaultAsync();

        if (sampleDoc == null)
        {
            _logger.LogWarning("No documents found in collection {Collection}", collectionName);
            return conditions;
        }

        // Parse search text as different types
        bool isInt = int.TryParse(searchText, out int intValue);
        bool isLong = long.TryParse(searchText, out long longValue);
        bool isDouble = double.TryParse(searchText, out double doubleValue);
        bool isDate = DateTime.TryParse(searchText, out DateTime dateValue);

        // Search top-level fields
        var excludedFields = new HashSet<string>
        {
            ID_FIELD, GROUP_HASH, RECORD1, RECORD2, RECORDS, FIELD_SCORES, METADATA
        };

        foreach (var field in sampleDoc.Names.Where(k => !excludedFields.Contains(k)))
        {
            AddFieldConditions(field, searchText, isInt, intValue, isLong, longValue,
                isDouble, doubleValue, isDate, dateValue, filterBuilder, conditions);
        }

        // Search Record1 fields (pairs collection)
        if (sampleDoc.Contains(RECORD1))
        {
            var record1Doc = sampleDoc[RECORD1].AsBsonDocument;
            foreach (var field in record1Doc.Names.Where(f => f != METADATA && f != "_id"))
            {
                var fieldPath = $"{RECORD1}.{field}";
                AddFieldConditions(fieldPath, searchText, isInt, intValue, isLong, longValue,
                    isDouble, doubleValue, isDate, dateValue, filterBuilder, conditions);
            }

            // RowNumber search
            if (isInt)
                conditions.Add(filterBuilder.Eq($"{RECORD1}.{METADATA}.RowNumber", intValue));
            if (isLong)
                conditions.Add(filterBuilder.Eq($"{RECORD1}.{METADATA}.RowNumber", longValue));
        }

        // Search Record2 fields (pairs collection)
        if (sampleDoc.Contains(RECORD2))
        {
            var record2Doc = sampleDoc[RECORD2].AsBsonDocument;
            foreach (var field in record2Doc.Names.Where(f => f != METADATA && f != "_id"))
            {
                var fieldPath = $"{RECORD2}.{field}";
                AddFieldConditions(fieldPath, searchText, isInt, intValue, isLong, longValue,
                    isDouble, doubleValue, isDate, dateValue, filterBuilder, conditions);
            }

            if (isInt)
                conditions.Add(filterBuilder.Eq($"{RECORD2}.{METADATA}.RowNumber", intValue));
            if (isLong)
                conditions.Add(filterBuilder.Eq($"{RECORD2}.{METADATA}.RowNumber", longValue));
        }

        // Search Records array (groups collection)
        if (sampleDoc.Contains(RECORDS) && sampleDoc[RECORDS].IsBsonArray)
        {
            var recordsArray = sampleDoc[RECORDS].AsBsonArray;
            // FIXED: Should be .Count not > 0 comparison
            if (recordsArray.Count > 0)
            {
                var firstRecord = recordsArray[0].AsBsonDocument;
                var elemMatchConditions = new List<FilterDefinition<BsonDocument>>();

                foreach (var field in firstRecord.Names.Where(f => f != METADATA && f != "_id"))
                {
                    var fieldConditions = new List<FilterDefinition<BsonDocument>>();

                    // String
                    fieldConditions.Add(Builders<BsonDocument>.Filter.Regex(field,
                        new BsonRegularExpression(searchText, "i")));

                    // Numeric
                    if (isInt)
                        fieldConditions.Add(Builders<BsonDocument>.Filter.Eq(field, intValue));
                    if (isLong)
                        fieldConditions.Add(Builders<BsonDocument>.Filter.Eq(field, longValue));
                    if (isDouble)
                        fieldConditions.Add(Builders<BsonDocument>.Filter.Eq(field, doubleValue));

                    // Numeric type variants (handle int stored as double, etc.)
                    if (isInt || isLong || isDouble)
                    {
                        double numericValue = isInt ? intValue : (isLong ? longValue : doubleValue);

                        // Try all numeric type combinations
                        fieldConditions.Add(Builders<BsonDocument>.Filter.Eq(field, (int)numericValue));
                        fieldConditions.Add(Builders<BsonDocument>.Filter.Eq(field, (long)numericValue));
                        fieldConditions.Add(Builders<BsonDocument>.Filter.Eq(field, numericValue));
                    }

                    // Date
                    if (isDate)
                    {
                        var startOfDay = dateValue.Date;
                        var endOfDay = startOfDay.AddDays(1);
                        fieldConditions.Add(Builders<BsonDocument>.Filter.And(
                            Builders<BsonDocument>.Filter.Gte(field, startOfDay),
                            Builders<BsonDocument>.Filter.Lt(field, endOfDay)
                        ));
                    }

                    elemMatchConditions.Add(Builders<BsonDocument>.Filter.Or(fieldConditions));
                }

                // RowNumber in array
                if (isInt || isLong)
                {
                    var rowNum = isInt ? (object)intValue : longValue;
                    elemMatchConditions.Add(Builders<BsonDocument>.Filter.Eq($"{METADATA}.RowNumber", rowNum));
                }

                if (elemMatchConditions.Any())
                {
                    conditions.Add(filterBuilder.ElemMatch<BsonDocument>(RECORDS,
                        Builders<BsonDocument>.Filter.Or(elemMatchConditions)));
                }
            }
        }

        // Search FieldScores (numeric only)
        if (sampleDoc.Contains(FIELD_SCORES) && (isInt || isDouble))
        {
            var fieldScoresDoc = sampleDoc[FIELD_SCORES].AsBsonDocument;
            foreach (var fieldName in fieldScoresDoc.Names)
            {
                if (!fieldScoresDoc[fieldName].IsBsonDocument) continue;

                var fieldDoc = fieldScoresDoc[fieldName].AsBsonDocument;
                foreach (var scoreName in fieldDoc.Names.Where(n =>
                    n != "Level" && n != "ContributionToTotal"))
                {
                    var scorePath = $"{FIELD_SCORES}.{fieldName}.{scoreName}";

                    if (isInt)
                        conditions.Add(filterBuilder.Eq(scorePath, intValue));
                    if (isDouble)
                        conditions.Add(filterBuilder.Eq(scorePath, doubleValue));
                }
            }
        }

        _logger.LogDebug("Built {Count} search conditions for '{Text}'", conditions.Count, searchText);
        return conditions;
    }

    /// <summary>
    /// Add field conditions for all data types
    /// </summary>
    private void AddFieldConditions(
        string fieldPath,
        string searchText,
        bool isInt, int intValue,
        bool isLong, long longValue,
        bool isDouble, double doubleValue,
        bool isDate, DateTime dateValue,
        FilterDefinitionBuilder<BsonDocument> filterBuilder,
        List<FilterDefinition<BsonDocument>> conditions)
    {
        // String search (case-insensitive)
        conditions.Add(filterBuilder.Regex(fieldPath,
            new BsonRegularExpression(searchText, "i")));

        // Numeric search - handle type variations (int vs long vs double)
        if (isInt || isLong || isDouble)
        {
            double numericValue = isInt ? intValue : (isLong ? longValue : doubleValue);

            // Try exact matches for all numeric types
            conditions.Add(filterBuilder.Eq(fieldPath, (int)numericValue));
            conditions.Add(filterBuilder.Eq(fieldPath, (long)numericValue));
            conditions.Add(filterBuilder.Eq(fieldPath, numericValue));

            // Range match for floating-point tolerance
            const double tolerance = 0.0001;
            conditions.Add(filterBuilder.And(
                filterBuilder.Gte(fieldPath, numericValue - tolerance),
                filterBuilder.Lte(fieldPath, numericValue + tolerance)
            ));
        }

        // Date search (same day range)
        if (isDate)
        {
            var startOfDay = dateValue.Date;
            var endOfDay = startOfDay.AddDays(1);
            conditions.Add(filterBuilder.And(
                filterBuilder.Gte(fieldPath, startOfDay),
                filterBuilder.Lt(fieldPath, endOfDay)
            ));
        }
    }

    #endregion    

    #region Index Creation

    /// <summary>
    /// Create required indexes for optimal performance
    /// CRITICAL: Call this during application startup or after matching completes
    /// </summary>
    public async Task CreateGroupFilterIndexesAsync(string collectionName)
    {
        try
        {
            var collection = GetCollection(collectionName);
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            _logger.LogInformation("Creating indexes for {Collection}...", collectionName);

            // Index 1: avg_match_score (CRITICAL for band filtering)
            await CreateIndexSafely(collection,
                indexBuilder.Descending("Metadata.avg_match_score"),
                "idx_avg_match_score");

            // Index 2: GroupId (for direct lookups)
            await CreateIndexSafely(collection,
                indexBuilder.Ascending("GroupId"),
                "idx_groupid");

            // Index 3: Group size
            await CreateIndexSafely(collection,
                indexBuilder.Ascending("Metadata.size"),
                "idx_group_size");

            // Index 4: Clique status
            await CreateIndexSafely(collection,
                indexBuilder.Ascending("Metadata.is_clique"),
                "idx_is_clique");

            // Index 5: Compound index for common query patterns
            await CreateIndexSafely(collection,
                indexBuilder
                    .Descending("Metadata.avg_match_score")
                    .Ascending("Metadata.size"),
                "idx_score_size");

            _logger.LogInformation("Successfully created all indexes for {Collection}", collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create indexes for {Collection}", collectionName);
            throw;
        }
    }

    /// <summary>
    /// Create index safely (ignores if already exists)
    /// </summary>
    private async Task CreateIndexSafely(
        IMongoCollection<BsonDocument> collection,
        IndexKeysDefinition<BsonDocument> keys,
        string indexName)
    {
        try
        {
            await collection.Indexes.CreateOneAsync(
                new CreateIndexModel<BsonDocument>(keys,
                    new CreateIndexOptions
                    {
                        Name = indexName,
                        Background = true
                    }));

            _logger.LogDebug("Created index: {IndexName}", indexName);
        }
        catch (MongoCommandException ex) when (ex.Code == 85 || ex.Code == 86)
        {
            // Index already exists or index options conflict - ignore
            _logger.LogDebug("Index {IndexName} already exists", indexName);
        }
    }

    #endregion


    private bool IsValidSortField(string field)
    {
        return !field.StartsWith("Record1.") &&
               !field.StartsWith("Record2.") &&
               field != ID_FIELD;
    }

    private string GetSortColumn(string field, string collectionName)
    {
        var splitArray = collectionName.Split("_");
        var collectionIdentified = splitArray.Length > 0 ? splitArray[0] : "";

        return collectionIdentified switch
        {
            "pairs" => GetPairsSortColumn(field),
            "groups" => GetGroupsSortColumn(field),
            _ => GetDefaultSortColumn(field)
        };
    }

    private string GetPairsSortColumn(string field)
    {
        if (field.StartsWith(FIELD_SCORES))
        {
            var arrField = field.Split('.');
            return $"{FIELD_SCORES}.{arrField[1]}.{arrField[2]}";
        }

        if (MetaFieldsName.Contains(field) || field.Contains("_"))
        {
            return field;
        }

        if (field == "Record")
        {
            return $"{RECORD1}.{METADATA}.RowNumber";
        }

        return $"{RECORD1}.{field}";
    }

    private string GetGroupsSortColumn(string field)
    {
        if (field == "GroupId")
        {
            return field;
        }

        if (field == "Record")
        {
            return $"{RECORDS}.0.{METADATA}.RowNumber";
        }

        return $"{RECORDS}.0.{field}";
    }

    private string GetDefaultSortColumn(string field)
    {
        if (field == "Record")
        {
            return $"{METADATA}.RowNumber";
        }

        return field;
    }

    private async Task<List<FilterDefinition<BsonDocument>>> BuildSearchConditionsAsync(
        IMongoCollection<BsonDocument> collection,
        string searchText)
    {
        var conditions = new List<FilterDefinition<BsonDocument>>();
        var filterBuilder = Builders<BsonDocument>.Filter;

        // Get a sample document to understand structure
        var sampleDoc = await collection.Find(FilterDefinition<BsonDocument>.Empty)
            .Limit(1)
            .FirstOrDefaultAsync();

        if (sampleDoc == null)
            return conditions;

        // Add top-level field conditions
        foreach (var field in sampleDoc.Names.Where(k =>
            k != ID_FIELD && k != GROUP_HASH && k != RECORD1 &&
            k != RECORD2 && k != RECORDS && k != FIELD_SCORES))
        {
            if (field == METADATA)
            {
                if (int.TryParse(searchText, out int value))
                {
                    conditions.Add(filterBuilder.Eq($"{METADATA}.RowNumber", value));
                }
            }
            else
            {
                conditions.Add(filterBuilder.Regex(field, new BsonRegularExpression(searchText, "i")));
            }
        }

        // Add Record1 field conditions
        if (sampleDoc.Contains(RECORD1))
        {
            var record1Doc = sampleDoc[RECORD1].AsBsonDocument;
            foreach (var field in record1Doc.Names)
            {
                if (field == METADATA)
                {
                    if (int.TryParse(searchText, out int value))
                    {
                        conditions.Add(filterBuilder.Eq($"{RECORD1}.{METADATA}.RowNumber", value));
                    }
                }
                else
                {
                    conditions.Add(filterBuilder.Regex($"{RECORD1}.{field}",
                        new BsonRegularExpression(searchText, "i")));
                }
            }
        }

        // Add Record2 field conditions
        if (sampleDoc.Contains(RECORD2))
        {
            var record2Doc = sampleDoc[RECORD2].AsBsonDocument;
            foreach (var field in record2Doc.Names)
            {
                if (field == METADATA)
                {
                    if (int.TryParse(searchText, out int value))
                    {
                        conditions.Add(filterBuilder.Eq($"{RECORD2}.{METADATA}.RowNumber", value));
                    }
                }
                else
                {
                    conditions.Add(filterBuilder.Regex($"{RECORD2}.{field}",
                        new BsonRegularExpression(searchText, "i")));
                }
            }
        }

        // Add Records array conditions (for groups)
        if (sampleDoc.Contains(RECORDS) && sampleDoc[RECORDS].IsBsonArray)
        {
            var recordsArray = sampleDoc[RECORDS].AsBsonArray;
            if (recordsArray.Count > 0)
            {
                var firstRecord = recordsArray[0].AsBsonDocument;
                foreach (var field in firstRecord.Names)
                {
                    if (field == METADATA)
                    {
                        if (int.TryParse(searchText, out int value))
                        {
                            conditions.Add(filterBuilder.ElemMatch<BsonDocument>(RECORDS,
                                Builders<BsonDocument>.Filter.Eq($"{METADATA}.RowNumber", value)));
                        }
                    }
                    else
                    {
                        conditions.Add(filterBuilder.ElemMatch<BsonDocument>(RECORDS,
                            Builders<BsonDocument>.Filter.Regex(field,
                                new BsonRegularExpression(searchText, "i"))));
                    }
                }
            }
        }

        // Add FieldScores conditions
        if (sampleDoc.Contains(FIELD_SCORES))
        {
            var fieldScoresDoc = sampleDoc[FIELD_SCORES].AsBsonDocument;
            foreach (var fieldName in fieldScoresDoc.Names)
            {
                var fieldDoc = fieldScoresDoc[fieldName].AsBsonDocument;
                foreach (var scoreName in fieldDoc.Names.Where(n => n != "Level" && n != "ContributionToTotal"))
                {
                    if (double.TryParse(searchText.Trim(), out double value))
                    {
                        conditions.Add(filterBuilder.Eq($"{FIELD_SCORES}.{fieldName}.{scoreName}", value));
                    }
                }
            }
        }

        return conditions;
    }

    private FilterDefinition<BsonDocument> BuildFilterQuery(string filters)
    {
        if (string.IsNullOrEmpty(filters))
            return FilterDefinition<BsonDocument>.Empty;

        var filterBuilder = Builders<BsonDocument>.Filter;
        var filterConditions = new List<FilterDefinition<BsonDocument>>();

        var filterPairs = filters.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var pair in filterPairs)
        {
            var keyValue = pair.Split('=');
            if (keyValue.Length != 2)
                continue;

            var fieldName = Uri.UnescapeDataString(keyValue[0]);
            var values = keyValue[1].Split(',', StringSplitOptions.RemoveEmptyEntries);

            if (values.Length == 0)
                continue;

            var fieldPath = $"{FIELD_SCORES}.{fieldName}.Level";
            var valueFilters = values.Select(v => filterBuilder.Eq(fieldPath, v)).ToList();

            filterConditions.Add(filterBuilder.Or(valueFilters));
        }

        if (!filterConditions.Any())
            return FilterDefinition<BsonDocument>.Empty;

        return filterBuilder.And(filterConditions);
    }

    #endregion

    #region Sampling Operations

    public async IAsyncEnumerable<IDictionary<string, object>> GetRandomSample(
        double maxPairs,
        string collectionName)
    {
        var collection = GetCollection(collectionName);

        var (sampleSize, probability) = await CalculateSampleSizeAsync(collection, maxPairs);

        // Use MongoDB's $sample aggregation for efficient random sampling
        var pipeline = new BsonDocument[]
        {
            new BsonDocument("$sample", new BsonDocument("size", sampleSize))
        };

        using var cursor = await collection.AggregateAsync<BsonDocument>(pipeline);

        while (await cursor.MoveNextAsync())
        {
            foreach (var document in cursor.Current)
            {
                yield return MongoDbBsonConverter.ConvertBsonDocumentToDictionary(document);
            }
        }
    }

    public async Task SampleAndStoreTempData(
        string sourceCollectionName,
        string tempCollectionName,
        double maxPairs)
    {
        try
        {
            var sourceCollection = GetCollection(sourceCollectionName);
            var tempCollection = GetCollection(tempCollectionName);

            var (sampleSize, _) = await CalculateSampleSizeAsync(sourceCollection, maxPairs);

            _logger.LogInformation(
                "Sampling {SampleSize} documents from {Source} to {Temp}",
                sampleSize, sourceCollectionName, tempCollectionName);

            // Use $sample aggregation and $out to write directly to temp collection
            // This is more efficient than reading and writing
            var pipeline = new BsonDocument[]
            {
                new BsonDocument("$sample", new BsonDocument("size", sampleSize)),
                new BsonDocument("$out", GetCollectionName(tempCollectionName))
            };

            await sourceCollection.AggregateAsync<BsonDocument>(pipeline);

            var resultCount = await tempCollection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);

            _logger.LogInformation(
                "Completed sampling. Stored {Count} documents in {Temp}",
                resultCount, tempCollectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error sampling data from {Source} to {Temp}",
                sourceCollectionName, tempCollectionName);
            throw;
        }
    }

    private async Task<(int sampleSize, double probability)> CalculateSampleSizeAsync(
        IMongoCollection<BsonDocument> collection,
        double maxPairs)
    {
        var totalRecords = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);

        // For n records, we get n(n-1)/2 pairs
        // Solving for n: n² - n - 2*maxPairs = 0
        // Using quadratic formula
        double n = (1 + Math.Sqrt(1 + 8 * maxPairs)) / 2;
        int sampleSize = (int)Math.Min(n, totalRecords);

        // Calculate sampling probability
        double probability = totalRecords > 0 ? (double)sampleSize / totalRecords : 0;

        return (sampleSize, probability);
    }

    #endregion
}