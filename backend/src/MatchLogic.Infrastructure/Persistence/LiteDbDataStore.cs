using Dasync.Collections;
using LiteDB;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence;

public class LiteDbDataStore : IDataStore
{
    private readonly string _dbPath;
    private LiteDatabase _db;
    private readonly ILogger _logger;
    private const int CommitBatchSize = 10000;
    private readonly HashSet<string> _commonSortFields = new() { "FinalScore", "WeightedScore" };
    private const string ID_FIELD = "_id";
    private const string GROUP_HASH = "GroupHash";
    private const string RECORD1 = "Record1";
    private const string RECORD2 = "Record2";
    private const string RECORDS = "Records";
    private const string FIELDSCORES = "FieldScores";
    private const string METADATA = "_metadata";
    private readonly List<string> metaFieldsName = new() { "GroupId", "PairId", "FinalScore", "WeightedScore" };

    public LiteDbDataStore(string dbPath, ILogger logger)
    {
        var mapper = BsonMapper.Global;
        BsonConverter.RegisterGuidDictionaryType<IList<CleaningRule>>(mapper);
        BsonConverter.RegisterGuidDictionaryType<IList<ExtendedCleaningRule>>(mapper);
        BsonConverter.RegisterGuidDictionaryType<IList<MappingRule>>(mapper);
        BsonConverter.RegisterGuidDictionaryType<List<Guid>>(mapper);
        // Register multidimensional array converters
        BsonConverter.RegisterMultidimensionalArrayTypes(mapper);
        BsonConverter.RegisterDictionaryTypes(mapper);

        BsonConverter.RegisterGuidDictionaryType<bool>(mapper);
        BsonConverter.RegisterGuidDictionaryType<long>(BsonMapper.Global);


        _dbPath = dbPath;
        _db = new LiteDatabase(_dbPath);
        _logger = logger;
    }


    public Task<Guid> InitializeJobAsync(string collectionName = "")
    {
        try
        {
            var jobId = Guid.NewGuid();
            _logger.LogInformation("Initializing new import job: {JobId}", jobId);
            var collection = string.IsNullOrEmpty(collectionName) ? _db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId)) :
                _db.GetCollection<BsonDocument>(collectionName);
            collection.EnsureIndex("_id");
            return Task.FromResult(jobId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize new import job");
            throw;
        }
    }

    public Task<bool> DeleteCollection(string collectionName)
    {
        bool isSuccessful = false;
        try
        {
            isSuccessful = _db.DropCollection(collectionName);
            _logger.LogInformation("Deleted collection {CollectionName}", collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting collection {CollectionName}", collectionName);
            throw;
        }
        return Task.FromResult(isSuccessful);
    }

    public async Task InsertBatchAsync(Guid jobId, IEnumerable<IDictionary<string, object>> batch, string collectionName = "")
    {
        try
        {
            var collection = string.IsNullOrEmpty(collectionName) ? _db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId)) :
                _db.GetCollection<BsonDocument>(collectionName);

            var bsonDocs = batch.Select(item => new BsonDocument(item.ToDictionary(kvp => kvp.Key, kvp => BsonConverter.ConvertToBsonValue(kvp.Value)))).ToList();

            await Task.Run(() =>
            {
                bool trans = _db.BeginTrans();
                for (int i = 0; i < bsonDocs.Count; i += CommitBatchSize)
                {
                    var subBatch = bsonDocs.Skip(i).Take(CommitBatchSize);
                    collection.InsertBulk(subBatch);
                    /*
                     i + CommitBatchSize >= bsonDocs.Count: This checks if we're at or beyond the end of the batch. It ensures we commit the final set of documents.
                    (i + CommitBatchSize) % (CommitBatchSize * 5) == 0: This checks if we've processed a multiple of 5 times the CommitBatchSize. For example, if CommitBatchSize is 10,000, this will be true every 50,000 documents.

                    For example, with a CommitBatchSize of 10,000:

                    A batch of 40,000 documents would be handled in a single transaction.
                    A batch of 60,000 documents would be handled in two transactions: one for the first 50,000 and one for the remaining 10,000.
                    A batch of 120,000 documents would be handled in three transactions: two of 50,000 and one of 20,000.
                     */
                    if (i + CommitBatchSize >= bsonDocs.Count || (i + CommitBatchSize) % (CommitBatchSize * 5) == 0)
                    {
                        _db.Commit();
                        if (i + CommitBatchSize < bsonDocs.Count)
                        {
                            trans = _db.BeginTrans();
                        }
                    }
                }
            });

            _logger.LogInformation("Inserted batch of {Count} records into job {JobId}", bsonDocs.Count, jobId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting batch into job {JobId}", jobId);
            throw;
        }
    }

    public async Task InsertBatchAsync(string collectionName, IEnumerable<IDictionary<string, object>> batch)
    {
        try
        {
            var collection = _db.GetCollection<BsonDocument>(collectionName);
            var bsonDocs = batch.Select(BsonConverter.ConvertToBsonDocument).ToList();

            await Task.Run(() =>
            {
                bool trans = _db.BeginTrans();
                for (int i = 0; i < bsonDocs.Count; i += CommitBatchSize)
                {
                    var subBatch = bsonDocs.Skip(i).Take(CommitBatchSize);
                    collection.InsertBulk(subBatch);
                    /*
                     i + CommitBatchSize >= bsonDocs.Count: This checks if we're at or beyond the end of the batch. It ensures we commit the final set of documents.
                    (i + CommitBatchSize) % (CommitBatchSize * 5) == 0: This checks if we've processed a multiple of 5 times the CommitBatchSize. For example, if CommitBatchSize is 10,000, this will be true every 50,000 documents.

                    For example, with a CommitBatchSize of 10,000:

                    A batch of 40,000 documents would be handled in a single transaction.
                    A batch of 60,000 documents would be handled in two transactions: one for the first 50,000 and one for the remaining 10,000.
                    A batch of 120,000 documents would be handled in three transactions: two of 50,000 and one of 20,000.
                     */
                    if (i + CommitBatchSize >= bsonDocs.Count || (i + CommitBatchSize) % (CommitBatchSize * 5) == 0)
                    {
                        _db.Commit();
                        if (i + CommitBatchSize < bsonDocs.Count)
                        {
                            trans = _db.BeginTrans();
                        }
                    }
                }
            });

            _logger.LogInformation("Inserted batch of {Count} records into {collectionName}", bsonDocs.Count, collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting batch into {collectionName}", collectionName);
            throw;
        }
    }

    public async Task InsertProbabilisticBatchAsync(string collectionName, IEnumerable<MatchResult> batch)
    {
        try
        {
            var collection = _db.GetCollection<BsonDocument>(collectionName);
            var bsonDocs = batch.Select(BsonConverter.SerializeToDocument).ToList();

            await Task.Run(() =>
            {
                bool trans = _db.BeginTrans();
                for (int i = 0; i < bsonDocs.Count(); i += CommitBatchSize)
                {
                    var subBatch = bsonDocs.Skip(i).Take(CommitBatchSize);
                    collection.InsertBulk(subBatch);
                    /*
                     i + CommitBatchSize >= bsonDocs.Count: This checks if we're at or beyond the end of the batch. It ensures we commit the final set of documents.
                    (i + CommitBatchSize) % (CommitBatchSize * 5) == 0: This checks if we've processed a multiple of 5 times the CommitBatchSize. For example, if CommitBatchSize is 10,000, this will be true every 50,000 documents.

                    For example, with a CommitBatchSize of 10,000:

                    A batch of 40,000 documents would be handled in a single transaction.
                    A batch of 60,000 documents would be handled in two transactions: one for the first 50,000 and one for the remaining 10,000.
                    A batch of 120,000 documents would be handled in three transactions: two of 50,000 and one of 20,000.
                     */
                    if (i + CommitBatchSize >= bsonDocs.Count() || (i + CommitBatchSize) % (CommitBatchSize * 5) == 0)
                    {
                        _db.Commit();
                        if (i + CommitBatchSize < bsonDocs.Count())
                        {
                            trans = _db.BeginTrans();
                        }
                    }
                }
            });

            _logger.LogInformation("Inserted batch of {Count} records into {collectionName}", bsonDocs.Count, collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting batch into {collectionName}", collectionName);
            throw;
        }
    }
    public async Task<IEnumerable<IDictionary<string, object>>> GetJobDataAsync(Guid jobId)
    {
        try
        {
            var collection = _db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId));
            var data = await Task.Run(() => collection.FindAll().Select(ConvertFromBsonDocument).ToList());
            _logger.LogInformation("Retrieved {Count} records from job {JobId}", data.Count, jobId);
            return data;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving data for job {JobId}", jobId);
            throw;
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobDataAsync(Guid jobId, int pageNumber, int pageSize)
    {
        try
        {
            var collection = _db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId));
            var totalCount = await Task.Run(() => collection.Count());
            var data = await Task.Run(() => collection.Find(LiteDB.Query.All(), skip: (pageNumber - 1) * pageSize, limit: pageSize)
                                                       .Select(ConvertFromBsonDocument)
                                                       .ToList());

            _logger.LogInformation("Fetched page {PageNumber} with {Count} records from job {JobId}", pageNumber, data.Count, jobId);
            return (data, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching paged data for job {JobId}", jobId);
            throw;
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedDataAsync(string collectionName, int pageNumber, int pageSize)
    {
        try
        {
            var collection = _db.GetCollection<BsonDocument>(collectionName);
            var totalCount = await Task.Run(() => collection.Count());
            var data = await Task.Run(() => collection.Find(LiteDB.Query.All(), skip: (pageNumber - 1) * pageSize, limit: pageSize)
                                                       .Select(ConvertFromBsonDocument)
                                                       .ToList());

            _logger.LogInformation("Fetched page {PageNumber} with {Count} records from {collectionName}", pageNumber, data.Count, collectionName);
            return (data, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching paged data from {collectionName}", collectionName);
            throw;
        }
    }

    public IAsyncEnumerable<IDictionary<string, object>> StreamJobDataAsync(Guid jobId, IStepProgressTracker progressTracker, string collectionName = "", CancellationToken cancellationToken = default)
    {

        return new AsyncEnumerable<IDictionary<string, object>>(async yield =>
        {
            try
            {
                var collection = string.IsNullOrEmpty(collectionName) ? _db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId)) :
                _db.GetCollection<BsonDocument>(collectionName);

                var collect = collection.FindAll();
                var count = collect.Count();
                await progressTracker.StartStepAsync(count, cancellationToken);
                using (var enumerator = collect.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        await yield.ReturnAsync(ConvertFromBsonDocument(enumerator.Current));

                    }
                }
                await progressTracker.CompleteStepAsync(null);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error streaming data for job {JobId}", jobId);
                throw;
            }
        });
    }

    public IAsyncEnumerable<IDictionary<string, object>> StreamDataAsync(string collectionName, CancellationToken cancellationToken = default)
    {

        return new AsyncEnumerable<IDictionary<string, object>>(async yield =>
        {
            try
            {
                var collection = _db.GetCollection<BsonDocument>(collectionName);
                var collect = collection.FindAll();
                var count = collect.Count();
                using (var enumerator = collect.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        await yield.ReturnAsync(ConvertFromBsonDocument(enumerator.Current));

                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error streaming data for {collection}", collectionName);
                throw;
            }
        });
    }

    private IDictionary<string, object> ConvertFromBsonDocument(BsonDocument doc)
    {
        return doc.ToDictionary(kvp => kvp.Key, kvp => ConvertBsonValueToObject(kvp.Value));
    }

    private object ConvertBsonValueToObject(BsonValue bsonValue)
    {
        // Check the type of BsonValue and convert it to the appropriate .NET type
        if (bsonValue.IsString) return bsonValue.AsString;
        if (bsonValue.IsInt32) return bsonValue.AsInt32;
        if (bsonValue.IsInt64) return bsonValue.AsInt64;
        if (bsonValue.IsDouble) return bsonValue.AsDouble;
        if (bsonValue.IsBoolean) return bsonValue.AsBoolean;
        // Handle arrays by converting each element
        if (bsonValue.IsArray)
        {
            var array = bsonValue.AsArray;
            return array.Select(item => ConvertBsonValueToObject(item)).ToList();
        }
        if (bsonValue.IsDocument) return ConvertFromBsonDocument(bsonValue.AsDocument); // Recursively convert nested BsonDocument

        // Handle other types as needed (DateTime, Guid, etc.)
        return bsonValue.RawValue; // The raw value if you don't want to check every type
    }

    public void Dispose()
    {
        _db?.Dispose();
        _logger.LogInformation("Disposed LiteDB connection");
    }

    public async Task<List<T>> QueryAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        var collection = _db.GetCollection<T>(collectionName);
        return await Task.FromResult(collection.Find(predicate).ToList());
    }
    public async Task<T> GetByIdAsync<T, Tkey>(Tkey id, string collectionName)
    {
        var collection = _db.GetCollection<T>(collectionName);
        return await Task.FromResult(collection.FindById(new BsonValue(id)));
    }

    public async Task<List<T>> GetAllAsync<T>(string collectionName)
    {
        var collection = _db.GetCollection<T>(collectionName);
        return await Task.FromResult(collection.FindAll().ToList());
    }

    public async Task<int> DeleteAllAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        var collection = _db.GetCollection<T>(collectionName);
        return await Task.FromResult(collection.DeleteMany(predicate));
    }
    public async Task InsertAsync<T>(T entity, string collectionName)
    {
        var collection = _db.GetCollection<T>(collectionName);
        await Task.FromResult(collection.Insert(entity));
    }
    public async Task BulkInsertAsync<T>(IEnumerable<T> entity, string collectionName)
    {
        var collection = _db.GetCollection<T>(collectionName);
        await Task.FromResult(collection.InsertBulk(entity));
    }

    public async Task UpdateAsync<T>(T entity, string collectionName)
    {
        var collection = _db.GetCollection<T>(collectionName);
        await Task.FromResult(collection.Update(entity));
    }


    public async Task UpdateAsync(IDictionary<string, object> entity, string collectionName)
    {
        var collection = _db.GetCollection<BsonDocument>(collectionName);

        //// Extract GroupId from the entity dictionary
        var bsonDoc = BsonConverter.ConvertToBsonDocument(entity);
        // Check if _id is a complex object resembling ObjectId components
        if (entity.TryGetValue("_id", out var idValue) && idValue is IDictionary<string, object> idDict)
        {
            // Check if it has the expected ObjectId structure
            if (idDict.TryGetValue("timestamp", out var timestampObj) &&
                idDict.TryGetValue("machine", out var machineObj) &&
                idDict.TryGetValue("pid", out var pidObj) &&
                idDict.TryGetValue("increment", out var incrementObj))
            {
                int timestamp = Convert.ToInt32(timestampObj);
                int machine = Convert.ToInt32(machineObj);
                int pid = Convert.ToInt32(pidObj);
                int increment = Convert.ToInt32(incrementObj);

                // Create ObjectId from components
                var objectId = new ObjectId(timestamp, machine, (short)pid, increment);
                bsonDoc["_id"] = new BsonValue(objectId);
            }
            else if (idValue is string guidStr && Guid.TryParse(guidStr, out var guid))
            {
                // Handle plain string GUID
                bsonDoc["_id"] = new BsonValue(guid);
            }
            else
            {
                // If not the expected structure, convert using BsonConverter
                bsonDoc["_id"] = BsonConverter.ConvertToBsonValue(entity["_id"]);
            }
        }
        else
        {
            // Ensure _id is included using the converter
            bsonDoc["_id"] = BsonConverter.ConvertToBsonValue(entity["_id"]);
        }
        // Update the document in the collection
        var result = await Task.FromResult(collection.Update(bsonDoc));
        if (!result)
            throw new InvalidOperationException("Update failed in LiteDB.");
    }
    public async Task BulkUpdateAsync(IEnumerable<IDictionary<string, object>> entities, string collectionName)
    {
        try
        {
            var collection = _db.GetCollection<BsonDocument>(collectionName);
            var bsonDocs = entities.Select(entity =>
            {
                var bsonDoc = BsonConverter.ConvertToBsonDocument(entity);
                // Handle _id conversion as in single update
                if (entity.TryGetValue("_id", out var idValue) && idValue is IDictionary<string, object> idDict)
                {
                    if (idDict.TryGetValue("timestamp", out var timestampObj) &&
                        idDict.TryGetValue("machine", out var machineObj) &&
                        idDict.TryGetValue("pid", out var pidObj) &&
                        idDict.TryGetValue("increment", out var incrementObj))
                    {
                        int timestamp = Convert.ToInt32(timestampObj);
                        int machine = Convert.ToInt32(machineObj);
                        int pid = Convert.ToInt32(pidObj);
                        int increment = Convert.ToInt32(incrementObj);
                        var objectId = new ObjectId(timestamp, machine, (short)pid, increment);
                        bsonDoc["_id"] = new BsonValue(objectId);
                    }
                    else if (idValue is string guidStr && Guid.TryParse(guidStr, out var guid))
                    {
                        // Handle plain string GUID
                        bsonDoc["_id"] = new BsonValue(guid);
                    }
                    else
                    {
                        bsonDoc["_id"] = BsonConverter.ConvertToBsonValue(entity["_id"]);
                    }
                }
                else
                {
                    bsonDoc["_id"] = BsonConverter.ConvertToBsonValue(entity["_id"]);
                }
                return bsonDoc;
            }).ToList();

            await Task.Run(() =>
            {
                bool trans = _db.BeginTrans();
                for (int i = 0; i < bsonDocs.Count; i += CommitBatchSize)
                {
                    var subBatch = bsonDocs.Skip(i).Take(CommitBatchSize);
                    foreach (var doc in subBatch)
                    {
                        collection.Update(doc);
                    }
                    if (i + CommitBatchSize >= bsonDocs.Count || (i + CommitBatchSize) % (CommitBatchSize * 5) == 0)
                    {
                        _db.Commit();
                        if (i + CommitBatchSize < bsonDocs.Count)
                        {
                            trans = _db.BeginTrans();
                        }
                    }
                }
            });

            _logger.LogInformation("Bulk updated {Count} records in {collectionName}", bsonDocs.Count, collectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error bulk updating in {collectionName}", collectionName);
            throw;
        }
    }
    public async Task DeleteAsync<Tkey>(Tkey id, string collectionName)
    {
        var collection = _db.GetCollection(collectionName);
        await Task.FromResult(collection.Delete(new BsonValue(id)));
    }
    public async Task<bool> UpdateByFieldAsync<TField>(
    IDictionary<string, object> data,
    string collectionName,
    string fieldName,
    TField fieldValue)
    {
        var collection = _db.GetCollection<BsonDocument>(collectionName);

        var existingDoc = collection.FindOne(Query.EQ(fieldName, new BsonValue(fieldValue)));
        if (existingDoc == null) return false;

        var updatedDoc = BsonConverter.ConvertToBsonDocument(data);
        updatedDoc["_id"] = existingDoc["_id"];

        return await Task.FromResult(collection.Update(updatedDoc));
    }
    #region Sorting and Filtering

    private void EnsureIndexes(ILiteCollection<BsonDocument> collection)
    {
        foreach (var field in _commonSortFields)
        {
            collection.EnsureIndex(field);
        }
    }

    private bool IsValidSortField(string field)
    {
        return !field.StartsWith("Record1.") &&
               !field.StartsWith("Record2.") &&
               field != ID_FIELD;
    }

    private string FormatFieldName(string fieldName)
    {
        // This method handles field names that may start with spaces, numbers, or special characters. 
        // For example:
        // - A field name like "  Name" (with leading spaces) will be formatted correctly.
        // - A field name like "2Name" (starting with a number) will be handled properly.
        // - A field name like "_Name" (starting with an underscore) will also be processed correctly.
        // The goal is to ensure compatibility with LiteDB's query syntax by wrapping such field names in proper formatting.
        return $"$.['{fieldName}']";
    }

    private string GetSortColumn(string field, string collectionName)
    {
        var splitArray = collectionName.Split("_");
        var collectionIdentified = splitArray[splitArray.Length - 1];
        string columnName;
        switch (collectionIdentified)
        {
            case "pairs":
                {
                    if (field.StartsWith(FIELDSCORES))
                    {
                        var arrField = field.Split('.');
                        columnName = $"$.['{arrField[0]}'].['{arrField[1]}'].['{arrField[2]}']";
                    }
                    else if (metaFieldsName.Contains(field) || field.Contains("_"))
                    {
                        columnName = FormatFieldName(field);
                    }
                    else
                    {
                        if (field == "Record")
                        {
                            columnName = $"$.['{RECORD1}'].['{METADATA}'].['RowNumber']";
                        }
                        else
                        {
                            columnName = FormatNestedFieldName(RECORD1, field);
                        }

                    }
                }
                break;
            case "groups":
                if ("GroupId" == field)
                {
                    columnName = FormatFieldName(field);
                }
                else
                {
                    if (field == "Record")
                    {
                        columnName = $"{RECORDS}[0].['{METADATA}'].['RowNumber']";
                    }
                    else
                    {
                        columnName = $"{RECORDS}[0].['{field}']";
                    }
                }
                break;
            default:
                {
                    if (field == "Record")
                    {
                        columnName = $"$.['{METADATA}'].['RowNumber']";
                    }
                    else
                    {
                        columnName = FormatFieldName(field);
                    }
                }

                break;
        }
        return columnName;
    }
    private string FormatNestedFieldName(string parent, string field)
    {
        return $"$.['{parent}'].['{field}']";
    }

    private List<BsonExpression> BuildSearchConditions(ILiteCollection<BsonDocument> collection, string searchText)
    {
        var conditions = new List<BsonExpression>();
        var sampleDoc = collection.FindOne(_ => true);

        if (sampleDoc == null) return conditions;

        // Add top-level field conditions
        foreach (var field in sampleDoc.Keys.Where(k => k != ID_FIELD && k != GROUP_HASH && k != RECORD1 && k != RECORD2 && k != RECORDS && k != FIELDSCORES))
        {
            string formattedField;
            if (field == METADATA)
            {
                formattedField = $"$.['{METADATA}'].['RowNumber']";
                bool isNumber = int.TryParse(searchText, out int value);
                if (isNumber)
                {
                    conditions.Add(Query.EQ(formattedField, value));
                }
            }
            else
            {
                formattedField = FormatFieldName(field);
                conditions.Add(Query.Contains(formattedField, searchText));
            }
        }

        // Add Record1 field conditions
        if (sampleDoc.ContainsKey(RECORD1))
        {
            foreach (var field in sampleDoc[RECORD1].AsDocument.Keys)
            {
                string formattedField;
                if (field == METADATA)
                {
                    formattedField = $"$.['{RECORD1}'].['{METADATA}'].['RowNumber']";
                    bool isNumber = int.TryParse(searchText, out int value);
                    if (isNumber)
                    {
                        conditions.Add(Query.EQ(formattedField, value));
                    }
                }
                else
                {
                    formattedField = FormatNestedFieldName(RECORD1, field);
                    conditions.Add(Query.Contains(formattedField, searchText));
                }


            }
        }

        // Add Record2 field conditions
        if (sampleDoc.ContainsKey(RECORD2))
        {
            foreach (var field in sampleDoc[RECORD2].AsDocument.Keys)
            {
                string formattedField;
                if (field == METADATA)
                {
                    formattedField = $"$.['{RECORD2}'].['{METADATA}'].['RowNumber']";
                    bool isNumber = int.TryParse(searchText, out int value);
                    if (isNumber)
                    {
                        conditions.Add(Query.EQ(formattedField, value));
                    }
                }
                else
                {
                    formattedField = FormatNestedFieldName(RECORD2, field);
                    conditions.Add(Query.Contains(formattedField, searchText));
                }

            }
        }

        if (sampleDoc.ContainsKey(RECORDS))
        {
            var nestedArray = sampleDoc[RECORDS].AsArray.First();

            foreach (var field in nestedArray.AsDocument.Keys)
            {
                string formattedField;
                if (field == METADATA)
                {

                    bool isNumber = int.TryParse(searchText, out int value);
                    if (isNumber)
                    {
                        formattedField = $"MAP({RECORDS}[*]=>@.['{METADATA}'].['RowNumber']) ANY = {value}";
                        conditions.Add(formattedField);
                    }

                }
                else
                {
                    formattedField = $"MAP($.{RECORDS}[*]=>@.['{field}']) ANY LIKE '%{searchText}%'";
                    conditions.Add(formattedField);
                }

            }
        }
        if (sampleDoc.ContainsKey(FIELDSCORES))
        {
            foreach (var field in sampleDoc[FIELDSCORES].AsDocument.Keys)
            {
                foreach (var fieldDoc in sampleDoc[FIELDSCORES].AsDocument[field].AsDocument.Keys)
                {
                    if (fieldDoc == "Level" || fieldDoc == "ContributionToTotal")
                        continue;

                    string formattedField = $"$.['{FIELDSCORES}'].['{field}'].['{fieldDoc}']";

                    // Handle both negative and positive numbers
                    string searchTextTrimmed = searchText.Trim();
                    bool isNumber = false;
                    double value;

                    if (double.TryParse(searchTextTrimmed,
                         System.Globalization.NumberStyles.Float | System.Globalization.NumberStyles.AllowLeadingSign,
                         System.Globalization.CultureInfo.InvariantCulture,
                         out value))
                    {
                        // Store exact string representation
                        var exactValue = $"{searchTextTrimmed}";

                        // Add the field name for the query
                        conditions.Add($"{formattedField} = DOUBLE('{exactValue}')");
                    }
                }
            }
        }

        return conditions;
    }

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobWithSortingAndFilteringDataAsync(
       string collectionName,
       int pageNumber,
       int pageSize,
       string filterText = null,
       string sortColumn = null,
       bool ascending = true,
        string filters = "",
            GroupQueryFilter groupFilter = null)
    {
        try
        {
            var collection = _db.GetCollection<BsonDocument>(collectionName);

            // Ensure indexes exist for performance
            EnsureIndexes(collection);

            var query = collection.Query();

            if (!string.IsNullOrWhiteSpace(filterText))
            {
                var conditions = BuildSearchConditions(collection, filterText);
                if (conditions.Any())
                {
                    query = query.Where(Query.Or(conditions.ToArray()));
                }
            }
            if (!string.IsNullOrWhiteSpace(filters))
            {

                // Build path map from one sample document
                var levelFilter = BuildFilterQuery(filters, BuildJsonPaths(collection.FindOne(Query.All())));
                if (levelFilter != null)
                {
                    query = query.Where(levelFilter);
                }
            }

            if (!string.IsNullOrWhiteSpace(sortColumn) && IsValidSortField(sortColumn))
            {
                query = ascending
                    ? query.OrderBy(GetSortColumn(sortColumn, collectionName))
                    : query.OrderByDescending(GetSortColumn(sortColumn, collectionName));
            }

            var totalCount = query.Count();
            var data = await Task.Run(() =>
                query.Skip((pageNumber - 1) * pageSize)
                     .Limit(pageSize)
                     .ToList()
                     .Select(ConvertFromBsonDocument)
                     .ToList());

            return (data, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching paged data for collection {collectionName}", collectionName);
            throw;
        }
    }
    private BsonExpression BuildFilterQuery(string filters, Dictionary<string, List<string>> pathMap)
    {
        if (string.IsNullOrWhiteSpace(filters))
            return null;

        var andConditions = new List<string>();
        var filterPairs = filters.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var pair in filterPairs)
        {
            string key = string.Empty;
            string op = "=";
            string value = string.Empty;

            if (pair.Contains(">=")) { var p = pair.Split(">="); key = p[0]; op = ">="; value = p[1]; }
            else if (pair.Contains("<=")) { var p = pair.Split("<="); key = p[0]; op = "<="; value = p[1]; }
            else if (pair.Contains(">")) { var p = pair.Split(">"); key = p[0]; op = ">"; value = p[1]; }
            else if (pair.Contains("<")) { var p = pair.Split("<"); key = p[0]; op = "<"; value = p[1]; }
            else if (pair.Contains("~")) { var p = pair.Split("~"); key = p[0]; op = "contains"; value = p[1]; }
            else if (pair.Contains("=")) { var p = pair.Split("="); key = p[0]; op = "="; value = p[1]; }

            key = key.Trim();
            value = value.Trim();

            var values = value.Split(',').Select(v => v.Trim()).ToList();

            List<string> jsonPaths;
            if (key.StartsWith("$."))
            {
                jsonPaths = new List<string>() { key };
            }
            else if (key.Contains("["))
            {
                jsonPaths = new List<string>() { "$." + key };
            }
            else if (key.Contains("."))
            {
                jsonPaths = new List<string>() { "$." + key };
            }
            else
            {
                if (!pathMap.ContainsKey(key))
                    continue;
                jsonPaths = pathMap[key];
            }

            foreach (var jsonPath in jsonPaths)
            {
                var orList = new List<string>();
                bool isArrayPath = jsonPath.Contains("[*]");

                foreach (var v in values)
                {
                    string condition;
                    if (isArrayPath)
                    {
                        // Extract the array and field for MAP
                        var match = System.Text.RegularExpressions.Regex.Match(jsonPath, @"\$\.(\w+)\[\*\]\.(.+)");
                        if (match.Success)
                        {
                            var arrayName = match.Groups[1].Value;
                            var fieldName = match.Groups[2].Value;
                            string mapExpr = $"MAP($.{arrayName}[*]=>@.{fieldName})";
                            string opExpr = op switch
                            {
                                //"=" => $"ANY({mapExpr} = '{v}')",
                                "=" => $"'{v}' = ANY({mapExpr})",
                                ">" => $"ANY({mapExpr} > {v})",
                                "<" => $"ANY({mapExpr} < {v})",
                                ">=" => $"ANY({mapExpr} >= {v})",
                                "<=" => $"ANY({mapExpr} <= {v})",
                                //"contains" => $"ANY({mapExpr} LIKE '%{v}%')",
                                //_ => $"ANY({mapExpr} = '{v}')"
                                _ => $"'{v}' = ANY({mapExpr})"
                            };
                            condition = opExpr;
                        }
                        else
                        {
                            // fallback, just wrap with ANY
                            string baseCondition = op switch
                            {
                                //"=" => $"'{v}' = {jsonPath}",
                                "=" => $"'{v}' = ANY({jsonPath})",
                                ">" => $"{jsonPath} > {v}",
                                "<" => $"{jsonPath} < {v}",
                                ">=" => $"{jsonPath} >= {v}",
                                "<=" => $"{jsonPath} <= {v}",
                                //"contains" => $"{jsonPath} LIKE '%{v}%'",
                                "contains" => $"ANY({jsonPath} LIKE '%{v}%')",
                                _ => $"'{v}' = {jsonPath}"
                            };
                            condition = $"ANY({baseCondition})";
                        }
                    }
                    else
                    {
                        condition = op switch
                        {
                            "=" => $"{jsonPath} = '{v}'",
                            ">" => $"{jsonPath} > {v}",
                            "<" => $"{jsonPath} < {v}",
                            ">=" => $"{jsonPath} >= {v}",
                            "<=" => $"{jsonPath} <= {v}",
                            "contains" => $"{jsonPath} LIKE '%{v}%'",
                            _ => $"{jsonPath} = '{v}'"
                        };
                    }
                    orList.Add(condition);
                }
                andConditions.Add("(" + string.Join(" OR ", orList) + ")");
            }
        }

        if (!andConditions.Any())
            return null;

        return BsonExpression.Create(string.Join(" AND ", andConditions));
    }

    private BsonExpression BuildFilterQuery(string filters)
    {
        if (string.IsNullOrEmpty(filters))
            return null;

        var filterConditions = new List<string>();
        var filterPairs = filters.Split(';');

        foreach (var pair in filterPairs)
        {
            var keyValue = pair.Split('=');
            if (keyValue.Length == 2)
            {
                var fieldName = Uri.UnescapeDataString(keyValue[0]);
                var values = keyValue[1].Split(',');

                if (values.Length > 0)
                {
                    // Format the field path properly for LiteDB
                    var formattedFieldName = fieldName.Contains(" ")
                        ? $"$.[\"FieldScores\"].[\"" + fieldName + "\"].[\"Level\"]"
                        : $"$.FieldScores.{fieldName}.Level";

                    // Create a series of OR conditions for each value
                    var valueChecks = values.Select(v => $"{formattedFieldName} = '{v}'");
                    filterConditions.Add($"({string.Join(" OR ", valueChecks)})");
                }
            }
        }

        if (!filterConditions.Any())
            return null;

        // Combine all conditions with AND
        var finalExpression = string.Join(" AND ", filterConditions);
        _logger.LogInformation("Generated filter expression: {Expression}", finalExpression);
        return finalExpression;
    }
    public async IAsyncEnumerable<IDictionary<string, object>> GetRandomSample(double maxPairs, string collectionName)
    {
        ILiteCollection<BsonDocument> collection;
        int sampleSize;
        double probability;
        CalculateSampleSize(maxPairs, collectionName, out collection, out sampleSize, out probability);

        var _random = new Random();

        // Process in batches
        int batchSize = 1000;
        int processed = 0;
        int selected = 0;

        while (processed < sampleSize && selected < sampleSize)
        {
            // Get batch
            var batch = collection.Find(Query.All(), skip: processed, limit: batchSize);

            foreach (var doc in batch)
            {
                // Select with probability
                if (_random.NextDouble() < probability && selected < sampleSize)
                {
                    selected++;
                    yield return ConvertFromBsonDocument(doc);
                }
                processed++;
            }
        }
    }

    public async Task SampleAndStoreTempData(string sourceCollectionName, string tempCollectionName, double maxPairs)
    {
        try
        {
            var sourceCollection = _db.GetCollection<BsonDocument>(sourceCollectionName);
            var tempCollection = _db.GetCollection<BsonDocument>(tempCollectionName);

            int sampleSize;
            double probability;
            CalculateSampleSize(maxPairs, sourceCollectionName, out _, out sampleSize, out probability);

            var random = new Random();
            int batchSize = 1000;
            int processed = 0;
            int selected = 0;
            var sampledBatch = new List<BsonDocument>();

            while (processed < sampleSize && selected < sampleSize)
            {
                var batch = sourceCollection.Find(Query.All(), skip: processed, limit: batchSize);

                foreach (var doc in batch)
                {
                    if (random.NextDouble() < probability && selected < sampleSize)
                    {
                        sampledBatch.Add(doc); // Direct BsonDocument use, no conversion needed
                        selected++;

                        if (sampledBatch.Count >= CommitBatchSize)
                        {
                            await Task.Run(() =>
                            {
                                bool trans = _db.BeginTrans();
                                tempCollection.InsertBulk(sampledBatch);
                                _db.Commit();
                            });
                            sampledBatch.Clear();
                        }
                    }
                    processed++;
                }
            }

            // Insert any remaining records
            if (sampledBatch.Count > 0)
            {
                await Task.Run(() =>
                {
                    _db.BeginTrans();
                    tempCollection.InsertBulk(sampledBatch);
                    _db.Commit();
                });
            }

            _logger.LogInformation("Completed sampling and storing {Selected} records from {Processed} total processed", selected, processed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error sampling and storing data from {Source} to {Temp}", sourceCollectionName, tempCollectionName);
            throw;
        }
    }


    public IAsyncEnumerable<IDictionary<string, object>> GetStreamFromTempCollection(string _tempCollectionName, CancellationToken cancellationToken)
    {
        return new AsyncEnumerable<IDictionary<string, object>>(async yield =>
        {
            try
            {
                var collection = _db.GetCollection<BsonDocument>(_tempCollectionName);
                var collect = collection.FindAll();
                var count = collect.Count();
                using (var enumerator = collect.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        await yield.ReturnAsync(ConvertFromBsonDocument(enumerator.Current));

                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error streaming sample data for {JobId}", _tempCollectionName);
                throw;
            }
        });
    }
    private void CalculateSampleSize(double maxPairs, string collectionName, out ILiteCollection<BsonDocument> collection, out int sampleSize, out double probability)
    {
        collection = _db.GetCollection<BsonDocument>(collectionName);
        var totalRecords = collection.Count();
        // For n records, we get n(n-1)/2 pairs
        // Solving for n: n² - n - 2*maxPairs = 0
        // Using quadratic formula
        double n = (1 + Math.Sqrt(1 + 8 * maxPairs)) / 2;
        sampleSize = (int)Math.Min(n, totalRecords);



        // Calculate sampling probability        
        probability = (double)sampleSize / totalRecords;
    }

    #endregion

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedWithSmartFilteringAndProjectionAsync(
        string collectionName,
        int pageNumber,
        int pageSize,
        string filterText = null,
        string sortColumn = null,
        bool ascending = true,
        string filters = "")
    {
        var collection = _db.GetCollection<BsonDocument>(collectionName);

        EnsureIndexes(collection);

        var query = collection.Query();

        BsonExpression? projectionExpr = null;

        // Text search (original logic)
        if (!string.IsNullOrWhiteSpace(filterText))
        {
            var conditions = BuildSearchConditions(collection, filterText);
            if (conditions.Any())
                query = query.Where(Query.Or(conditions.ToArray()));
        }
        if (!string.IsNullOrWhiteSpace(filters))
        {
            // Build path map from a sample document for auto-resolution
            var (filterExpr, _projectionExpr) = BuildSmartFilterAndProjection(filters, BuildJsonPaths(collection.FindOne(Query.All())), new[] { "_id", "GroupId", "GroupHash", "Records", "Metadata" });

            // Apply filter and projection later
            projectionExpr = _projectionExpr;
            // Filtering
            if (filterExpr != null)
                query = query.Where(filterExpr);
        }


        // Sorting
        if (!string.IsNullOrWhiteSpace(sortColumn) && IsValidSortField(sortColumn))
        {
            query = ascending
                ? query.OrderBy(GetSortColumn(sortColumn, collectionName))
                : query.OrderByDescending(GetSortColumn(sortColumn, collectionName));
        }

        // Projection
        ILiteQueryableResult<BsonDocument> result = query;
        if (projectionExpr != null)
            result = query.Select(projectionExpr);

        var totalCount = result.Count();
        var data = await Task.Run(() =>
            result.Skip((pageNumber - 1) * pageSize)
                  .Limit(pageSize)
                  .ToList()
                  .Select(ConvertFromBsonDocument)
                  .ToList());

        return (data, totalCount);
    }

    private string BuildCondition(string path, string op, string value)
    {
        var parsedValue = ParseValue(value);
        string formattedValue = FormatValueForQuery(parsedValue);


        if (op == "contains" && value.Contains("|"))
        {
            var parts = value.Split('|');
            //var orConditions = parts.Select(v => $"{path} LIKE '%{v}%' OR {path} = '{v}'");
            var orConditions = parts.Select(v => v.Contains("%") ? $"{path} LIKE '{v}'" : $"{path} LIKE '%{v}%'");
            return string.Join(" OR ", orConditions);
        }
        return op switch
        {
            "=" => $"{path} = {formattedValue}",
            "contains" => $"{path} LIKE '%{value}%'",  // Contains assumes string; original value used for LIKE
            ">" => $"{path} > {formattedValue}",
            "<" => $"{path} < {formattedValue}",
            ">=" => $"{path} >= {formattedValue}",
            "<=" => $"{path} <= {formattedValue}",
            _ => $"{path} = {formattedValue}"
        };
    }

    private object ParseValue(string value)
    {
        // Attempt parsing in order of specificity (bool first, then numbers, then complex types)
        if (bool.TryParse(value, out bool b)) return b;
        if (int.TryParse(value, out int i)) return i;
        if (long.TryParse(value, out long l)) return l;
        if (double.TryParse(value, out double d)) return d;
        if (DateTime.TryParse(value, out DateTime dt)) return dt;
        if (Guid.TryParse(value, out Guid g)) return g;
        // ObjectId parsing (LiteDB-specific; try-catch for invalid hex)
        try { return new ObjectId(value); } catch { }
        return value; // Fallback to string
    }
    private string FormatValueForQuery(object parsedValue)
    {
        return parsedValue switch
        {
            bool b => b.ToString().ToLower(),  // "true" or "false" (unquoted)
            int or long or double => parsedValue.ToString(),  // Numeric as-is
            DateTime dt => $"DATETIME('{dt:yyyy-MM-ddTHH:mm:ss.fffZ}')",  // ISO 8601 format for LiteDB
            Guid g => $"GUID('{g:D}')",  // Standard GUID format
            ObjectId oid => $"OBJECTID('{oid}')",  // Hex string for ObjectId
            _ => $"'{parsedValue}'"  // Strings quoted
        };
    }
    private Dictionary<string, List<string>> BuildJsonPaths(BsonValue root)
    {
        var map = new Dictionary<string, List<string>>();

        if (root == null || root.IsNull)
            return map;

        void Walk(BsonValue node, string path)
        {
            if (node.IsDocument)
            {
                foreach (var kv in node.AsDocument)
                {
                    Walk(kv.Value, $"{path}.{kv.Key}");
                }
            }
            else if (node.IsArray)
            {
                var array = node.AsArray;

                // Find first non-null element (best representation)
                var first = array.FirstOrDefault(a => !a.IsNull);

                if (first != null)
                {
                    Walk(first, $"{path}[*]");
                }
            }
            else
            {
                string key = path.Split('.').Last().Replace("[*]", "");
                if (!map.ContainsKey(key))
                    map[key] = new List<string>();

                if (!map[key].Contains(path))
                    map[key].Add(path);
            }
        }

        Walk(root, "$");

        return map;
    }

    private (BsonExpression filter, BsonExpression? projection) BuildSmartFilterAndProjection(
    string filters,
    Dictionary<string, List<string>> pathMap,
    IEnumerable<string>? projectionColumns = null)
    {
        if (string.IsNullOrWhiteSpace(filters))
            return (null, null);

        var andConditions = new List<string>();
        var projections = new List<string>();

        // Build projections from user-provided columns (support full paths)
        if (projectionColumns != null)
        {
            foreach (var col in projectionColumns)
            {
                var trimmed = col.Trim();
                if (!string.IsNullOrEmpty(trimmed))
                {
                    if (trimmed.StartsWith("$.")) // Full JSON path
                    {
                        // Extract a simple alias from the path (e.g., "$.Records[*].address" -> "address")
                        var pathParts = trimmed.Split('.');
                        var alias = pathParts.Last().Replace("[*]", "").Replace("]", "").Replace("[", "");
                        projections.Add($"{alias}: {trimmed}");
                    }
                    else // Simple field name
                    {
                        projections.Add($"{trimmed}: {trimmed}");
                    }
                }
            }
        }

        var filterPairs = filters.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var pair in filterPairs)
        {
            string key = string.Empty;
            string op = "=";
            string value = string.Empty;

            if (pair.Contains(">=")) { var p = pair.Split(">="); key = p[0]; op = ">="; value = p[1]; }
            else if (pair.Contains("<=")) { var p = pair.Split("<="); key = p[0]; op = "<="; value = p[1]; }
            else if (pair.Contains(">")) { var p = pair.Split(">"); key = p[0]; op = ">"; value = p[1]; }
            else if (pair.Contains("<")) { var p = pair.Split("<"); key = p[0]; op = "<"; value = p[1]; }
            else if (pair.Contains("~")) { var p = pair.Split("~"); key = p[0]; op = "contains"; value = p[1]; }
            else if (pair.Contains("=")) { var p = pair.Split("="); key = p[0]; op = "="; value = p[1]; }

            key = key.Trim();
            value = value.Trim();

            // Resolve to full JSON paths
            List<string> jsonPaths;
            if (key.StartsWith("$.")) // Full JSON path provided
            {
                jsonPaths = new List<string> { key };
            }
            else if (key.Contains(".")) // Nested path (add $ prefix and assume [*] for arrays)
            {
                // Convert to full path with [*] for arrays (e.g., Records._group_match_details.neighbor -> $.Records[*]._group_match_details[*].neighbor)
                var segments = key.Split('.');
                var fullPath = "$";
                for (int i = 0; i < segments.Length; i++)
                {
                    fullPath += $".{segments[i]}";
                    // Assume arrays based on common patterns or pathMap; adjust if needed
                    if (i < segments.Length - 1 && (segments[i] == "Records" || segments[i].StartsWith("_"))) // Customize array detection
                        fullPath += "[*]";
                }
                jsonPaths = new List<string> { fullPath };
            }
            else if (pathMap.ContainsKey(key))
            {
                jsonPaths = pathMap[key];
            }
            else
            {
                jsonPaths = new List<string> { "$." + key };
            }

            foreach (var jsonPath in jsonPaths)
            {
                // Build dynamic nested COUNT expression
                var (filterCondition, arrayProjection) = BuildNestedArrayFilter(jsonPath, op, value);
                if (!string.IsNullOrEmpty(filterCondition))
                    andConditions.Add(filterCondition);
                if (!string.IsNullOrEmpty(arrayProjection) && !projections.Contains(arrayProjection))
                    projections.Add(arrayProjection);
            }
        }

        var filterExpr = andConditions.Any() ? BsonExpression.Create(string.Join(" AND ", andConditions)) : null;
        // Create projection only if columns were provided or array projections exist
        var projectionExpr = projections.Any()
            ? BsonExpression.Create("{" + string.Join(", ", projections) + "}")
            : null;

        return (filterExpr, projectionExpr);
    }

    // Helper to build nested COUNT expressions dynamically
    private (string filter, string projection) BuildNestedArrayFilter(string jsonPath, string op, string value)
    {
        // Parse path into segments (e.g., ["$", "Records[*]", "_group_match_details[*]", "neighbor"])
        var segments = jsonPath.Split('.').Where(s => !string.IsNullOrEmpty(s)).ToArray();
        if (segments.Length < 2) return ("", ""); // Not a nested path

        // Find the deepest field and build from there
        var fieldSegment = segments.Last();
        var arraySegments = segments.Skip(1).Take(segments.Length - 2).Where(s => s.Contains("[*]")).ToArray(); // Arrays in between
        var rootArray = segments[1].Replace("[*]", ""); // e.g., "Records"

        // Build inner condition: @.neighbor LIKE 'value%'
        var innerCondition = BuildCondition($"@.{fieldSegment}", op, value);

        // Recursively build nested COUNT
        string currentFilter = innerCondition;
        string currentProjection = "";

        // Work backwards from the deepest array
        for (int i = arraySegments.Length - 1; i > 0; i--)
        {
            var arrayName = arraySegments[i].Replace("[*]", "");
            currentFilter = $"COUNT(@.{arrayName}[{currentFilter}]) > 0";
        }

        // Wrap with root array
        var finalFilter = $"COUNT($.{rootArray}[{currentFilter}]) > 0";
        var finalProjection = $"{rootArray}: $.{rootArray}[{currentFilter}]";

        return (finalFilter, finalProjection);
    }

    public Task<bool> CollectionExistsAsync(string collectionName)
    {
        return Task.Run(() =>
        {
            return _db.CollectionExists(collectionName);
        });
    }
    public async Task<bool> RenameCollection(string oldName, string newName)
    {
        bool returnValue = _db.RenameCollection(oldName, newName);
        return await Task.FromResult(returnValue);
    }

    /// <summary>
    /// Bulk upsert entities by Id (most efficient for LiteDB)
    /// </summary>
    public async Task BulkUpsertAsync<T>(IEnumerable<T> entities, string collectionName)
    {
        try
        {
            var collection = _db.GetCollection<T>(collectionName);
            var entityList = entities.ToList();

            if (!entityList.Any())
            {
                _logger.LogWarning("No entities to upsert in {CollectionName}", collectionName);
                return;
            }

            await Task.Run(() =>
            {
                _db.BeginTrans();

                try
                {
                    for (int i = 0; i < entityList.Count; i += CommitBatchSize)
                    {
                        var batch = entityList.Skip(i).Take(CommitBatchSize);

                        // LiteDB's Upsert is very efficient - uses Id field
                        foreach (var entity in batch)
                        {
                            collection.Upsert(entity);
                        }

                        // Commit in batches
                        if (i + CommitBatchSize >= entityList.Count ||
                            (i + CommitBatchSize) % (CommitBatchSize * 5) == 0)
                        {
                            _db.Commit();
                            if (i + CommitBatchSize < entityList.Count)
                            {
                                _db.BeginTrans();
                            }
                        }
                    }

                    _logger.LogInformation(
                        "Bulk upserted {Count} records into {CollectionName}",
                        entityList.Count,
                        collectionName);
                }
                catch
                {
                    _db.Rollback();
                    throw;
                }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error bulk upserting into {CollectionName}", collectionName);
            throw;
        }
    }

    /// <summary>
    /// Bulk upsert with custom match criteria (e.g., DataSourceId + FieldName)
    /// This is MORE efficient than individual queries
    /// </summary>
    public async Task BulkUpsertByFieldsAsync<T>(
        IEnumerable<T> entities,
        string collectionName,
        Expression<Func<T, object>>[] matchFields)
    {
        try
        {
            var collection = _db.GetCollection<T>(collectionName);
            var entityList = entities.ToList();

            if (!entityList.Any())
            {
                _logger.LogWarning("No entities to upsert in {CollectionName}", collectionName);
                return;
            }

            // Ensure indexes exist on match fields for performance
            foreach (var field in matchFields)
            {
                var memberExpression = GetMemberExpression(field);
                if (memberExpression != null)
                {
                    collection.EnsureIndex(memberExpression.Member.Name);
                }
            }

            await Task.Run(() =>
            {
                _db.BeginTrans();

                try
                {
                    int upserted = 0;
                    int updated = 0;
                    int inserted = 0;

                    for (int i = 0; i < entityList.Count; i += CommitBatchSize)
                    {
                        var batch = entityList.Skip(i).Take(CommitBatchSize);

                        foreach (var entity in batch)
                        {
                            // Build query to find existing record
                            var query = BuildMatchQuery(entity, matchFields, collection);
                            var existing = query.FirstOrDefault();

                            if (existing != null)
                            {
                                // Update: preserve the original Id
                                var idProp = typeof(T).GetProperty("Id");
                                if (idProp != null)
                                {
                                    var existingId = idProp.GetValue(existing);
                                    idProp.SetValue(entity, existingId);
                                }
                                collection.Update(entity);
                                updated++;
                            }
                            else
                            {
                                // Insert new
                                collection.Insert(entity);
                                inserted++;
                            }
                            upserted++;
                        }

                        // Commit in batches
                        if (i + CommitBatchSize >= entityList.Count ||
                            (i + CommitBatchSize) % (CommitBatchSize * 5) == 0)
                        {
                            _db.Commit();
                            if (i + CommitBatchSize < entityList.Count)
                            {
                                _db.BeginTrans();
                            }
                        }
                    }

                    _logger.LogInformation(
                        "Bulk upserted {Total} records into {CollectionName}: {Inserted} inserted, {Updated} updated",
                        upserted, collectionName, inserted, updated);
                }
                catch
                {
                    _db.Rollback();
                    throw;
                }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error bulk upserting by fields into {CollectionName}", collectionName);
            throw;
        }
    }

    private ILiteQueryable<T> BuildMatchQuery<T>(
       T entity,
       Expression<Func<T, object>>[] matchFields,
       ILiteCollection<T> collection)
    {
        ILiteQueryable<T> query = collection.Query();

        foreach (var field in matchFields)
        {
            var memberExpression = GetMemberExpression(field);
            if (memberExpression != null)
            {
                var propertyName = memberExpression.Member.Name;
                var property = typeof(T).GetProperty(propertyName);
                var value = property?.GetValue(entity);

                if (value != null)
                {
                    // Create expression: x => x.PropertyName == value
                    var parameter = Expression.Parameter(typeof(T), "x");
                    var propertyAccess = Expression.Property(parameter, propertyName);
                    var constant = Expression.Constant(value);
                    var equals = Expression.Equal(propertyAccess, constant);
                    var lambda = Expression.Lambda<Func<T, bool>>(equals, parameter);

                    query = query.Where(lambda);
                }
            }
        }

        return query;
    }

    private MemberExpression GetMemberExpression<T>(Expression<Func<T, object>> expression)
    {
        if (expression.Body is MemberExpression memberExpression)
        {
            return memberExpression;
        }

        if (expression.Body is UnaryExpression unaryExpression &&
            unaryExpression.Operand is MemberExpression operand)
        {
            return operand;
        }

        return null;
    }

    public Task CreateGroupFilterIndexesAsync(string collectionName)
    {
        return Task.FromResult(0);
        //throw new NotImplementedException();
    }
}


