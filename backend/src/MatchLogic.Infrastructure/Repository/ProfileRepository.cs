using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Auth.Interfaces;
using MatchLogic.Domain.DataProfiling;
using LiteDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Repository;

public class ProfileRepository : GenericRepository<ProfileResult, Guid>, IProfileRepository
{
    private const string ROW_REFERENCE_DOCUMENTS_COLLECTION = "RowReferenceDocument";

    public ProfileRepository(Func<StoreType, IDataStore> storeFactory,
        IStoreTypeResolver storeTypeResolver,
        ICurrentUser currentUser)
        : base(storeFactory, storeTypeResolver, currentUser)
    {
    }

    /// <summary>
    /// Save profile result with all row references
    /// Generic method handles both ProfileResult and AdvancedProfileResult
    /// </summary>
    private async Task<Guid> SaveProfileResultAsync<TProfileResult>(
        TProfileResult profileResult,
        Dictionary<string, Dictionary<ProfileCharacteristic, List<RowReference>>> characteristicRowsByColumn,
        Dictionary<string, Dictionary<string, List<RowReference>>> patternRowsByColumn,
        Dictionary<string, Dictionary<string, List<RowReference>>> valueRowsByColumn,
        string collectionName) where TProfileResult : ProfileResult
    {
        string rowReferenceCollectionName = $"{collectionName}_{ROW_REFERENCE_DOCUMENTS_COLLECTION}";

        await _dataStore.DeleteCollection(collectionName);
        await _dataStore.DeleteCollection(rowReferenceCollectionName);

        profileResult.RowReferenceDocumentIds = new List<Guid>();

        // Buffer all RowReferenceDocuments and bulk-insert in one round-trip.
        // The original per-document InsertAsync loop spawned 1000+ network round-trips
        // per profile and was unworkable against MongoDB Atlas (~300ms WAN latency).
        var documents = new List<RowReferenceDocument>();

        foreach (var columnPair in profileResult.ColumnProfiles)
        {
            var columnName = columnPair.Key;
            var columnProfile = columnPair.Value;

            columnProfile.CharacteristicRowDocumentIds = new Dictionary<ProfileCharacteristic, Guid>();
            columnProfile.PatternMatchRowDocumentIds = new Dictionary<string, Guid>();
            columnProfile.ValueRowDocumentIds = new Dictionary<string, Guid>();

            if (characteristicRowsByColumn.TryGetValue(columnName, out var characteristicRows))
            {
                foreach (var pair in characteristicRows)
                {
                    var doc = BuildRowReferenceDocument(
                        profileResult.Id, columnName, ReferenceType.Characteristic, pair.Key.ToString(), pair.Value);
                    columnProfile.CharacteristicRowDocumentIds[pair.Key] = doc.Id;
                    profileResult.RowReferenceDocumentIds.Add(doc.Id);
                    documents.Add(doc);
                }
            }

            if (patternRowsByColumn.TryGetValue(columnName, out var patternRows))
            {
                foreach (var pair in patternRows)
                {
                    var doc = BuildRowReferenceDocument(
                        profileResult.Id, columnName, ReferenceType.Pattern, pair.Key, pair.Value);
                    columnProfile.PatternMatchRowDocumentIds[pair.Key] = doc.Id;
                    profileResult.RowReferenceDocumentIds.Add(doc.Id);
                    documents.Add(doc);
                }
            }

            if (valueRowsByColumn.TryGetValue(columnName, out var valueRows))
            {
                foreach (var pair in valueRows)
                {
                    var doc = BuildRowReferenceDocument(
                        profileResult.Id, columnName, ReferenceType.Value, pair.Key, pair.Value);
                    columnProfile.ValueRowDocumentIds[pair.Key] = doc.Id;
                    profileResult.RowReferenceDocumentIds.Add(doc.Id);
                    documents.Add(doc);
                }
            }
        }

        if (documents.Count > 0)
            await _dataStore.BulkInsertAsync(documents, rowReferenceCollectionName);

        await _dataStore.InsertAsync(profileResult, collectionName);

        return profileResult.Id;
    }

    private static RowReferenceDocument BuildRowReferenceDocument(
        Guid profileId,
        string columnName,
        ReferenceType type,
        string key,
        List<RowReference> rows) => new()
    {
        Id = Guid.NewGuid(),
        ProfileResultId = profileId,
        ColumnName = columnName,
        Type = type,
        Key = key,
        Rows = rows
    };

    /// <summary>
    /// Save a row reference document
    /// </summary>
    private async Task<Guid> SaveRowReferenceDocumentAsync(
        Guid profileId,
        string columnName,
        ReferenceType type,
        string key,
        List<RowReference> rows,
        string collectionName)
    {
        // Create the document
        var document = new RowReferenceDocument
        {
            Id = Guid.NewGuid(),
            ProfileResultId = profileId,
            ColumnName = columnName,
            Type = type,
            Key = key,
            Rows = rows
        };

        // Save to database
        await _dataStore.InsertAsync(document, collectionName);

        return document.Id;
    }

    /// <summary>
    /// Get profile result by ID
    /// </summary>
    public async Task<ProfileResult> GetProfileResultAsync(Guid profileId, string collectionName)
    {
        var result = await _dataStore.GetByIdAsync<ProfileResult, Guid>(profileId, collectionName);
        return result;
    }

    public async Task<AdvancedProfileResult> GetAdvanceProfileResultAsync(Guid profileId, string collectionName)
    {
        var result = await _dataStore.GetByIdAsync<AdvancedProfileResult, Guid>(profileId, collectionName);
        return result;
    }

    /// <summary>
    /// Get row references by document ID
    /// </summary>
    public async Task<List<RowReference>> GetRowReferencesByDocumentIdAsync(Guid documentId, string collectionName)
    {
        var document = await _dataStore.GetByIdAsync<RowReferenceDocument, Guid>(documentId, collectionName);
        return document?.Rows ?? new List<RowReference>();
    }

    /// <summary>
    /// Get row references by type and key
    /// </summary>
    public async Task<List<RowReference>> GetRowReferencesByTypeAsync(
        Guid profileId,
        string columnName,
        ReferenceType type,
        string key,
        string collectionName)
    {
        string lookupKey = $"{profileId}_{columnName}_{type}_{key}";
        var document = await _dataStore.QueryAsync<RowReferenceDocument>(rd => rd.LookupKey == lookupKey, collectionName);
        var first = document.FirstOrDefault();
        return first?.Rows ?? new List<RowReference>();
    }

    /// <summary>
    /// Delete profile result and all associated row reference documents
    /// </summary>
    public async Task DeleteProfileResultAsync(Guid profileId, string collectionName)
    {
        // Get the profile result
        var profileResult = await _dataStore.GetByIdAsync<ProfileResult, Guid>(profileId, collectionName);

        if (profileResult == null)
            return;

        // Delete all associated row reference documents
        await _dataStore.DeleteAllAsync<RowReferenceDocument>(
            x => profileResult.RowReferenceDocumentIds.Contains(x.Id),
            $"{collectionName}_{ROW_REFERENCE_DOCUMENTS_COLLECTION}");

        // Delete the profile result
        await _dataStore.DeleteAsync(profileId, collectionName);
    }

    // Public methods - just delegate to generic method
    public async Task<Guid> SaveProfileResultAsync(
        ProfileResult profileResult,
        Dictionary<string, Dictionary<ProfileCharacteristic, List<RowReference>>> characteristicRowsByColumn,
        Dictionary<string, Dictionary<string, List<RowReference>>> patternRowsByColumn,
        Dictionary<string, Dictionary<string, List<RowReference>>> valueRowsByColumn,
        string collectionName)
    {
        return await SaveProfileResultAsync<ProfileResult>(
            profileResult,
            characteristicRowsByColumn,
            patternRowsByColumn,
            valueRowsByColumn,
            collectionName);
    }

    // Good - just delegates, no duplicate code
    public async Task<Guid> SaveProfileResultAsync(
        AdvancedProfileResult profileResult,
        Dictionary<string, Dictionary<ProfileCharacteristic, List<RowReference>>> characteristicRowsByColumn,
        Dictionary<string, Dictionary<string, List<RowReference>>> patternRowsByColumn,
        Dictionary<string, Dictionary<string, List<RowReference>>> valueRowsByColumn,
        string collectionName)
    {
        return await SaveProfileResultAsync<AdvancedProfileResult>(
            profileResult,
            characteristicRowsByColumn,
            patternRowsByColumn,
            valueRowsByColumn,
            collectionName);
    }
}