using MatchLogic.Application.Common;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace MatchLogic.Application.Services;

public class FieldMappingService : IFieldMappingService
{
    private readonly IDataStore _dataStore;
    private readonly IGenericRepository<MatchDefinitionCollection, Guid> _matchDefinitionRepository;
    private readonly ILogger<FieldMappingService> _logger;

    public FieldMappingService(
        IDataStore dataStore,
        IGenericRepository<MatchDefinitionCollection, Guid> matchDefinitionRepository,
        ILogger<FieldMappingService> logger)
    {
        _dataStore = dataStore;
        _matchDefinitionRepository = matchDefinitionRepository;       
        _logger = logger;
    }

    /// <summary>
    /// ONE method for ALL sources: Cleansing, DataRefresh, etc.
    /// </summary>
    public async Task SyncSystemGeneratedFieldsAsync(
        Guid dataSourceId,
        string dataSourceName,
        List<FieldColumnInfo> currentColumns,
        FieldOrigin origin,
        Guid projectId,
        Guid? sourceOperationId = null)
    {
        try
        {
            _logger.LogInformation(
                "Syncing {Origin} fields for DataSource {DataSourceId}",
                origin, dataSourceId);

            // 1. Get existing fields from this origin
            var existingFields = await _dataStore.QueryAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId &&
                     x.Origin == origin &&
                     x.IsSystemManaged == true,
                Constants.Collections.FieldMapping);

            // 2. Calculate differences
            var currentNames = currentColumns.Select(c => c.Name).ToHashSet();
            var existingNames = existingFields.Select(f => f.FieldName).ToHashSet();

            var toAdd = currentColumns.Where(c => !existingNames.Contains(c.Name)).ToList();
            var toRemove = existingFields.Where(f => !currentNames.Contains(f.FieldName)).ToList();
            var toUpdate = currentColumns
                .Where(c => existingNames.Contains(c.Name))
                .Select(c => existingFields.First(f => f.FieldName == c.Name))
                .Where(f => !f.IsActive)
                .ToList();

            _logger.LogInformation(
                "Sync plan: {Add} to add, {Remove} to remove, {Update} to reactivate",
                toAdd.Count, toRemove.Count, toUpdate.Count);

            // 3. Handle removals (check usage first!)
            if (toRemove.Any())
            {
                await HandleOrphanedFieldsAsync(toRemove, projectId, origin);
            }

            // 4. Handle additions
            if (toAdd.Any())
            {
                await AddFieldMappingsAsync(
                    toAdd, dataSourceId, dataSourceName, origin, sourceOperationId);
            }

            // 5. Reactivate if field came back
            if (toUpdate.Any())
            {
                foreach (var field in toUpdate)
                {
                    field.IsActive = true;
                    field.InactivatedAt = null;
                    field.InactivationReason = null;
                    field.UpdatedAt = DateTime.UtcNow;
                }

                await _dataStore.BulkUpsertByFieldsAsync(
                    toUpdate,
                    Constants.Collections.FieldMapping,
                    new Expression<Func<FieldMappingEx, object>>[]
                    {
                        x => x.DataSourceId,
                        x => x.FieldName
                    });

                _logger.LogInformation("Reactivated {Count} fields", toUpdate.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error syncing {Origin} fields for DataSource {DataSourceId}",
                origin, dataSourceId);
            throw;
        }
    }

    /// <summary>
    /// Handle data source refresh (future)
    /// </summary>
    public async Task HandleDataSourceRefreshAsync(
        Guid dataSourceId,
        string dataSourceName,
        List<string> newSchemaColumns,
        Guid projectId)
    {
        _logger.LogInformation("Handling data refresh for DataSource {DataSourceId}", dataSourceId);

        // Get existing IMPORT fields only
        var existingFields = await _dataStore.QueryAsync<FieldMappingEx>(
            x => x.DataSourceId == dataSourceId && x.Origin == FieldOrigin.Import,
            Constants.Collections.FieldMapping);

        var existingNames = existingFields.Select(f => f.FieldName).ToHashSet();
        var newNames = newSchemaColumns.ToHashSet();

        // New columns
        var newColumns = newSchemaColumns
            .Where(col => !existingNames.Contains(col))
            .Select((col, idx) => new FieldColumnInfo
            {
                Name = col,
                Index = existingFields.Count + idx,
                IsNewColumn = true,
                SourceOperation = $"DataRefresh_{DateTime.UtcNow:yyyyMMdd}"
            })
            .ToList();

        // Removed columns
        var removedFields = existingFields.Where(f => !newNames.Contains(f.FieldName)).ToList();

        // Add new columns with DataRefresh origin
        if (newColumns.Any())
        {
            await SyncSystemGeneratedFieldsAsync(
                dataSourceId,
                dataSourceName,
                newColumns,
                FieldOrigin.DataRefresh,
                projectId,
                Guid.NewGuid());
        }

        // Handle removed columns
        if (removedFields.Any())
        {
            await HandleOrphanedFieldsAsync(removedFields, projectId, FieldOrigin.Import);
        }
    }

    public async Task<List<FieldMappingEx>> GetActiveFieldMappingsAsync(
        Guid dataSourceId,
        bool includeSystemGenerated = true)
    {
        if (includeSystemGenerated)
        {
            return await _dataStore.QueryAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId && x.IsActive == true,
                Constants.Collections.FieldMapping);
        }
        else
        {
            return await _dataStore.QueryAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId &&
                     x.IsActive == true &&
                     x.Origin == FieldOrigin.Import,
                Constants.Collections.FieldMapping);
        }
    }

    public async Task<List<FieldMappingEx>> GetFieldMappingsByOriginAsync(
        Guid dataSourceId,
        FieldOrigin origin)
    {
        return await _dataStore.QueryAsync<FieldMappingEx>(
            x => x.DataSourceId == dataSourceId && x.Origin == origin,
            Constants.Collections.FieldMapping);
    }

    // PRIVATE HELPERS

    private async Task HandleOrphanedFieldsAsync(
    List<FieldMappingEx> orphanedFields,
    Guid projectId,
    FieldOrigin origin)
    {
        // Reuse classification method
        var (mustKeep, safeToDelete) = await ClassifyFieldsByUsageAsync(
            orphanedFields,
            projectId);

        // Log warnings
        foreach (var field in mustKeep)
        {
            _logger.LogWarning(
                "Cannot delete '{FieldName}' ({Origin}): used in match definitions",
                field.FieldName, origin);
        }

        // Soft delete if used
        if (mustKeep.Any())
        {
            foreach (var field in mustKeep)
            {
                field.IsActive = false;
                field.InactivatedAt = DateTime.UtcNow;
                field.InactivationReason = $"{origin} operation removed";
                field.UpdatedAt = DateTime.UtcNow;
            }

            await _dataStore.BulkUpsertByFieldsAsync(
                mustKeep,
                Constants.Collections.FieldMapping,
                new Expression<Func<FieldMappingEx, object>>[]
                {
                x => x.DataSourceId,
                x => x.FieldName
                });

            _logger.LogInformation("Marked {Count} fields as inactive", mustKeep.Count);
        }

        // Hard delete if not used
        if (safeToDelete.Any())
        {
            var ids = safeToDelete.Select(f => f.Id).ToList();
            var deleteCount = await _dataStore.DeleteAllAsync<FieldMappingEx>(
                x => ids.Contains(x.Id),
                Constants.Collections.FieldMapping);

            _logger.LogInformation("Deleted {Count} orphaned fields", deleteCount);
        }
    }

    private async Task AddFieldMappingsAsync(
        List<FieldColumnInfo> columns,
        Guid dataSourceId,
        string dataSourceName,
        FieldOrigin origin,
        Guid? sourceOperationId)
    {
        var fieldMappings = columns.Select((col, idx) => new FieldMappingEx
        {
            Id = Guid.NewGuid(),
            DataSourceId = dataSourceId,
            DataSourceName = dataSourceName,
            FieldName = col.Name,
            FieldIndex = col.Index ?? idx,
            DataType = col.DataType ?? typeof(string).ToString(),
            Origin = origin,
            IsSystemManaged = true,
            IsActive = true,
            SourceReferenceId = sourceOperationId,
            CreatedAt = DateTime.UtcNow
        }).ToList();

        await _dataStore.BulkUpsertByFieldsAsync(
            fieldMappings,
            Constants.Collections.FieldMapping,
            new Expression<Func<FieldMappingEx, object>>[]
            {
                x => x.DataSourceId,
                x => x.FieldName
            });

        _logger.LogInformation("Added {Count} new {Origin} fields", fieldMappings.Count, origin);
    }

    private bool IsFieldUsedInMatchDefinitions(
    FieldMappingEx field,
    List<MatchDefinition> matchDefinitions)
    {
        if (matchDefinitions != null && matchDefinitions.Count > 0)
        {
            foreach (var matchDef in matchDefinitions)
            {
                // Check all Criteria (replaces old BlockingCriteria + MatchingCriteria)
                if (matchDef.Criteria?.Any(criteria =>
                    criteria.FieldMappings?.Any(fm =>
                        fm.DataSourceId == field.DataSourceId &&
                        fm.FieldName == field.FieldName) == true) == true)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Classify fields by usage in MatchDefinition
    /// Returns: (fields used in match, fields safe to delete)
    /// </summary>
    public async Task<(List<FieldMappingEx> UsedInMatch, List<FieldMappingEx> SafeToRemove)>
        ClassifyFieldsByUsageAsync(
            List<FieldMappingEx> fields,
            Guid projectId)
    {
        var projectMatchDefs = await _matchDefinitionRepository.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MatchDefinitionCollection);

        var definitions = projectMatchDefs?.FirstOrDefault()?.Definitions;

        var usedInMatch = new List<FieldMappingEx>();
        var safeToRemove = new List<FieldMappingEx>();

        foreach (var field in fields)
        {
            if (IsFieldUsedInMatchDefinitions(field, definitions))
            {
                usedInMatch.Add(field);
            }
            else
            {
                safeToRemove.Add(field);
            }
        }

        return (usedInMatch, safeToRemove);
    }
}