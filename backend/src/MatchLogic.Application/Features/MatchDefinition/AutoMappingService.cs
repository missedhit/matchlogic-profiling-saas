using Ardalis.Result;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchDefinition;

/// <summary>
/// Class for auto-mapping fields.
/// </summary>
public class AutoMappingService : IAutoMappingService
{
    private readonly IGenericRepository<FieldMappingEx, Guid> _fieldsRepository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedFieldRowsRepository;
    private readonly IStringSimilarityCalculator _similarityCalculator;
    private readonly IDataStore _dataStore;
    private readonly IHeaderUtility _headerUtility;
    private readonly IFieldMappingService _fieldMappingService;
    /// <summary>
    /// Creates a new instance of the AutoMappingService class.
    /// </summary>
    public AutoMappingService(IGenericRepository<FieldMappingEx, Guid> fieldsRepository,
        IGenericRepository<DataSource, Guid> dataSourceRepository,
        IGenericRepository<MappedFieldsRow, Guid> mappedFieldRowsRepository,
        IStringSimilarityCalculator similarityCalculator,
        IDataStore dataStore,
        IHeaderUtility headerUtility,
        IFieldMappingService fieldMappingService)
    {
        _fieldsRepository = fieldsRepository;
        _dataSourceRepository = dataSourceRepository;
        _mappedFieldRowsRepository = mappedFieldRowsRepository;
        _similarityCalculator = similarityCalculator;
        _dataStore = dataStore;
        _headerUtility = headerUtility;
        _fieldMappingService = fieldMappingService;
    }

    /// <summary>
    /// Gets extended field information for a project.
    /// </summary>
    public async Task<Dictionary<string, List<FieldMappingEx>>> GetExtendedFieldsAsync(
    Guid projectId,
    bool activeOnly = true)
    {
        var dataSources = await _dataSourceRepository
            .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.DataSources);

        var result = new Dictionary<string, List<FieldMappingEx>>();

        foreach (var dataSource in dataSources)
        {           
            var fields = await _fieldMappingService.GetActiveFieldMappingsAsync(
                dataSource.Id,
                includeSystemGenerated: true);

            var extendedFields = fields
                .OrderBy(x => x.FieldIndex)
                .ToList();

            result[dataSource.Name] = extendedFields;
        }

        return result;
    }


    public async Task DeleteExtendedFieldsByProjectAsync(Guid projectId)
    {
        var dataSources = await _dataSourceRepository
           .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.DataSources);

        foreach (var dataSource in dataSources)
        {
            await _fieldsRepository.DeleteAllAsync(x => x.DataSourceId == dataSource.Id, Constants.Collections.FieldMapping);
        }

    }
    public async Task InsertExtendedFieldsByProjectAsync(Guid projectId)
    {
        var dataSources = await _dataSourceRepository
           .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.DataSources);

        List<FieldMappingEx> fieldMappings = new List<FieldMappingEx>();

        foreach (var dataSource in dataSources)
        {
            var headers = await _headerUtility.GetHeadersAsync(dataSource, true);

            for (int key = 0; key < headers.Count; key++)
            {
                // SET ORIGIN
                FieldMappingEx fieldMappingEx = new FieldMappingEx()
                {
                    FieldName = headers[key],
                    DataSourceId = dataSource.Id,
                    DataSourceName = dataSource.Name,
                    FieldIndex = key,
                    DataType = typeof(string).ToString(),
                    Origin = FieldOrigin.Import,      //  Original import
                    IsSystemManaged = false,          //  Not auto-managed
                    IsActive = true,
                    CreatedAt = DateTime.UtcNow
                };

                fieldMappings.Add(fieldMappingEx);
            }
        }

        if (fieldMappings.Count > 0)
        {
            await _fieldsRepository.BulkInsertAsync(
                fieldMappings,
                Constants.Collections.FieldMapping);
        }
    }
    /// <summary>
    /// Performs auto-mapping of fields.
    /// </summary>
    public List<MappedFieldRow> AutoMapFields(Dictionary<string, List<FieldMappingEx>> fieldsPerDataSource)
    {
        // Create a list to hold the mapped rows
        var mappedRows = new List<MappedFieldRow>();

        // Get list of data source names
        var dataSourceNames = fieldsPerDataSource.Keys.ToList();

        // For each pair of data sources, generate auto-mapping candidates
        for (int i = 0; i < dataSourceNames.Count; i++)
        {
            // CRITICAL FIX: Accumulate ALL candidates between data source i and ALL subsequent data sources
            var allCandidatesForThisDataSource = new List<AutoMapPairCandidate>();

            for (int j = i + 1; j < dataSourceNames.Count; j++)
            {
                var dsName1 = dataSourceNames[i];
                var dsName2 = dataSourceNames[j];

                // Generate candidates for this pair
                var candidates = GenerateAutoMapCandidates(
                    fieldsPerDataSource[dsName1],
                    fieldsPerDataSource[dsName2]);

                // Add to accumulated list
                allCandidatesForThisDataSource.AddRange(candidates);
            }

            // Sort ALL accumulated candidates by score (highest first)
            allCandidatesForThisDataSource.Sort((a, b) => b.Score.CompareTo(a.Score));

            // Apply mappings for all pairs together (this is the key!)
            // This ensures fields from data source i can be mapped to ALL subsequent data sources
            // in the same row, not separate rows
            ApplyMappings(allCandidatesForThisDataSource, mappedRows, false);
            ApplyMappings(allCandidatesForThisDataSource, mappedRows, true);
        }

        // Include any remaining unmapped fields
        IncludeUnmappedFields(fieldsPerDataSource, mappedRows);

        return mappedRows;
    }

    /// <summary>
    /// Performs sequential mapping of fields based on field order/position.
    /// This method maps fields sequentially rather than by similarity score.
    /// </summary>
    public List<MappedFieldRow> SequentialMapFields(Dictionary<string, List<FieldMappingEx>> fieldsPerDataSource)
    {
        var mappedRows = new List<MappedFieldRow>();
        var dataSourceNames = fieldsPerDataSource.Keys.ToList();

        // Process each data source table
        foreach (string tableName in dataSourceNames)
        {
            var fieldsFromProcessedTable = fieldsPerDataSource[tableName];

            for (int i = 0; i < fieldsFromProcessedTable.Count; i++)
            {
                var field = fieldsFromProcessedTable[i];
                bool alreadyMapped = false;

                // Check if this field is already mapped in any existing row
                foreach (var row in mappedRows)
                {
                    var mappedField = row[tableName];
                    if (field.FieldName == mappedField?.FieldName)
                    {
                        alreadyMapped = true;
                        break;
                    }
                }

                // If not already mapped, add to a new row
                if (!alreadyMapped)
                {
                    var mappedFieldsRow = new MappedFieldRow();
                    mappedFieldsRow[tableName] = field;
                    mappedRows.Add(mappedFieldsRow);
                    field.Mapped = true; // Mark as mapped
                }
            }
        }

        return mappedRows;
    }

    /// <summary>
    /// Generates auto-map candidates between two lists of fields.
    /// </summary>
    private List<AutoMapPairCandidate> GenerateAutoMapCandidates(
        List<FieldMappingEx> fieldsFromFirstDataSource,
        List<FieldMappingEx> fieldsFromSecondDataSource)
    {
        var candidates = new List<AutoMapPairCandidate>();

        foreach (var field1 in fieldsFromFirstDataSource)
        {
            if (!field1.Mapped)
            {
                foreach (var field2 in fieldsFromSecondDataSource)
                {
                    if (!field2.Mapped)
                    {
                        var candidate = new AutoMapPairCandidate(field1, field2, _similarityCalculator);
                        candidates.Add(candidate);
                    }
                }
            }
        }

        // Sort by score, highest first
        candidates.Sort((a, b) => b.Score.CompareTo(a.Score));

        return candidates;
    }

    /// <summary>
    /// Applies mappings from candidates to the mapped rows list.
    /// </summary>
    private void ApplyMappings(List<AutoMapPairCandidate> candidates, List<MappedFieldRow> mappedRows, bool allowEmpty)
    {
        MappedFieldRow mappedFieldsRow = null;

        foreach (var candidate in candidates)
        {
            // Check if either field is empty (may happen in some cases)
            bool emptyExists = string.IsNullOrEmpty(candidate.MasterField.FieldName) ||
                               string.IsNullOrEmpty(candidate.SecondaryField.FieldName);

            if (emptyExists && !allowEmpty)
            {
                break;
            }

            if (candidate.SecondaryField.Mapped)
            {
                continue;
            }

            if (candidate.MasterField.Mapped && candidate.SecondaryField.Mapped)
            {
                continue;
            }

            if (!candidate.SecondaryField.Mapped || allowEmpty)
            {
                String masterFieldName = candidate.MasterField.FieldName;
                String masterFieldTableName = candidate.MasterField.DataSourceName;

                if ((!candidate.MasterField.Mapped))
                {
                    mappedFieldsRow = new MappedFieldRow();
                    mappedFieldsRow.AddField(candidate.MasterField);
                    mappedRows.Add(mappedFieldsRow);
                    candidate.MasterField.Mapped = true;
                }
                else
                {
                    int i = 0;
                    for (Int32 row = 0; row < mappedRows.Count; row++)
                    {
                        MappedFieldRow mappedFieldsRowTest = mappedRows[row];
                        if (mappedFieldsRowTest[masterFieldTableName].FieldName == masterFieldName)
                        {
                            mappedFieldsRow = mappedFieldsRowTest;
                            break;
                        }
                    }
                }

                if (mappedFieldsRow == null)
                {
                    mappedFieldsRow = new MappedFieldRow();
                }

                //if ((!autoMapPairCandidate.FieldMapInfoOther.Mapped))
                if (mappedFieldsRow[candidate.SecondaryField.DataSourceName] == null)
                //if (!mappedFieldsRow.IsPlaceUsed(autoMapPairCandidate.FieldMapInfoOther))
                {
                    mappedFieldsRow[candidate.SecondaryField.DataSourceName] =
                        candidate.SecondaryField;
                    candidate.SecondaryField.Mapped = true;
                }
            }
        }
    }


    /// <summary>
    /// Includes unmapped fields in the mapping.
    /// </summary>
    private void IncludeUnmappedFields(
        Dictionary<string, List<FieldMappingEx>> fieldsPerDataSource,
        List<MappedFieldRow> mappedRows)
    {
        foreach (var kvp in fieldsPerDataSource)
        {
            foreach (var field in kvp.Value)
            {
                if (!field.Mapped)
                {
                    var row = new MappedFieldRow();
                    row.AddField(field);
                    mappedRows.Add(row);
                    field.Mapped = true;
                }
            }
        }
    }

    /// <summary>
    /// Performs auto-mapping of fields for a project.
    /// </summary>
    public async Task<List<MappedFieldRow>> PerformAutoMappingAsync(Guid projectId)
    {
        // 1. Get extended field information for all data sources in the project
        var fieldsPerDataSource = await GetExtendedFieldsAsync(projectId);

        // 2. Perform auto-mapping
        var mappedRows = AutoMapFields(fieldsPerDataSource);

        mappedRows.ForEach(x => x.Include = true);

        // 3. Saving mappedFieldRows
        await SaveMappedFieldRows(projectId, mappedRows);

        return mappedRows;
    }

    /// <summary>
    /// Performs sequential mapping of fields for a project based on field order.
    /// </summary>
    public async Task<List<MappedFieldRow>> PerformSequentialMappingAsync(Guid projectId)
    {
        // 1. Get extended field information for all data sources in the project
        var fieldsPerDataSource = await GetExtendedFieldsAsync(projectId);

        // 2. Perform sequential mapping
        var mappedRows = SequentialMapFields(fieldsPerDataSource);

        mappedRows.ForEach(x => x.Include = true);

        // 3. Save mappedFieldRows
        await SaveMappedFieldRows(projectId, mappedRows);

        return mappedRows;
    }

    /// <summary>
    /// Gets the last saved mapped field rows for a project.
    /// </summary>
    public async Task<List<MappedFieldRow>> GetSavedMappedFieldRowsAsync(Guid projectId)
    {
        var mappedFieldsRow = await _mappedFieldRowsRepository
            .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.MappedFieldRows);

        var latestMapping = mappedFieldsRow.FirstOrDefault();

        return latestMapping?.MappedFields ?? new List<MappedFieldRow>();
    }

    public async Task<List<FieldMappingEx>> GetSavedExportableMappedFieldRowsAsync(Guid projectId)
    {
        List<FieldMappingEx> selectedMappedFields = new();
        var mappedFeildRow = await GetSavedMappedFieldRowsAsync(projectId);
        var dataSourcePairKey = mappedFeildRow[0].FieldByDataSource.Keys.ToArray();
        foreach (var row in mappedFeildRow)
        {
            var dataSource1 = dataSourcePairKey[0];
            var dataSource2 = dataSourcePairKey[1];
            var fieldByDataSource1 = row.FieldByDataSource[dataSource1];
            var fieldByDataSource2 = row.FieldByDataSource[dataSource2];
            var selectedField = ExportTypeResolver.DetermineLosslessExportField(fieldByDataSource1, fieldByDataSource2);
            selectedMappedFields.Add(selectedField);

        }
        return selectedMappedFields;
    }


    /// <summary>
    /// Deletes mapped field rows for a project.
    /// </summary>
    public async Task DeleteMappedFieldRowsAsync(Guid projectId)
    {
        var mappedFieldsRows = await _mappedFieldRowsRepository
            .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.MappedFieldRows);

        foreach (var row in mappedFieldsRows)
        {
            await _mappedFieldRowsRepository.DeleteAsync(row.Id, Constants.Collections.MappedFieldRows);
        }
    }

    /// <summary>
    /// Updates existing mapped field rows for a project.
    /// </summary>
    public async Task UpdateMappedFieldRowsAsync(Guid projectId, List<MappedFieldRow> mappedRows)
    {
        // Delete existing mappings
        await DeleteMappedFieldRowsAsync(projectId);

        // Save new mappings
        await SaveMappedFieldRows(projectId, mappedRows);
    }

    /// <summary>
    /// Save MappedField Rows
    /// </summary>
    /// <param name="projectId"></param>
    /// <param name="mappedRows"></param>
    /// <returns></returns>
    private async Task SaveMappedFieldRows(Guid projectId, List<MappedFieldRow> mappedRows)
    {
        await DeleteMappedFieldRowsAsync(projectId);

        var mappedFiledRows = new MappedFieldsRow()
        {
            MappedFields = mappedRows,
            ProjectId = projectId
        };
        await _mappedFieldRowsRepository.InsertAsync(mappedFiledRows, Constants.Collections.MappedFieldRows);
    }

    /// <summary>
    /// Clears all mapped fields and creates a basic mapping structure using only the first data source.
    /// Each field from the first data source gets its own row with other data sources empty.
    /// </summary>
    public List<MappedFieldRow> ClearMapFields(Dictionary<string, List<FieldMappingEx>> fieldsPerDataSource)
    {
        var mappedRows = new List<MappedFieldRow>();

        // Clear all mapped status
        foreach (var dataSourceFields in fieldsPerDataSource.Values)
        {
            foreach (var field in dataSourceFields)
            {
                field.Mapped = false;
            }
        }

        // Get the first data source (if any)
        if (fieldsPerDataSource.Count > 0)
        {
            var firstDataSource = fieldsPerDataSource.First();
            string tableName = firstDataSource.Key;
            var fieldsFromFirstTable = firstDataSource.Value;

            // Create a row for each field in the first data source
            for (int i = 0; i < fieldsFromFirstTable.Count; i++)
            {
                var field = fieldsFromFirstTable[i];
                var mappedFieldsRow = new MappedFieldRow();
                mappedFieldsRow[tableName] = field;
                mappedRows.Add(mappedFieldsRow);
                field.Mapped = true; // Mark as mapped
            }
        }

        return mappedRows;
    }

    /// <summary>
    /// Clears mapped fields for a project and creates a basic mapping structure using the first data source.
    /// </summary>
    public async Task<List<MappedFieldRow>> ClearMapFieldsAsync(Guid projectId)
    {
        // 1. Get extended field information for all data sources in the project
        var fieldsPerDataSource = await GetExtendedFieldsAsync(projectId);

        // 2. Clear mappings and create basic structure
        var mappedRows = ClearMapFields(fieldsPerDataSource);

        // 3. Save the cleared mappings
        await SaveMappedFieldRows(projectId, mappedRows);

        return mappedRows;
    }

    /// <summary>
    /// Add system-generated fields to MappedFieldRows
    /// Works for ANY origin: Cleansing, DataRefresh, Transformation, etc.
    /// </summary>
    public async Task AddSystemGeneratedFieldsToMappedRowsAsync(
        Guid projectId,
        Guid dataSourceId,
        List<FieldMappingEx> newFields)
    {
        if (!newFields.Any())
        {
            
            return;
        }

        // Get current saved mapping
        var savedMapping = await _mappedFieldRowsRepository
            .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.MappedFieldRows);

        var mappedFieldsRow = savedMapping.FirstOrDefault();

        if (mappedFieldsRow == null)
        {
            // No existing mapping, create new
            mappedFieldsRow = new MappedFieldsRow
            {
                Id = Guid.NewGuid(),
                ProjectId = projectId,
                MappedFields = new List<MappedFieldRow>()
            };
        }

        // Get data source info
        var dataSource = await _dataSourceRepository.GetByIdAsync(
            dataSourceId,
            Constants.Collections.DataSources);

        // Add each new field as a separate row
        foreach (var field in newFields)
        {
            // Check if row already exists for this field
            var existingRow = mappedFieldsRow.MappedFields?.FirstOrDefault(row =>
                row.FieldByDataSource.Values.Any(f =>
                    f.DataSourceId == field.DataSourceId &&
                    f.FieldName == field.FieldName));

            if (existingRow != null)
            {
                
                continue;
            }

            // Create new MappedFieldRow with this field
            var newRow = new MappedFieldRow
            {
                Include = true,  // Show in UI by default
                FieldByDataSource = new Dictionary<string, FieldMappingEx>
                {
                    // Add field with data source name as key
                    [dataSource.Name.ToLower()] = new FieldMappingEx
                    {
                        Id = field.Id,
                        DataSourceId = field.DataSourceId,
                        DataSourceName = field.DataSourceName,
                        FieldName = field.FieldName,
                        FieldIndex = field.FieldIndex,
                        DataType = field.DataType,
                        Origin = field.Origin,           // Preserves origin
                        IsSystemManaged = field.IsSystemManaged,
                        IsActive = field.IsActive,
                        Mapped = false  // Not mapped to match criteria yet
                    }
                }
            };

            mappedFieldsRow.MappedFields.Add(newRow);
        }

        // Save updated mapping
        await _mappedFieldRowsRepository.UpdateAsync(
            mappedFieldsRow,
            Constants.Collections.MappedFieldRows);
    }

    /// <summary>
    /// Remove specific fields from MappedFieldRows by field name
    /// This is called BEFORE FieldMapping deletion to clean up UI state
    /// </summary>
    public async Task RemoveFieldsFromMappedRowsAsync(
        Guid projectId,
        Guid dataSourceId,
        List<string> fieldNames)
    {
        if (!fieldNames.Any())
        {          
            return;
        }

        // Get current saved mapping
        var savedMapping = await _mappedFieldRowsRepository
            .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.MappedFieldRows);

        var mappedFieldsRow = savedMapping.FirstOrDefault();

        if (mappedFieldsRow == null || !mappedFieldsRow.MappedFields.Any())
        {        
            return;
        }

        var dataSource = await _dataSourceRepository.GetByIdAsync(
            dataSourceId,
            Constants.Collections.DataSources);

        var dataSourceKey = dataSource.Name.ToLower();
        var fieldNamesSet = new HashSet<string>(fieldNames, StringComparer.OrdinalIgnoreCase);
        var rowsToRemove = new List<MappedFieldRow>();
        bool hasChanges = false;

        foreach (var row in mappedFieldsRow.MappedFields)
        {
            // Check if this row has any of the fields to remove
            var keysToRemove = row.FieldByDataSource
                .Where(kvp =>
                    kvp.Value.DataSourceId == dataSourceId &&
                    fieldNamesSet.Contains(kvp.Value.FieldName))
                .Select(kvp => kvp.Key)
                .ToList();

            // Remove the fields
            foreach (var key in keysToRemove)
            {
                row.FieldByDataSource.Remove(key);
                hasChanges = true;

            }

            // Mark empty rows for removal
            if (!row.HasAnyFields())
            {
                rowsToRemove.Add(row);
            }
        }

        // Remove empty rows
        if (rowsToRemove.Any())
        {
            foreach (var row in rowsToRemove)
            {
                mappedFieldsRow.MappedFields.Remove(row);
            }


            hasChanges = true;
        }

        // Save if there were changes
        if (hasChanges)
        {
            await _mappedFieldRowsRepository.UpdateAsync(
                mappedFieldsRow,
                Constants.Collections.MappedFieldRows);

        }
    }

    /// <summary>
    /// Update IsActive status for fields in MappedFieldRows without removing them
    /// Used when field is removed from cleansing but still used in MatchDefinition
    /// </summary>
    public async Task UpdateFieldsInactiveStatusInMappedRowsAsync(
        Guid projectId,
        Guid dataSourceId,
        List<string> fieldNames,
        bool isActive)
    {
        if (!fieldNames.Any()) return;

        var savedMapping = await _mappedFieldRowsRepository
            .QueryAsync(x => x.ProjectId == projectId, Constants.Collections.MappedFieldRows);

        var mappedFieldsRow = savedMapping.FirstOrDefault();
        if (mappedFieldsRow == null || !mappedFieldsRow.MappedFields.Any()) return;

        var fieldNamesSet = new HashSet<string>(fieldNames, StringComparer.OrdinalIgnoreCase);
        bool hasChanges = false;

        foreach (var row in mappedFieldsRow.MappedFields)
        {
            foreach (var kvp in row.FieldByDataSource)
            {
                if (kvp.Value.DataSourceId == dataSourceId &&
                    fieldNamesSet.Contains(kvp.Value.FieldName))
                {
                    // UPDATE IsActive but DON'T remove
                    kvp.Value.IsActive = isActive;
                    hasChanges = true;                  
                }
            }
        }

        if (hasChanges)
        {
            await _mappedFieldRowsRepository.UpdateAsync(
                mappedFieldsRow,
                Constants.Collections.MappedFieldRows);
        }
    }
}
