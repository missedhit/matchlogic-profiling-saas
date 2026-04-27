using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.MatchConfiguration;

/// <summary>
/// Interface for auto-mapping service.
/// </summary>
public interface IAutoMappingService
{
    /// <summary>
    /// Gets extended field information for a project.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <returns>Dictionary mapping data source names to their field lists.</returns>
    Task<Dictionary<string, List<FieldMappingEx>>> GetExtendedFieldsAsync(Guid projectId, bool activeOnly = true);

    /// <summary>
    /// Insert FieldMappingEx for each datasource in project
    /// </summary>
    /// <param name="projectId"></param>
    /// <returns></returns>
    Task InsertExtendedFieldsByProjectAsync(Guid projectId);

    /// <summary>
    /// Insert FieldMappingEx for each datasource in project
    /// </summary>
    /// <param name="projectId"></param>
    /// <returns></returns>
    Task DeleteExtendedFieldsByProjectAsync(Guid projectId);
    /// <summary>
    /// Performs auto-mapping of fields using similarity-based algorithm.
    /// </summary>
    /// <param name="fieldsPerDataSource">Fields grouped by data source.</param>
    /// <returns>List of mapped field rows.</returns>
    List<MappedFieldRow> AutoMapFields(Dictionary<string, List<FieldMappingEx>> fieldsPerDataSource);

    /// <summary>
    /// Performs sequential mapping of fields based on field order/position.
    /// </summary>
    /// <param name="fieldsPerDataSource">Fields grouped by data source.</param>
    /// <returns>List of mapped field rows.</returns>
    List<MappedFieldRow> SequentialMapFields(Dictionary<string, List<FieldMappingEx>> fieldsPerDataSource);

    /// <summary>
    /// Performs auto-mapping of fields for a project using similarity-based algorithm.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <returns>List of mapped field rows.</returns>
    Task<List<MappedFieldRow>> PerformAutoMappingAsync(Guid projectId);

    /// <summary>
    /// Performs sequential mapping of fields for a project based on field order.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <returns>List of mapped field rows.</returns>
    Task<List<MappedFieldRow>> PerformSequentialMappingAsync(Guid projectId);

    /// <summary>
    /// Gets the last saved mapped field rows for a project.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <returns>List of saved mapped field rows.</returns>
    Task<List<MappedFieldRow>> GetSavedMappedFieldRowsAsync(Guid projectId);

    /// <summary>
    /// Gets the last saved exportable mapped field rows for a project.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <returns>List of saved mapped field rows.</returns>
    Task<List<FieldMappingEx>> GetSavedExportableMappedFieldRowsAsync(Guid projectId);
    /// <summary>
    /// Deletes mapped field rows for a project.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <returns>Task representing the asynchronous operation.</returns>
    Task DeleteMappedFieldRowsAsync(Guid projectId);

    /// <summary>
    /// Updates existing mapped field rows for a project.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <param name="mappedRows">The new mapped field rows.</param>
    /// <returns>Task representing the asynchronous operation.</returns>
    Task UpdateMappedFieldRowsAsync(Guid projectId, List<MappedFieldRow> mappedRows);

    /// <summary>
    /// Clears all mapped fields and creates a basic mapping structure using only the first data source.
    /// </summary>
    /// <param name="fieldsPerDataSource">Fields grouped by data source.</param>
    /// <returns>List of cleared mapped field rows.</returns>
    List<MappedFieldRow> ClearMapFields(Dictionary<string, List<FieldMappingEx>> fieldsPerDataSource);

    /// <summary>
    /// Clears mapped fields for a project and creates a basic mapping structure using the first data source.
    /// </summary>
    /// <param name="projectId">The project ID.</param>
    /// <returns>List of cleared mapped field rows.</returns>
    Task<List<MappedFieldRow>> ClearMapFieldsAsync(Guid projectId);

    /// <summary>
    /// Add system-generated fields to MappedFieldRows (Cleansing, DataRefresh, etc.)
    /// </summary>
    Task AddSystemGeneratedFieldsToMappedRowsAsync(
        Guid projectId,
        Guid dataSourceId,
        List<FieldMappingEx> newFields);

    /// <summary>
    /// Remove specific fields from MappedFieldRows by field name
    /// This is called BEFORE FieldMapping deletion to clean up UI state
    /// </summary>
    Task RemoveFieldsFromMappedRowsAsync(
        Guid projectId,
        Guid dataSourceId,
        List<string> fieldNames);

    /// <summary>
    /// Update IsActive status for fields in MappedFieldRows without removing them
    /// Used when field is removed from cleansing but still used in MatchDefinition
    /// </summary>
    Task UpdateFieldsInactiveStatusInMappedRowsAsync(
    Guid projectId,
    Guid dataSourceId,
    List<string> fieldNames,
    bool isActive);
}
