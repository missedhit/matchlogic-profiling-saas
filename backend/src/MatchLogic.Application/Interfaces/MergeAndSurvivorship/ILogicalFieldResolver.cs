using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MatchLogic.Application.Features.MergeAndSurvivorship;

namespace MatchLogic.Application.Interfaces.MergeAndSurvivorship;

/// <summary>
/// Service for resolving logical field names to physical field names
/// </summary>
public interface ILogicalFieldResolver
{
    /// <summary>
    /// Resolves logical field mappings from mapped field rows
    /// </summary>
    Task<List<LogicalFieldMapping>> ResolveLogicalFieldsAsync(
        List<MappedFieldRow> mappedFields,
        List<DataSource> dataSources);

    /// <summary>
    /// Gets the physical field name for a logical field in a specific data source
    /// </summary>
    string GetPhysicalFieldName(
        string logicalFieldName,
        Guid dataSourceId,
        List<LogicalFieldMapping> mappings);

    /// <summary>
    /// Gets the field value from record data using logical field name
    /// </summary>
    object GetFieldValue(
        IDictionary<string, object> recordData,
        string logicalFieldName,
        Guid dataSourceId,
        List<LogicalFieldMapping> mappings);
}
