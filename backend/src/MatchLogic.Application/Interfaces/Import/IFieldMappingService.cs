using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Import;
public interface IFieldMappingService
{
    Task SyncSystemGeneratedFieldsAsync(
        Guid dataSourceId,
        string dataSourceName,
        List<FieldColumnInfo> currentColumns,
        FieldOrigin origin,
        Guid projectId,
        Guid? sourceOperationId = null);

    Task<List<FieldMappingEx>> GetActiveFieldMappingsAsync(
        Guid dataSourceId,
        bool includeSystemGenerated = true);

    Task<(List<FieldMappingEx> UsedInMatch, List<FieldMappingEx> SafeToRemove)>
        ClassifyFieldsByUsageAsync(
            List<FieldMappingEx> fields,
            Guid projectId);
}

public class FieldColumnInfo
{
    public string Name { get; set; }
    public string DataType { get; set; }
    public int? Index { get; set; }
    public bool IsNewColumn { get; set; }
    public string SourceOperation { get; set; }
}