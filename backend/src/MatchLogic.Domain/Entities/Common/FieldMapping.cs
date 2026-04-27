using System;

namespace MatchLogic.Domain.Entities.Common;

public class FieldMapping : IEntity
{
    public Guid DataSourceId { get; set; }
    public string DataSourceName { get; set; }
    public string FieldName { get; set; }
    public string? DataType { get; set; }
    public int? Length { get; set; }
    public int? Ordinal { get; set; }

    public FieldMapping() { }

    public FieldMapping(Guid dataSourceId, string dataSourceName, string fieldName)
    {
        DataSourceId = dataSourceId;
        DataSourceName = dataSourceName;
        FieldName = fieldName;
    }

    public FieldMapping(Guid dataSourceId, string dataSourceName, string fieldName, string? dataType, int? length = null)
    {
        DataSourceId = dataSourceId;
        DataSourceName = dataSourceName;
        FieldName = fieldName;
        DataType = dataType;
        Length = length;
    }
}

public class FieldMappingEx : FieldMapping
{
    public bool Mapped { get; set; }
    public int FieldIndex { get; set; }
    public FieldOrigin Origin { get; set; } = FieldOrigin.Import;
    public bool IsSystemManaged { get; set; }
    public bool IsActive { get; set; } = true;
    public DateTime? InactivatedAt { get; set; }
    public string InactivationReason { get; set; }
    public Guid? SourceReferenceId { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }
}

public enum FieldOrigin
{
    Import = 0,
    CleansingOperation = 1,
    DataRefresh = 2,
    ManualAddition = 3,
    Transformation = 4
}
