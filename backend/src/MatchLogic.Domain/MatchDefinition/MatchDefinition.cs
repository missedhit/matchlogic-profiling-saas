using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace MatchLogic.Domain.Entities;

/// <summary>
/// Collection of match definitions
/// </summary>
public class MatchDefinitionCollection : AuditableEntity
{    
    public Guid ProjectId { get; set; }
    public Guid JobId { get; set; }
    public string Name { get; set; }

    // List of match definitions (one per DataSourcePair)
    public List<MatchDefinition> Definitions { get; set; } = new();

    public MatchDefinitionCollection() { }

    public MatchDefinitionCollection(Guid projectId, Guid jobId, string name)
    {
        ProjectId = projectId;
        JobId = jobId;
        Name = name;

    }
}
/// <summary>
/// Match definition for a specific DataSourcePair
/// </summary>
public class MatchDefinition : IEntity
{    
    public Guid DataSourcePairId { get; set; }
    public int UIDefinitionIndex { get; set; }
    #region Need to remove this 
    public Guid ProjectRunId { get; set; }
    public bool MergeOverlappingGroups { get; set; } = false;
    public bool IsProbabilistic { get; set; } = false;
    public Guid ProjectId { get; set; }
    public Guid JobId { get; set; }
    public string Name { get; set; }
    #endregion
    // Criteria for this DataSourcePair
    public List<MatchCriteria> Criteria { get; set; } = new();

    public MatchDefinition() { }

    public MatchDefinition(Guid dataSourcePairId, Guid projectRunId)
    {
        DataSourcePairId = dataSourcePairId;
        ProjectRunId = projectRunId;
    }
}

/// <summary>
/// Match criterion with fields for a specific DataSourcePair
/// </summary>
public class MatchCriteria : IEntity
{
    // Need to remove this
    public string FieldName { get; set; }    
    public MatchingType MatchingType { get; set; }
    public CriteriaDataType DataType { get; set; }
    public double Weight { get; set; }

    // Arguments for this criterion
    public Dictionary<ArgsValue, string> Arguments { get; set; } = new();

    // Field mappings for this criterion
    public List<FieldMapping> FieldMappings { get; set; } = new();

    public MatchCriteria() { }

    public MatchCriteria(MatchingType matchingType, CriteriaDataType dataType, double weight)
    {
        MatchingType = matchingType;
        DataType = dataType;
        Weight = weight;
    }
}
public enum MatchingType : byte
{
    Exact,
    Fuzzy,
}

public enum CriteriaDataType : byte
{
    Text,
    Number,
    Phonetic
}

public enum ArgsValue : byte
{
    // Fuzzy
    FastLevel,
    Level,
    //Numeric
    LowerLimit,
    UpperLimit,
    // Phonetic
    PhoneticRating,
    // Percentage Numeric
    UpperPercentage,
    LowerPercentage,
    UsePercentage,
}

// Current criteria has only one field but for cross fields each field could be from different datasource
// So the Field name would be in separate field along with the datasource
// Match Definition
// So the field should have the name datasource id/name , there should be identifier that it is already added so that
// we can not add it again

/// <summary>
/// Field mapping for a criterion
/// </summary>
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

// This calls will manange every thing adding field to matchcriteria and add matchdefinition
/// <summary>
/// Settings for match configurations
/// </summary>
public class MatchSettings : AuditableEntity
{
    public Guid ProjectId { get; set; }
    // Group settings
    public bool MergeOverlappingGroups { get; set; } = false;
    public int MaxMatchesPerResultGroup { get; set; } = 0;

    // Match execution settings
    public bool IsProbabilistic { get; set; } = false;
    public int MaxMatchesPerRecord { get; set; } = 500;

    // Output settings
    public bool AutogenerateReport { get; set; } = false;

    // Advanced options
    public bool AdvancedOptions { get; set; } = false;
}

/// <summary>
/// Extended FieldInfo class with mapping properties
/// </summary>
public class FieldMappingEx : FieldMapping
{
    /// <summary>
    /// Gets or sets a value indicating whether this field is mapped.
    /// </summary>
    public bool Mapped { get; set; }

    /// <summary>
    /// Gets or sets the index of the field within its data source.
    /// </summary>
    public int FieldIndex { get; set; }

    public FieldOrigin Origin { get; set; } = FieldOrigin.Import;
    public bool IsSystemManaged { get; set; }
    public bool IsActive { get; set; } = true;
    public DateTime? InactivatedAt { get; set; }
    public string InactivationReason { get; set; }
    public Guid? SourceReferenceId { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }

    /// <summary>
    /// Creates a new FieldMappingEx from a base FieldInfo.
    /// </summary>
    public static FieldMappingEx FromFieldInfo(FieldMapping fieldInfo, string dataSourceName, int fieldIndex)
    {
        return new FieldMappingEx
        {
            FieldName = fieldInfo.FieldName,
            DataSourceName = dataSourceName,
            DataSourceId = fieldInfo.DataSourceId,
            FieldIndex = fieldIndex,
            Mapped = false
        };
    }

    public static FieldMappingEx FromFieldInfo(FieldMappingEx field, string dataSourceName, int fieldIndex)
    {
        return new FieldMappingEx
        {
            Id = field.Id,
            DataSourceId = field.DataSourceId,
            DataSourceName = dataSourceName,
            FieldName = field.FieldName,
            DataType = field.DataType,
            FieldIndex = fieldIndex,
            Mapped = field.Mapped,
            Origin = field.Origin,
            IsSystemManaged = field.IsSystemManaged,
            IsActive = field.IsActive,
            SourceReferenceId = field.SourceReferenceId,
            CreatedAt = field.CreatedAt
        };
    }

    /// <summary>
    /// Converts to a FieldDto for database storage.
    /// </summary>
    public FieldMapping ToFieldDto(Guid dataSourceId)
    {
        return new FieldMapping
        {
            FieldName = this.FieldName,
            DataSourceId = dataSourceId,
            DataSourceName = this.DataSourceName
        };
    }
}

public enum FieldOrigin
{
    Import = 0,
    CleansingOperation = 1,
    DataRefresh = 2,
    ManualAddition = 3,
    Transformation = 4
}
public class MappedFieldRow
{
    private Dictionary<string, FieldMappingEx> _fieldByDataSource = new Dictionary<string, FieldMappingEx>();

    public Dictionary<string, FieldMappingEx> FieldByDataSource
    {
        get => _fieldByDataSource;
        set => _fieldByDataSource = value ?? new Dictionary<string, FieldMappingEx>();
    }

    public bool Include { get; set; }
    public void AddField(FieldMappingEx field)
    {
        _fieldByDataSource[field.DataSourceName.ToLower()] = field;
    }

    public void RemoveField(FieldMappingEx field)
    {
        if (_fieldByDataSource.ContainsKey(field.DataSourceName.ToLower()))
        {
            _fieldByDataSource.Remove(field.DataSourceName.ToLower());
        }
    }


    public FieldMappingEx this[string dataSourceName]
    {
        get
        {
            _fieldByDataSource.TryGetValue(dataSourceName.ToLower(), out FieldMappingEx fieldMapInfo);
            return fieldMapInfo;
        }
        set
        {
            _fieldByDataSource[dataSourceName.ToLower()] = value;
        }
    }

    /// <summary>
    /// Returns all fields in this row.
    /// </summary>
    public IEnumerable<FieldMappingEx> GetAllFields()
    {
        return _fieldByDataSource.Values;
    }

    /// <summary>
    /// Checks if the row contains any fields.
    /// </summary>
    public bool HasAnyFields()
    {
        return _fieldByDataSource.Count > 0;
    }
}

public class MappedFieldsRow : IEntity
{
    public List<MappedFieldRow> MappedFields { get; set; }
    public Guid ProjectId { get; set; }
}