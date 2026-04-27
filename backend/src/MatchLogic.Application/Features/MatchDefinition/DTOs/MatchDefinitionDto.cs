
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchDefinition.DTOs;

#region Common DTOs

public class MatchDefinitionDto
{
    public Guid Id { get; set; }
    public Guid ProjectId { get; set; }
    public Guid ProjectRunId { get; set; }
    public Guid JobId { get; set; }
    public string Name { get; set; }
    public bool MergeOverlappingGroups { get; set; }
    public bool IsProbabilistic { get; set; }
}

public class MatchSettingsDto
{
    // Group settings
    public bool MergeOverlappingGroups { get; set; } = false;
    public bool SimilarRecordsInGroups { get; set; } = false;
    public int MaxMatchesPerResultGroup { get; set; } = 0;

    // Match execution settings
    public bool IsProbabilistic { get; set; } = false;
    public int MaxMatchesPerRecord { get; set; } = 500;

    // Output settings
    public bool AutogenerateReport { get; set; } = false;

    // Advanced options
    public bool AdvancedOptions { get; set; } = false;
}

public class FieldDto
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public Guid DataSourceId { get; set; }
    public string DataSourceName { get; set; }
    public string DisplayName { get { return $"{DataSourceName}_{Name}"; } }

    public string Origin { get; set; }          // "Import", "CleansingOperation", "DataRefresh"
    public bool IsActive { get; set; }          // true/false
    public bool IsSystemManaged { get; set; }
}

public class DataSourcePairDto
{
    public Guid Id { get; set; }
    public Guid DataSourceAId { get; set; }
    public Guid DataSourceBId { get; set; }
}

#endregion

#region Field List (DataSourcePair) DTOs
public class MatchDefinitionCollectionFieldListDto
{
    public Guid Id { get; set; }
    public Guid ProjectId { get; set; }
    public Guid JobId { get; set; }
    public string Name { get; set; }
    public List<MatchDefinitionFieldListDto> Definitions { get; set; }
}

public class MatchDefinitionFieldListDto
{
    public Guid Id { get; set; }
    public Guid DataSourcePairId { get; set; }
    public Guid ProjectRunId { get; set; }
    public string DataSourcePairName { get; set; } // For display
    public List<MatchCriterionFieldListDto> Criteria { get; set; }
}

public class MatchCriterionFieldListDto
{
    public Guid Id { get; set; }
    public MatchingType MatchingType { get; set; }
    public CriteriaDataType DataType { get; set; }
    public double Weight { get; set; }
    public Dictionary<ArgsValue, string> Arguments { get; set; }
    public List<FieldDto> Fields { get; set; }
}
#endregion

#region MappedRow DTOs
public class MatchDefinitionCollectionMappedRowDto
{
    public Guid Id { get; set; }
    public Guid ProjectId { get; set; }
    public Guid JobId { get; set; }
    public string Name { get; set; }

    public List<MatchDefinitionMappedRowDto> Definitions { get; set; }
}

public class MatchDefinitionMappedRowDto
{
    public Guid Id { get; set; }
    public Guid ProjectRunId { get; set; }
    public List<MatchCriterionMappedRowDto> Criteria { get; set; }
}

public class MatchCriterionMappedRowDto
{
    public Guid Id { get; set; }
    public MatchingType MatchingType { get; set; }
    public CriteriaDataType DataType { get; set; }
    public double Weight { get; set; }
    public Dictionary<ArgsValue, string> Arguments { get; set; }
    public MappedFieldRowDto MappedRow { get; set; }
}

public class MappedFieldRowDto
{
    public bool Include { get; set; } = true;
    public Dictionary<string, FieldDto> FieldsByDataSource { get; set; }
}
#endregion
