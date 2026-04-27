using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.CleansingAndStandardization.DTOs;

/// <summary>
/// Data transfer object for standard cleaning rules
/// </summary>
public class CleaningRuleDto
{
    public Guid? Id { get; set; }
    public string ColumnName { get; set; }
    public CleaningRuleType RuleType { get; set; }
    public Dictionary<string, string> Arguments { get; set; } = new();
}

/// <summary>
/// Data transfer object for extended cleaning rules
/// </summary>
public class ExtendedCleaningRuleDto : CleaningRuleDto
{
    public OperationType OperationType { get; set; } = OperationType.Standard;
    public List<DataCleansingColumnMappingDto> ColumnMappings { get; set; } = new();
    public List<Guid> DependsOnRules { get; set; } = new();
    public int ExecutionOrder { get; set; } = 0;
}
/// <summary>
/// Data transfer object for column mapping
/// </summary>
public class DataCleansingColumnMappingDto
{
    public string SourceColumn { get; set; }
    public string TargetColumn { get; set; }
    public List<string> OutputColumns { get; set; } = new();
}

/// <summary>
/// Data transfer object for mapping rules
/// </summary>
public class MappingRuleDto
{
    public Guid? Id { get; set; }
    public MappingOperationType OperationType { get; set; }
    public List<string> SourceColumn { get; set; }
    public Dictionary<string, string> MappingConfig { get; set; } = new();
    public List<string> OutputColumns { get; set; } = new();
}

public class WordSmithDictionaryDto
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public string Category { get; set; }
    public string OriginalFileName { get; set; }
    public int Version { get; set; }
    public int TotalRules { get; set; }
    public int ReplacementRules { get; set; }
    public int DeletionRules { get; set; }
    public int NewColumnRules { get; set; }
    public List<string> ExtractedColumns { get; set; }
    public bool IsActive { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? ModifiedAt { get; set; }
}

public class WordSmithRuleDto
{
    public Guid Id { get; set; }
    public string Words { get; set; }
    public string Replacement { get; set; }
    public string NewColumnName { get; set; }
    public bool ToDelete { get; set; }
    public int Priority { get; set; }
    public bool IsActive { get; set; }
    public DateTime? ModifiedAt { get; set; }
}

public class CreateWordSmithRuleDto
{
    public string Words { get; set; }
    public string Replacement { get; set; }
    public string NewColumnName { get; set; }
    public bool ToDelete { get; set; }
    public int Priority { get; set; } = 5;
}

public class UpdateWordSmithRuleDto
{
    public string Replacement { get; set; }
    public string NewColumnName { get; set; }
    public bool? ToDelete { get; set; }
    public int? Priority { get; set; }
    public bool? IsActive { get; set; }
}

public class UploadWordSmithDictionaryDto
{
    public string Name { get; set; }
    public string Description { get; set; }
    public string Category { get; set; } = "Custom";

    public bool AddFlagColumn { get; set; }
    public string WordDelimiters { get; set; }

    public string MaxWords { get; set; }
    public string Encoding { get; set; }
}

public class WordSmithDictionaryResponse
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public string Category { get; set; }
    public string OriginalFileName { get; set; }

    public bool AddFlagColumn { get; set; }
    public string WordDelimiters { get; set; }

    public string MaxWords { get; set; }
    public int Version { get; set; }
    public int TotalRuleCount { get; set; }
    public int ReplacementRuleCount { get; set; }
    public int DeletionRuleCount { get; set; }
    public int NewColumnRuleCount { get; set; }
    public List<string> ExtractedColumns { get; set; }
    public DateTime CreatedAt { get; set; }
    public List<WordSmithRuleDto> PreviewRules { get; set; }
}

public class ReplacingStep
{
    public String Words = String.Empty;
    public String Replacement = String.Empty;
    public String NewColumnName = String.Empty;
    public Int32 Priority = 0;
    public Boolean ToDelete = false;

    public ReplacingStep Clone()
    {
        return (ReplacingStep)MemberwiseClone();
    }
}

public class SchemaInfo
{
    public List<CleansingColumnInfo> OutputColumns { get; set; }
    public Dictionary<string, ColumnFlowInfo> ColumnFlow { get; set; }
    public int TotalRules { get; set; }
}

public class CleansingColumnInfo
{
    public string Name { get; set; }
    public string ProducedBy { get; set; }
    public Guid RuleId { get; set; }
    public bool IsNewColumn { get; set; }
}

public class ColumnFlowInfo
{
    public string ColumnName { get; set; }
    public List<string> ProducedBy { get; set; }
    public List<string> ConsumedBy { get; set; }
}

public class WordExtractionPreviewDto
{
    /// <summary>
    /// Total unique words found
    /// </summary>
    public int TotalUniqueWords { get; set; }

    /// <summary>
    /// Total records scanned
    /// </summary>
    public int RecordsScanned { get; set; }

    /// <summary>
    /// Sample of extracted words with frequencies
    /// </summary>
    public List<WordFrequencyDto> TopWords { get; set; } = new();

    /// <summary>
    /// Sample source values
    /// </summary>
    public List<string> SampleValues { get; set; } = new();

    /// <summary>
    /// Estimated total words if full refresh is run
    /// </summary>
    public int EstimatedTotalWords { get; set; }
}

public class WordFrequencyDto
{
    public string Word { get; set; }
    public int Count { get; set; }
    public int CharacterLength { get; set; }
    public bool IsExistingRule { get; set; }
}

public class RefreshDictionaryRequest
{
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }
    public string ColumnName { get; set; }
    public string Separators { get; set; } = " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`";
    public int MaxWordCount { get; set; } = 3;
    public bool IncludeFullText { get; set; } = false;
    public bool IgnoreCase { get; set; } = true;
}

public class CreateDictionaryFromColumnRequest
{
    public string DictionaryName { get; set; }
    public string Description { get; set; }
    public string Category { get; set; } = "User";
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }
    public string ColumnName { get; set; }
    public string Separators { get; set; } = " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`";
    public int MaxWordCount { get; set; } = 3;
    public bool IncludeFullText { get; set; } = false;
    public bool IgnoreCase { get; set; } = true;
}
/// <summary>
/// Request DTO for previewing word extraction
/// </summary>
public class PreviewExtractionRequest : RefreshDictionaryRequest
{
    public int MaxRecords { get; set; } = 100;
}

public class ValidationResult
{
    public bool IsValid { get; set; }
    public List<string> Errors { get; set; } = new List<string>();
    public List<string> Warnings { get; set; } = new List<string>();
}