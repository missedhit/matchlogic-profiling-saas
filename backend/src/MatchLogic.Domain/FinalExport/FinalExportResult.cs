using MatchLogic.Domain.Entities.Common;
using System.Collections.Generic;
using System;

namespace MatchLogic.Domain.FinalExport;

public class FinalExportResult : IEntity
{
    public Guid ProjectId { get; set; }
    public Guid StepJobId { get; set; }
    public string CollectionName { get; set; } = string.Empty;
    public string? ExportFilePath { get; set; }

    public string SettingsHash { get; set; } = string.Empty;
    public ExportAction ExportAction { get; set; }
    public SelectedAction SelectedAction { get; set; }

    public bool IsPreview { get; set; }
    public FinalExportStatistics Statistics { get; set; } = new();
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

public class FinalExportStatistics
{
    public long TotalRecordsProcessed { get; set; }
    public long RecordsExported { get; set; }
    public long RecordsSkipped { get; set; }
    public int GroupsProcessed { get; set; }
    public int TotalGroupsAvailable { get; set; }
    public long UniqueRecordsExported { get; set; }
    public long DuplicateRecordsExported { get; set; }
    public long MasterRecordsExported { get; set; }
    public long SelectedRecordsExported { get; set; }
    public long CrossReferenceRecordsExported { get; set; }
    public Dictionary<Guid, long> RecordsByDataSource { get; set; } = new();
    public TimeSpan ProcessingTime { get; set; }
    public bool IsLimited { get; set; }
}

public class ExportPreviewResult
{
    public IEnumerable<IDictionary<string, object>> Data { get; set; } = new List<IDictionary<string, object>>();
    public int TotalCount { get; set; }
    public DateTime? LastExportedAt { get; set; }
    public ExportAction? LastExportAction { get; set; }
    public SelectedAction? LastSelectedAction { get; set; }
}