using MatchLogic.Domain.Entities.Common;
using System.Collections.Generic;
using System;
using MatchLogic.Domain.Project;

namespace MatchLogic.Domain.FinalExport;

public class FinalExportSettings : IEntity
{    
    public Guid ProjectId { get; set; }

    public ExportAction ExportAction { get; set; } = ExportAction.AllRecordsAndFlagDuplicates;
    public SelectedAction SelectedAction { get; set; } = SelectedAction.ShowAll;
    public Dictionary<Guid, bool> DataSetsToInclude { get; set; } = new();
    public bool IncludeScoreFields { get; set; } = true;
    public bool IncludeSystemFields { get; set; } = true;
    public BaseConnectionInfo ConnectionInfo { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}

public enum ExportAction : byte
{
    AllRecordsAndFlagDuplicates = 0,
    SuppressAllDuplicateRecords = 1,
    NonDupsAndMasterRecordRemaining = 2,
    DuplicatesOnly = 3,
    CrossReference = 4
}

public enum SelectedAction : byte
{
    ShowAll = 0,
    SuppressSelected = 1,
    ShowSelectedOnly = 2
}

public enum ExportFormat : byte
{
    Csv = 0,
    Excel = 1,
    Json = 2
}