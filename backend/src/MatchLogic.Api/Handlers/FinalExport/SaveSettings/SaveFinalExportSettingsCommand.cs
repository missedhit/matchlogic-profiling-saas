using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.FinalExport.SaveSettings;

public class SaveFinalExportSettingsCommand : IRequest<Result<SaveFinalExportSettingsResponse>>
{
    public Guid ProjectId { get; set; }
    public ExportAction ExportAction { get; set; }

    public BaseConnectionInfo? ConnectionInfo { get; set; }
    public SelectedAction SelectedAction { get; set; }
    public Dictionary<Guid, bool> DataSetsToInclude { get; set; } = new();
    public bool IncludeScoreFields { get; set; } = true;
    public bool IncludeSystemFields { get; set; } = true;
}