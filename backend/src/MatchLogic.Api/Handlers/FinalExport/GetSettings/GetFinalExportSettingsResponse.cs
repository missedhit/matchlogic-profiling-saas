using MatchLogic.Domain.FinalExport;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.FinalExport.GetSettings;

public record GetFinalExportSettingsResponse(
    FinalExportSettings Settings,
    List<DataSourceInfo> AvailableDataSources
);

public class DataSourceInfo
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public long RecordCount { get; set; }
}