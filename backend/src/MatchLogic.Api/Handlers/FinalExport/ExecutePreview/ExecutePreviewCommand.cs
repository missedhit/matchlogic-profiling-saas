using MatchLogic.Domain.FinalExport;
using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.ExecutePreview;

public class ExecutePreviewCommand : IRequest<Result<ExecutePreviewResponse>>
{
    public Guid ProjectId { get; set; }
    public ExportAction? ExportAction { get; set; }
    public SelectedAction? SelectedAction { get; set; }
    public Dictionary<Guid, bool>? DataSetsToInclude { get; set; }
    public bool? IncludeScoreFields { get; set; }
    public bool? IncludeSystemFields { get; set; }
    public int MaxGroups { get; set; } = 20;
    public int PageSize { get; set; } = 100;  // First page size
}