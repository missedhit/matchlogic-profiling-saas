using System;

namespace MatchLogic.Api.Handlers.FinalExport.GetPreview;

public class GetFinalExportPreviewQuery : IRequest<Result<GetFinalExportPreviewResponse>>
{
    public Guid ProjectId { get; set; }
    public bool IsPreview { get; set; } = true;
    public int PageNumber { get; set; } = 1;
    public int PageSize { get; set; } = 100;
    public string? FilterText { get; set; }
    public string? SortColumn { get; set; }
    public bool Ascending { get; set; } = true;
    public string? Filters { get; set; }
}