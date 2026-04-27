using System;

namespace MatchLogic.Api.Handlers.Cleansing.DataPreview;

public record DataPreviewCleansingRequest : IRequest<Result<DataPreviewCleansingResponse>>
{
    public Guid DataSourceId { get; set; }
    public int PageNumber { get; set; } = 1;
    public int PageSize { get; set; } = 10;
    public string? FilterText { get; set; }
    public string? SortColumn { get; set; }
    public bool Ascending { get; set; } = true;
    public bool? IsPreview { get; set; } = false;
}
