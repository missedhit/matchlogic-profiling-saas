using System;

namespace MatchLogic.Api.Handlers.DataSource.Data;

public record PreviewDataSourceRequest : IRequest<Result<PreviewDataSourceResponse>>
{
    public Guid Id { get; set; }
    public int PageNumber { get; set; } = 1;
    public int PageSize { get; set; } = 10;
    public string? FilterText { get; set; }
    public string? SortColumn { get; set; }
    public bool Ascending { get; set; } = true;
}