using MatchLogic.Domain.DataProfiling;
using System;

namespace MatchLogic.Api.Handlers.DataProfile.AdvanceDataPreview;

public class AdvanceDataPreviewRequest : IRequest<Result<AdvanceDataPreviewResponse>>
{
    public Guid DocumentId { get; set; }
    public Guid DataSourceId { get; set; }
}
