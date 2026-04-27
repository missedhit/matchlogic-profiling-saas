using MatchLogic.Domain.DataProfiling;
using System;

namespace MatchLogic.Api.Handlers.DataProfile.DataPreview;

public class DataPreviewRequest : IRequest<Result<DataPreviewResponse>>
{
    public Guid DocumentId { get; set; }
    public Guid DataSourceId { get; set; }
}
