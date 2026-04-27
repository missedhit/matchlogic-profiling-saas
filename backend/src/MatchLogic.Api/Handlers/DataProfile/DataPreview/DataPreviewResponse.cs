using MatchLogic.Domain.DataProfiling;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataProfile.DataPreview;

public class DataPreviewResponse
{
    public List<RowReference> rowReferences { get; set; } = new();
}
