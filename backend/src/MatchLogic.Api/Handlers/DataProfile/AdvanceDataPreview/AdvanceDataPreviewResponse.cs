using MatchLogic.Domain.DataProfiling;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataProfile.AdvanceDataPreview;

public class AdvanceDataPreviewResponse
{
    public List<RowReference> rowReferences { get; set; } = new();
}
