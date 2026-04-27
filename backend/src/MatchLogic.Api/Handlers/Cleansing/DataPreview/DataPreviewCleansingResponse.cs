using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.DataPreview;

public record DataPreviewCleansingResponse(IEnumerable<IDictionary<string, object>> Data, int TotalCount);
