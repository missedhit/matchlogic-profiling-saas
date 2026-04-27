using MatchLogic.Domain.FinalExport;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.FinalExport.ExecutePreview;

public record ExecutePreviewResponse(
    bool Success,
    List<IDictionary<string, object>> Data,      // First page of data
    int TotalRecords,
    int PageNumber,
    int PageSize,
    int GroupsProcessed,
    int TotalGroupsAvailable,
    bool IsLimited,
    FinalExportStatistics Statistics
);