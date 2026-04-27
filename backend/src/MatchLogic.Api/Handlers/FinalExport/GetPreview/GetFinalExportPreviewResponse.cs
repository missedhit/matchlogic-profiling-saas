using MatchLogic.Domain.FinalExport;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.FinalExport.GetPreview;

public record GetFinalExportPreviewResponse(
    List<IDictionary<string, object>> Data,
    int TotalRecords,
    int PageNumber,
    int PageSize,
    DateTime? LastExportedAt,
    ExportAction? LastExportAction,
    SelectedAction? LastSelectedAction,
    FinalExportStatistics? Statistics
);