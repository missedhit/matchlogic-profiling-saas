using System.Collections.Generic;
using System;

namespace MatchLogic.Domain.FinalExport;

public class ExportValidationResult
{
    public bool IsValid { get; set; }
    public bool HasResults { get; set; }
    public bool ResultsInSync { get; set; }
    public bool HasPreview { get; set; }
    public bool HasSavedSettings { get; set; }
    public List<string> ValidationErrors { get; set; } = new();
    public List<MissingFieldInfo> MissingFields { get; set; } = new();
}

public class MissingFieldInfo
{
    public Guid DataSourceId { get; set; }
    public string DataSourceName { get; set; } = string.Empty;
    public string FieldName { get; set; } = string.Empty;
}