
using MatchLogic.Domain.Project;
using System.Collections.Generic;

namespace MatchLogic.Application.Features.Import;
public class DataImportOptions
{
    public int PreviewLimit { get; set; } = 100;
    public Dictionary<string, ColumnMapping> ColumnMappings { get; set; } = new();
}
