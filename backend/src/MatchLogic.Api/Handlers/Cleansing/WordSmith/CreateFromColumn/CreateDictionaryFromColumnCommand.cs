using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.CreateFromColumn;

public class CreateDictionaryFromColumnCommand : IRequest<Result<WordSmithDictionaryDto>>
{
    public string DictionaryName { get; set; }
    public string Description { get; set; }
    public string Category { get; set; }
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }
    public string ColumnName { get; set; }
    public string Separators { get; set; }
    public int MaxWordCount { get; set; }
    public bool IncludeFullText { get; set; }
    public bool IgnoreCase { get; set; }
}
