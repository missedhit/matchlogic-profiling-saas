using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Refresh;

public class RefreshDictionaryCommand : IRequest<Result<WordSmithDictionaryDto>>
{
    public Guid? DictionaryId { get; set; }
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }
    public string ColumnName { get; set; }
    public string Separators { get; set; } = " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`";
    public int MaxWordCount { get; set; } = 3;
    public bool IncludeFullText { get; set; } = false;
    public bool IgnoreCase { get; set; } = true;
}
