using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.ColumnNotes;

public class DataSourceColumnNotesDto
{
    public Guid Id { get; set; }
    public Guid DataSourceId { get; set; }
    public Dictionary<string, string> ColumnNotes { get; set; } = new();
}

public class UpsertColumnNotesRequest
{
    public Dictionary<string, string> ColumnNotes { get; set; } = new();
}
