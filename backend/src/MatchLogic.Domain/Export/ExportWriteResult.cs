using System.Collections.Generic;
using System;
using System.Linq;

namespace MatchLogic.Domain.Export;

public class ExportWriteResult
{
    public bool Success { get; set; }
    public long RowsWritten { get; set; }
    public string? FilePath { get; set; }
    public TimeSpan Duration { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();

    public static ExportWriteResult Succeeded(long rows, TimeSpan duration, string? path = null) => new()
    {
        Success = true,
        RowsWritten = rows,
        Duration = duration,
        FilePath = path
    };

    public static ExportWriteResult Failed(string error) => new()
    {
        Success = false,
        Errors = new List<string> { error }
    };

    public static ExportWriteResult Failed(IEnumerable<string> errors) => new()
    {
        Success = false,
        Errors = errors.ToList()
    };
}