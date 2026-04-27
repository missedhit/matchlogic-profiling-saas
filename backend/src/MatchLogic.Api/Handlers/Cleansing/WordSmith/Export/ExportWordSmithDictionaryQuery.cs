using OpenTelemetry;
using System;
using System.IO;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Export;

public class ExportWordSmithDictionaryQuery : IRequest<Result<ExportResult>>
{
    public Guid Id { get; set; }
    public string Format { get; set; } = "tsv";
}

public class ExportResult
{
    public Stream FileStream { get; set; }
    public string FileName { get; set; }
    public string ContentType { get; set; }
}