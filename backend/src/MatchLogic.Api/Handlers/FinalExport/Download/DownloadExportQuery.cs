using System;
using System.IO;

namespace MatchLogic.Api.Handlers.FinalExport.Download;

/// <summary>
/// Query to download an exported file by run ID.
/// </summary>
public class DownloadExportQuery : IRequest<Result<FileDownloadResult>>
{
    public Guid RunId { get; set; }
}

public sealed class FileDownloadResult
{
    public Stream Stream { get; }
    public string ContentType { get; }
    public string FileName { get; }

    public FileDownloadResult(Stream stream, string contentType, string fileName)
    {
        Stream = stream;
        ContentType = contentType;
        FileName = fileName;
    }
}