using MatchLogic.Domain.Import;
using System;

namespace MatchLogic.Api.Handlers.File.Upload;

public record FileUploadResponse(Guid Id,
    Guid ProjectId,
    string DataSourceType,
    string FileName,
    string OriginalName,
    string FilePath,
    long FileSize,
    string FileExtension,
    DateTime CreatedDate)
{
    // M2: S3 object key. Set for files uploaded via the presigned-PUT flow;
    // empty for legacy local-disk uploads.
    public string S3Key { get; init; } = string.Empty;
}


