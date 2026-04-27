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
    DateTime CreatedDate);


