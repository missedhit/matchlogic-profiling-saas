using System;

namespace MatchLogic.Api.Handlers.File.PresignedUpload;

public record PresignedUploadResponse(
    Guid FileId,
    Guid ProjectId,
    string DataSourceType,
    string OriginalName,
    string FileExtension,
    string S3Key,
    string PresignedUrl,
    DateTime ExpiresAt);
