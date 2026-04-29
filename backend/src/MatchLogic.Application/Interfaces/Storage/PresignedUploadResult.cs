using System;

namespace MatchLogic.Application.Interfaces.Storage;

public record PresignedUploadResult(
    string S3Key,
    string Url,
    DateTime ExpiresAt);
