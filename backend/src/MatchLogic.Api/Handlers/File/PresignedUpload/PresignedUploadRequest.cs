using Ardalis.Result;
using System;

namespace MatchLogic.Api.Handlers.File.PresignedUpload;

public record PresignedUploadRequest : IRequest<Result<PresignedUploadResponse>>
{
    public Guid ProjectId { get; set; }
    public string SourceType { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
}
