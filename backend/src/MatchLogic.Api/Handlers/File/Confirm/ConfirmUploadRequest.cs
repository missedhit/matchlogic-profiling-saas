using Ardalis.Result;
using MatchLogic.Api.Handlers.File.Upload;
using System;

namespace MatchLogic.Api.Handlers.File.Confirm;

public record ConfirmUploadRequest : IRequest<Result<FileUploadResponse>>
{
    public Guid FileId { get; set; }
    public Guid ProjectId { get; set; }
    public string SourceType { get; set; } = string.Empty;
    public string OriginalName { get; set; } = string.Empty;
    public string FileExtension { get; set; } = string.Empty;
    public string S3Key { get; set; } = string.Empty;
}
