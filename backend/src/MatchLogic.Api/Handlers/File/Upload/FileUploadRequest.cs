using Ardalis.Result;
using MatchLogic.Domain.Import;
using Microsoft.AspNetCore.Http;
using System;

namespace MatchLogic.Api.Handlers.File.Upload;

public record FileUploadRequest : IRequest<Result<FileUploadResponse>>
{
    public IFormFile File { get; set; }
    public Guid ProjectId { get; set; }
    public string SourceType { get; set; }
}
