using Ardalis.Result;
using MatchLogic.Application.Interfaces.Storage;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.File.PresignedUpload;

public class PresignedUploadHandler : IRequestHandler<PresignedUploadRequest, Result<PresignedUploadResponse>>
{
    private readonly IFileStorageService _storage;
    private readonly ILogger<PresignedUploadHandler> _logger;

    public PresignedUploadHandler(
        IFileStorageService storage,
        ILogger<PresignedUploadHandler> logger)
    {
        _storage = storage;
        _logger = logger;
    }

    public async Task<Result<PresignedUploadResponse>> Handle(
        PresignedUploadRequest request,
        CancellationToken cancellationToken)
    {
        var fileId = Guid.NewGuid();
        var extension = Path.GetExtension(request.FileName);

        var presigned = await _storage.CreatePresignedUploadAsync(fileId, extension, cancellationToken);

        _logger.LogInformation(
            "Minted presigned upload URL for project {ProjectId}, file {FileName}, fileId {FileId}",
            request.ProjectId, request.FileName, fileId);

        return Result<PresignedUploadResponse>.Success(new PresignedUploadResponse(
            FileId: fileId,
            ProjectId: request.ProjectId,
            DataSourceType: request.SourceType,
            OriginalName: request.FileName,
            FileExtension: extension,
            S3Key: presigned.S3Key,
            PresignedUrl: presigned.Url,
            ExpiresAt: presigned.ExpiresAt));
    }
}
