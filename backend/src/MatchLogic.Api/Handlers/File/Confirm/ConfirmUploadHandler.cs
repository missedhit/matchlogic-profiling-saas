using Ardalis.Result;
using MatchLogic.Api.Handlers.File.Upload;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Storage;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.File.Confirm;

public class ConfirmUploadHandler : IRequestHandler<ConfirmUploadRequest, Result<FileUploadResponse>>
{
    private readonly IFileStorageService _storage;
    private readonly IFileImportService _fileImportService;
    private readonly ILogger<ConfirmUploadHandler> _logger;

    public ConfirmUploadHandler(
        IFileStorageService storage,
        IFileImportService fileImportService,
        ILogger<ConfirmUploadHandler> logger)
    {
        _storage = storage;
        _fileImportService = fileImportService;
        _logger = logger;
    }

    public async Task<Result<FileUploadResponse>> Handle(
        ConfirmUploadRequest request,
        CancellationToken cancellationToken)
    {
        if (!await _storage.ExistsAsync(request.S3Key, cancellationToken))
        {
            _logger.LogWarning(
                "Confirm called for fileId {FileId} but s3 object {S3Key} does not exist",
                request.FileId, request.S3Key);
            return Result<FileUploadResponse>.NotFound(
                $"Uploaded object not found at s3 key '{request.S3Key}'. The presigned URL may have expired before the PUT completed.");
        }

        var size = await _storage.GetSizeAsync(request.S3Key, cancellationToken);

        var fileImport = new FileImport
        {
            Id = request.FileId,
            ProjectId = request.ProjectId,
            DataSourceType = Enum.Parse<DataSourceType>(request.SourceType, ignoreCase: true),
            OriginalName = request.OriginalName,
            FileName = $"{request.FileId:D}{request.FileExtension}",
            FilePath = string.Empty,
            S3Key = request.S3Key,
            FileSize = size,
            FileExtension = request.FileExtension
        };

        await _fileImportService.CreateFile(fileImport);

        _logger.LogInformation(
            "Confirmed upload fileId {FileId} ({Size} bytes) at {S3Key}",
            fileImport.Id, fileImport.FileSize, fileImport.S3Key);

        return Result<FileUploadResponse>.Success(new FileUploadResponse(
            Id: fileImport.Id,
            ProjectId: fileImport.ProjectId,
            DataSourceType: fileImport.DataSourceType.ToString(),
            FileName: fileImport.FileName,
            OriginalName: fileImport.OriginalName,
            FilePath: fileImport.FilePath,
            FileSize: fileImport.FileSize,
            FileExtension: fileImport.FileExtension,
            CreatedDate: fileImport.CreatedDate)
        {
            S3Key = fileImport.S3Key
        });
    }
}
