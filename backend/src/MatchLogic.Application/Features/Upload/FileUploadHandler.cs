using Ardalis.Result;
using MatchLogic.Application.Features.HealthCheck.Echo;
using MatchLogic.Application.Features.HealthCheck;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Extensions.Logging;
using System.IO;
using MatchLogic.Application.Common;

namespace MatchLogic.Application.Features.Upload;
public class FileUploadHandler : IRequestHandler<FileUploadRequest, Result<FileUploadResponse>>
{
    private ILogger<FileUploadHandler> _logger;

    public FileUploadHandler(ILogger<FileUploadHandler> logger)
    {
        _logger = logger;
    }
    public Task<Result<FileUploadResponse>> Handle(FileUploadRequest request, CancellationToken cancellationToken)
    {
        var uploadFolder = StoragePaths.DefaultUploadPath;

        Directory.CreateDirectory(uploadFolder);

        var file = request.File;
        var fileId = Guid.NewGuid();
        var fileExtension = Path.GetExtension(file.FileName);
        var fileName = $"{fileId}{fileExtension}";
        var filePath = Path.Combine(uploadFolder, fileName);

        using (var stream = new FileStream(filePath, FileMode.Create))
        {
            file.CopyToAsync(stream, cancellationToken).Wait();
        }

        _logger.LogInformation("File uploaded successfully. FileId: {FileId}, OriginalName: {OriginalFileName}", fileId, file.FileName);
        var response = new FileUploadResponse(fileId);

        return Task.FromResult(new Result<FileUploadResponse>(response));
    }
}
