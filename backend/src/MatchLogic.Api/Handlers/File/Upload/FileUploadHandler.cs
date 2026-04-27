using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Domain.Import;
using MatchLogic.Application.Interfaces.Import;
using Mapster;

namespace MatchLogic.Api.Handlers.File.Upload;
[Obsolete("This endpoint is deprecated. Use the new /DataImport File upload endpoint instead.")]
public class FileUploadHandler : IRequestHandler<FileUploadRequest, Result<FileUploadResponse>>
{
    private readonly ILogger<FileUploadHandler> _logger;
    private readonly IFileImportService _fileImportService;

    public FileUploadHandler(IFileImportService fileImportService, ILogger<FileUploadHandler> logger)
    {
        _logger = logger;
        _fileImportService = fileImportService;
    }
    public async Task<Result<FileUploadResponse>> Handle(FileUploadRequest request, CancellationToken cancellationToken)
    {
        try
        {

            _logger.LogInformation("Uploading file {FileName}", request.File.FileName);

            var uploadFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData)
                , "MatchLogicApi"
                , "Uploads");
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

            FileImport fileImport = new()
            {
                Id = fileId,
                ProjectId = request.ProjectId,
                DataSourceType = Enum.Parse<DataSourceType>(request.SourceType, true),
                OriginalName = file.FileName,   
                FileName = fileName,
                FilePath = filePath,
                FileSize = file.Length,
                FileExtension = fileExtension               
            };
            await _fileImportService.CreateFile(fileImport);

            _logger.LogInformation("File uploaded successfully. FileId: {FileId}, OriginalName: {OriginalFileName}", fileId, file.FileName);
            var response = fileImport.Adapt<FileUploadResponse>();
            return Result<FileUploadResponse>.Success(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while uploading file {FileName}", request.File.FileName);
            throw;
        }
    }
}
