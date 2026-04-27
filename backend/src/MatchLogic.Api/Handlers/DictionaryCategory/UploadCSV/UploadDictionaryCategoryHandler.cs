
using MatchLogic.Application.Interfaces.Dictionary;
using Mapster;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DictionaryCategory.UploadCSV;

public class UploadDictionaryCategoryHandler(IDictionaryCategoryService dictionaryCategoryService, ILogger<UploadDictionaryCategoryHandler> logger) : IRequestHandler<UploadDictionaryCategoryRequest, Result<UploadDictionaryCategoryResponse>>
{

    public async Task<Result<UploadDictionaryCategoryResponse>> Handle(UploadDictionaryCategoryRequest request, CancellationToken cancellationToken)
    {

        #region Upload File and Get File Path

        logger.LogInformation("Uploading CSV for Dictionary Category {FileName}", request.File.FileName);

        var uploadFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData)
            , "MatchLogicApi"
            , "Uploads");
        Directory.CreateDirectory(uploadFolder);
        var file = request.File;
        var filePath = Path.Combine(uploadFolder, file.FileName);
        using (var stream = new FileStream(filePath, FileMode.Create))
        {
            file.CopyToAsync(stream, cancellationToken).Wait(cancellationToken);
        }
        logger.LogInformation("File uploaded successfully to {FilePath}", filePath);
        #endregion

        logger.LogInformation("Creating Dictionary Category: {Name}", request.Name);
        try
        {
            var dictionaryCategory = await dictionaryCategoryService.CreateDictionaryCategoryByFilePath(request.Name, request.Description, filePath, cancellationToken);

            logger.LogInformation("Dictionary Category created successfully with ID: {Id}", dictionaryCategory.Id);
            var response = dictionaryCategory.Adapt<UploadDictionaryCategoryResponse>();
            return Result<UploadDictionaryCategoryResponse>.Success(response);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error creating Dictionary Category: {Message}", ex.Message);
            throw;
        }
    }
}
