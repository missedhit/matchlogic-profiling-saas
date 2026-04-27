using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Import;
public class FileImportService : IFileImportService
{
    private readonly IGenericRepository<Domain.Import.FileImport, Guid> _importFileRepository;
   
    public FileImportService(IGenericRepository<Domain.Import.FileImport, Guid> importFileRepository, ILogger<FileImportService> logger)
    {
        this._importFileRepository = importFileRepository;
    }
    public async Task<Domain.Import.FileImport> CreateFile(Domain.Import.FileImport file)
    {
        file.CreatedDate = DateTime.Now;
        await _importFileRepository.InsertAsync(file, Constants.Collections.ImportFile);
        return file;
    }

    public async Task DeleteFile(Guid fileId)
    {
        var importFile = await _importFileRepository.GetByIdAsync(fileId, Constants.Collections.ImportFile);
        if (importFile == null)
        {
            throw new InvalidOperationException($"File with ID {fileId} not found");
        }

        await _importFileRepository.DeleteAsync(fileId, Constants.Collections.ImportFile);
    }

    public async Task<List<Domain.Import.FileImport>> GetAllFiles()
    {
        return await _importFileRepository.GetAllAsync(Constants.Collections.ImportFile);
    }

    public async Task<Domain.Import.FileImport> GetFileById(Guid fileId)
    {
        var importFile = await _importFileRepository.GetByIdAsync(fileId, Constants.Collections.ImportFile);
        return importFile ?? throw new InvalidOperationException($"File with ID {fileId} not found");
    }

}