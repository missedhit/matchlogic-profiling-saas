using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using MatchLogic.Domain.Import;

namespace MatchLogic.Application.Interfaces.Import;
public interface IFileImportService
{
    Task<FileImport> CreateFile(FileImport file);
    Task<FileImport> GetFileById(Guid fileId);
    Task DeleteFile(Guid fileId);
    Task<List<FileImport>> GetAllFiles();
}