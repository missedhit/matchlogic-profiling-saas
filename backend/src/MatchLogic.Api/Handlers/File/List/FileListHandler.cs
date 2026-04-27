using MatchLogic.Application.Interfaces.Import;
using Mapster;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.File.List;

public class FileListHandler(IFileImportService fileImportService) : IRequestHandler<FileListRequest, Result<List<FileListResponse>>>
{
    public async Task<Result<List<FileListResponse>>> Handle(FileListRequest request, CancellationToken cancellationToken)
    {
        var result = await fileImportService.GetAllFiles();
        if (result == null)
            return Result<List<FileListResponse>>.NotFound();
        if (!result.Any())
            return Result<List<FileListResponse>>.NotFound();
        var items = result.Adapt<List<FileListResponse>>();
        return Result<List<FileListResponse>>.Success(items);
    }
}
