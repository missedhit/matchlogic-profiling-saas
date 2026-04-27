using MatchLogic.Application.Interfaces.Import;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.File.Delete;

public class FileDeleteHandler(IFileImportService fileImportService) : IRequestHandler<FileDeleteRequest, Result<FileDeleteResponse>>
{
    public async Task<Result<FileDeleteResponse>> Handle(FileDeleteRequest request, CancellationToken cancellationToken)
    {
        await fileImportService.DeleteFile(request.FileId);
        return Result<FileDeleteResponse>.NoContent();
    }
}
