using MatchLogic.Application.Interfaces.Dictionary;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DictionaryCategory.Delete;

public class DeleteDictionaryCategoryHandler(IDictionaryCategoryService dictionaryCategoryService,ILogger<DeleteDictionaryCategoryHandler> logger) : IRequestHandler<DeleteDictionaryCategoryRequest, Result<DeleteDictionaryCategoryResponse>>
{
    public async Task<Result<DeleteDictionaryCategoryResponse>> Handle(DeleteDictionaryCategoryRequest request,CancellationToken cancellationToken)
    {
        // Perform the delete operation
        await dictionaryCategoryService.DeleteDictionaryCategory(request.Id);
        logger.LogInformation("Dictionary Category deleted successfully with ID: {Id}", request.Id);
        return Result<DeleteDictionaryCategoryResponse>.Success(new DeleteDictionaryCategoryResponse(request.Id, "Dictionary category deleted successfully."));
    }
}
