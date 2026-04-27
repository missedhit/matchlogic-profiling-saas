using MatchLogic.Application.Interfaces.Dictionary;
using Mapster;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DictionaryCategory.RemoveItems;

public class RemoveItemsFromDictionaryCategoryHandler(IDictionaryCategoryService dictionaryCategoryService, ILogger<RemoveItemsFromDictionaryCategoryHandler> logger) 
    : IRequestHandler<RemoveItemsFromDictionaryCategoryRequest, Result<RemoveItemsFromDictionaryCategoryResponse>>
{
    public async Task<Result<RemoveItemsFromDictionaryCategoryResponse>> Handle(RemoveItemsFromDictionaryCategoryRequest request,CancellationToken cancellationToken)
    {
        logger.LogInformation("Removing items from Dictionary Category: {Id}", request.Id);
        await dictionaryCategoryService.RemoveItemsFromDictionaryCategory(request.Id, request.Items);

        // Map the updated dictionary category to the response model
        var updatedCategory = await dictionaryCategoryService.GetDictionaryCategoryById(request.Id);
        var response = updatedCategory.Adapt<RemoveItemsFromDictionaryCategoryResponse>();
      
        logger.LogInformation("Items removed successfully from Dictionary Category with ID: {Id}", updatedCategory.Id);
        return Result<RemoveItemsFromDictionaryCategoryResponse>.Success(response);
    }
}
