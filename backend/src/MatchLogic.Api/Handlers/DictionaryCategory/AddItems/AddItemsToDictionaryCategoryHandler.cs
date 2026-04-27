using MatchLogic.Application.Interfaces.Dictionary;
using Mapster;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DictionaryCategory.AddItems;

public class AddItemsToDictionaryCategoryHandler(IDictionaryCategoryService dictionaryCategoryService,ILogger<AddItemsToDictionaryCategoryHandler> logger) : IRequestHandler<AddItemsToDictionaryCategoryRequest, Result<AddItemsToDictionaryCategoryResponse>>
{
    public async Task<Result<AddItemsToDictionaryCategoryResponse>> Handle(AddItemsToDictionaryCategoryRequest request,CancellationToken cancellationToken)
    {        
        logger.LogInformation("Adding items to Dictionary Category: {Id}", request.Id);
        await dictionaryCategoryService.AddItemsToDictionaryCategory(request.Id, request.Items);

        // Map the updated dictionary category to the response model
        var updatedCategory = await dictionaryCategoryService.GetDictionaryCategoryById(request.Id);
        var response = updatedCategory.Adapt<AddItemsToDictionaryCategoryResponse>();

        logger.LogInformation("Items added successfully to Dictionary Category with ID: {Id}", updatedCategory.Id);
        return Result<AddItemsToDictionaryCategoryResponse>.Success(response);
    }
}
