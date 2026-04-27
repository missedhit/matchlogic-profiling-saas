using MatchLogic.Application.Interfaces.Dictionary;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;
using Mapster;

namespace MatchLogic.Api.Handlers.DictionaryCategory.Update;

public class UpdateDictionaryCategoryHandler(IDictionaryCategoryService dictionaryCategoryService,ILogger<UpdateDictionaryCategoryHandler> logger) : IRequestHandler<UpdateDictionaryCategoryRequest, Result<UpdateDictionaryCategoryResponse>>
{
    public async Task<Result<UpdateDictionaryCategoryResponse>> Handle(UpdateDictionaryCategoryRequest request,CancellationToken cancellationToken)
    {
        logger.LogInformation("Updating Dictionary Category: {Id}", request.Id);
        var dictionaryCategory = await dictionaryCategoryService.GetDictionaryCategoryById(request.Id);
        if (dictionaryCategory == null)
        {
            logger.LogWarning("Dictionary Category with ID {Id} not found.", request.Id);
            return Result.NotFound($"Dictionary category with ID {request.Id} not found.");
        }

        dictionaryCategory.Name = request.Name;
        dictionaryCategory.Description = request.Description;
        dictionaryCategory.Items = request.Items;

        await dictionaryCategoryService.UpdateDictionaryCategory(dictionaryCategory);
        logger.LogInformation("Dictionary Category updated successfully with ID: {Id}", dictionaryCategory.Id);
        // Map the updated dictionary category to the response model
        var response = dictionaryCategory.Adapt<UpdateDictionaryCategoryResponse>();
        return Result<UpdateDictionaryCategoryResponse>.Success(response);
    }
}
