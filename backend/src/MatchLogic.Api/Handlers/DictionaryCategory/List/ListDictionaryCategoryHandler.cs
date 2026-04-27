using MatchLogic.Application.Interfaces.Dictionary;
using Mapster;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DictionaryCategory.List;

public class ListDictionaryCategoryHandler(IDictionaryCategoryService dictionaryCategoryService,ILogger<ListDictionaryCategoryHandler> logger) : IRequestHandler<ListDictionaryCategoryRequest, Result<List<BaseDictionaryCategoryDTO>>>
{
    public async Task<Result<List<BaseDictionaryCategoryDTO>>> Handle(ListDictionaryCategoryRequest request,CancellationToken cancellationToken)
    {
        var categories = await dictionaryCategoryService.GetAllDictionaryCategories();
        if (categories == null || categories.Count == 0)
        {
            logger.LogInformation("No dictionary category found");
            return Result<List<BaseDictionaryCategoryDTO>>.NotFound("No dictionary category found.");
        }
        // Return success with the list of dictionary category
        var response = categories.Adapt<List<BaseDictionaryCategoryDTO>>();
        return Result<List<BaseDictionaryCategoryDTO>>.Success(response);
    }
}
