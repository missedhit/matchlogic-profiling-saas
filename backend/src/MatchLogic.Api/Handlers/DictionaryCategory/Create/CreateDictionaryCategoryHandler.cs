using MatchLogic.Application.Interfaces.Dictionary;
using Mapster;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DictionaryCategory.Create;

public class CreateDictionaryCategoryHandler(IDictionaryCategoryService dictionaryCategoryService,ILogger<CreateDictionaryCategoryHandler> logger) : IRequestHandler<CreateDictionaryCategoryRequest, Result<CreateDictionaryCategoryResponse>>
{
    public async Task<Result<CreateDictionaryCategoryResponse>> Handle(CreateDictionaryCategoryRequest request,CancellationToken cancellationToken)
    {
        logger.LogInformation("Creating Dictionary Category: {Name}", request.Name);

        var dictionaryCategory = await dictionaryCategoryService.CreateDictionaryCategory(
            request.Name,
            request.Description,
            request.Items);

        logger.LogInformation("Dictionary Category created successfully with ID: {Id}", dictionaryCategory.Id);
        var response = dictionaryCategory.Adapt<CreateDictionaryCategoryResponse>();
        return Result<CreateDictionaryCategoryResponse>.Success(response);
    }
}
