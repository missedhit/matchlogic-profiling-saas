using Microsoft.AspNetCore.Http;
using System;

namespace MatchLogic.Api.Handlers.DictionaryCategory.UploadCSV;

public record UploadDictionaryCategoryRequest : IRequest<Result<UploadDictionaryCategoryResponse>>
{
    public IFormFile File { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }

}