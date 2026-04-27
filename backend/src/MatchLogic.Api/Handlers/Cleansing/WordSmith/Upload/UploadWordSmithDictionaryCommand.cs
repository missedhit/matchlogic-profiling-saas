using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using Microsoft.AspNetCore.Http;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Upload;

public class UploadWordSmithDictionaryCommand : IRequest<Result<WordSmithDictionaryResponse>>
{
    public Guid? DictionaryId { get; set; }
    public IFormFile File {  get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public string Category { get; set; } = "Custom";
    public string? Encoding { get; set; }
}

public class UploadWordSmithDictionaryRequest
{
    public Guid? DictionaryId { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public string Category { get; set; } = "Custom";
    public string? Encoding { get; set; }
}