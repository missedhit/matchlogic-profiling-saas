using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Get;

public class GetWordSmithDictionaryQuery : IRequest<Result<WordSmithDictionaryDto>>
{
    public Guid Id { get; set; }
}
