using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Get;

public class ListWordSmithDictionariesQuery : IRequest<Result<List<WordSmithDictionaryDto>>>
{
}
