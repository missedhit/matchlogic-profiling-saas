using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Create;

public class AddWordSmithRuleCommandHandler : IRequestHandler<AddWordSmithRuleCommand, Result<WordSmithRuleDto>>
{
    private readonly IWordSmithDictionaryService _service;

    public AddWordSmithRuleCommandHandler(IWordSmithDictionaryService service)
    {
        _service = service;
    }

    public async Task<Result<WordSmithRuleDto>> Handle(AddWordSmithRuleCommand request, CancellationToken cancellationToken)
    {
        try
        {
            var rule = await _service.AddRuleAsync(request.DictionaryId, request.CreateDto);
            return Result<WordSmithRuleDto>.Success(rule);
        }
        catch (KeyNotFoundException ex)
        {
            return Result<WordSmithRuleDto>.NotFound(ex.Message);
        }
        catch (Exception ex)
        {
            return Result<WordSmithRuleDto>.Error($"Failed to add rule: {ex.Message}");
        }
    }
}
