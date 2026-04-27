using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Update;

public class UpdateWordSmithRuleCommandHandler : IRequestHandler<UpdateWordSmithRuleCommand, Result<WordSmithRuleDto>>
{
    private readonly IWordSmithDictionaryService _service;

    public UpdateWordSmithRuleCommandHandler(IWordSmithDictionaryService service)
    {
        _service = service;
    }

    public async Task<Result<WordSmithRuleDto>> Handle(UpdateWordSmithRuleCommand request, CancellationToken cancellationToken)
    {
        try
        {
            var rule = await _service.UpdateRuleAsync(request.RuleId, request.UpdateDto);
            return Result<WordSmithRuleDto>.Success(rule);
        }
        catch (KeyNotFoundException ex)
        {
            return Result<WordSmithRuleDto>.NotFound(ex.Message);
        }
        catch (Exception ex)
        {
            return Result<WordSmithRuleDto>.Error($"Failed to update rule: {ex.Message}");
        }
    }
}
