using MatchLogic.Application.Interfaces.Regex;
using Mapster;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.RegexInfo.Update;

public class UpdateRegexInfoHandler(IRegexInfoService regexInfoService,ILogger<UpdateRegexInfoHandler> logger) : IRequestHandler<UpdateRegexInfoRequest, Result<UpdateRegexInfoResponse>>
{
    public async Task<Result<UpdateRegexInfoResponse>> Handle(UpdateRegexInfoRequest request, CancellationToken cancellationToken)
    {
        // Check if the regex pattern exists
        var existingPattern = await regexInfoService.GetRegexInfoById(request.Id);
        if (existingPattern == null)
        {
            logger.LogWarning("Regex pattern with ID {Id} not found", request.Id);
            return Result<UpdateRegexInfoResponse>.NotFound($"Regex pattern with ID {request.Id} not found.");
        }

        // Check if it's a system pattern which shouldn't be modified
        if (existingPattern.IsSystem)
        {
            logger.LogWarning("System regex pattern with ID {Id} cannot be modified", request.Id);
            return Result<UpdateRegexInfoResponse>.Invalid(new ValidationError("Id", "System regex patterns cannot be modified."));
        }
        // Update the Model
        existingPattern.Name = request.Name;
        existingPattern.Description = request.Description;
        existingPattern.RegexExpression = request.RegexExpression;
        existingPattern.IsDefault = request.IsDefault;  

        // Update the regex pattern
        await regexInfoService.UpdateRegexInfo(existingPattern);

        logger.LogInformation("Regex pattern with ID {Id} updated successfully", existingPattern.Id);
        var response = existingPattern.Adapt<UpdateRegexInfoResponse>();
        return Result<UpdateRegexInfoResponse>.Success(response);
    }
}

