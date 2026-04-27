using MatchLogic.Domain.CleansingAndStandaradization;
using LiteDB;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using System.Reflection.Metadata;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Cleansing.ProperCaseOption.Update;

public class UpdateProperCaseOptionsHandler : IRequestHandler<UpdateProperCaseOptionsCommand, Result<UpdateProperCaseOptionsResponse>>
{
    private readonly IGenericRepository<ProperCaseOptions,Guid> _repository;
    private readonly ProperCaseOptions _currentOptions;
    private readonly ILogger<UpdateProperCaseOptionsHandler> _logger;

    public UpdateProperCaseOptionsHandler(
        IGenericRepository<ProperCaseOptions, Guid> repository,
        ProperCaseOptions currentOptions,
        ILogger<UpdateProperCaseOptionsHandler> logger)
    {
        _repository = repository;
        _currentOptions = currentOptions;
        _logger = logger;
    }

    public async Task<Result<UpdateProperCaseOptionsResponse>> Handle(UpdateProperCaseOptionsCommand request, CancellationToken cancellationToken)
    {

        await _repository.DeleteAllAsync(x => 1 == 1, Constants.Collections.ProperCaseOptions);

        request.Options.UpdatedAt = DateTime.UtcNow;
        var option = new ProperCaseOptions
        {
            ActionOnException = request.Options.ActionOnException,
            CreatedAt = DateTime.UtcNow,
            Delimiters = request.Options.Delimiters,
            Exceptions = request.Options.Exceptions,
            IgnoreCaseOnExceptions = request.Options.IgnoreCaseOnExceptions,
            UpdatedAt = DateTime.UtcNow,
        };
        await _repository.InsertAsync(option, Constants.Collections.ProperCaseOptions);


        // Update the singleton instance in-place
        _currentOptions.Delimiters = request.Options.Delimiters;
        _currentOptions.IgnoreCaseOnExceptions = request.Options.IgnoreCaseOnExceptions;
        _currentOptions.ActionOnException = request.Options.ActionOnException;
        _currentOptions.Exceptions.Clear();
        _currentOptions.Exceptions.AddRange(request.Options.Exceptions);
        _currentOptions.UpdatedAt = option.UpdatedAt;

        _logger.LogInformation("ProperCaseOptions updated successfully");

        return Result<UpdateProperCaseOptionsResponse>.Success(new UpdateProperCaseOptionsResponse(_currentOptions));


    }
}
