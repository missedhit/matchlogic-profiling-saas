using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using FluentValidation;
using System;
using System.Threading;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Delete;

public class DeleteMatchConfigurationValidator : AbstractValidator<DeleteMatchConfigurationRequest>
{
    public DeleteMatchConfigurationValidator(IMatchConfigurationService matchConfigurationService)
    {
        RuleFor(x => x.MatchConfigurationId)
            .NotEmpty()
            .WithMessage(ValidationMessages.CannotBeEmpty("Match configuration ID"))
            .NotNull()
            .WithMessage(ValidationMessages.CannotBeNull("Match configuration ID"));

        RuleFor(x => x.MatchConfigurationId)
            .MustAsync(async (Guid id, CancellationToken token) =>
            {
                if (id == Guid.Empty)
                    return false;

                var result = await matchConfigurationService.GetDataSourcePairsByIdAsync(id);
                return result is not null;
            })
            .WithMessage(ValidationMessages.NotExists("Match configuration ID"))
            .WithErrorCode(ErrorCodeConstants.NotExists);
    }
}