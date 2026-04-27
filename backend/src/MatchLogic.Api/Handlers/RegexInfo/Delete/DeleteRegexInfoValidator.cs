using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading;
using System.Threading.Tasks;
using DomainRegex = MatchLogic.Domain.Regex.RegexInfo;

namespace MatchLogic.Api.Handlers.RegexInfo.Delete;

public class DeleteRegexInfoValidator : AbstractValidator<DeleteRegexInfoRequest>
{
    public DeleteRegexInfoValidator(IGenericRepository<DomainRegex, Guid> regexRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;

        RuleFor(x => x.Id)
            .NotNull().NotEmpty().WithMessage(ValidationMessages.Required("ID"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .MustAsync(ExistInDatabase).WithMessage("Regex pattern not found.")
            .WithErrorCode(ErrorCodeConstants.NotExists)
            .MustAsync(IsSystemPattern).WithMessage("System regex patterns cannot be deleted.")
            .WithErrorCode(ErrorCodeConstants.Error);

        // Check if the regex pattern exists
        async Task<bool> ExistInDatabase(Guid id, CancellationToken cancellationToken)
        {
            var pattern = await regexRepository.GetByIdAsync(id, Constants.Collections.RegexInfo);
            return pattern != null;
        }

        // Check if it's a system pattern which shouldn't be deleted
        async Task<bool> IsSystemPattern(Guid id, CancellationToken cancellationToken)
        {
            var pattern = await regexRepository.GetByIdAsync(id, Constants.Collections.RegexInfo);
            return pattern != null && !pattern.IsSystem; // System patterns can't be updated
        }
    }
}
