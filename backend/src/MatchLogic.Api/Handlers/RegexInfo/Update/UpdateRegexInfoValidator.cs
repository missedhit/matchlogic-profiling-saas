using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading;
using System.Threading.Tasks;
using static Azure.Core.HttpHeader;
using DomainRegex = MatchLogic.Domain.Regex.RegexInfo;

namespace MatchLogic.Api.Handlers.RegexInfo.Update;

public class UpdateRegexInfoValidator : AbstractValidator<UpdateRegexInfoRequest>
{
    public UpdateRegexInfoValidator(IGenericRepository<DomainRegex, Guid> regexRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;

        RuleFor(x => x.Id)
            .NotNull().NotEmpty().WithMessage(ValidationMessages.Required("ID"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .MustAsync(ExistInDatabase).WithMessage(ValidationMessages.NotExists("Regex pattern"))
            .WithErrorCode(ErrorCodeConstants.NotExists)
            .MustAsync(IsSystemPattern).WithMessage("System regex patterns cannot be modified.")
            .WithErrorCode(ErrorCodeConstants.Error);

        RuleFor(x => x.Name)
            .NotNull().NotEmpty().WithMessage(ValidationMessages.Required("Name"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .MaximumLength(Common.ApiConstants.RegexFieldLength.NameMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Name", ApiConstants.RegexFieldLength.NameMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded)
            .MustAsync(BeUniqueNameForUpdate).WithMessage(ValidationMessages.AlreadyExists("regex pattern"))
            .WithErrorCode(ErrorCodeConstants.Error);

        RuleFor(x => x.Description)
            .MaximumLength(Application.Common.Constants.FieldLength.DescriptionMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Description", Application.Common.Constants.FieldLength.DescriptionMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);


        RuleFor(x => x.RegexExpression)
            .NotNull().NotEmpty().WithMessage(ValidationMessages.Required("Regex expression"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .Must(BeValidRegex).WithMessage(ValidationMessages.Invalid("regex expression"))
            .WithErrorCode(ErrorCodeConstants.Invalid)
            .MustAsync(BeUniqueRegex).WithMessage(ValidationMessages.AlreadyExists("regex expression"))
            .WithErrorCode(ErrorCodeConstants.Error)
            .MaximumLength(Common.ApiConstants.RegexFieldLength.RegexMaxLength).WithMessage(ValidationMessages.MaxLength("RegexExpression", ApiConstants.RegexFieldLength.RegexMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        async Task<bool> ExistInDatabase(Guid id, CancellationToken cancellationToken)
        {
            var pattern = await regexRepository.GetByIdAsync(id, Application.Common.Constants.Collections.RegexInfo);
            return pattern != null;
        }
        async Task<bool> BeUniqueRegex(UpdateRegexInfoRequest request, string regex, CancellationToken cancellationToken)
        {
            // Check if any other regex pattern (excluding the current request Id) has the same regex expression
            var existing = await regexRepository.QueryAsync(
                x => x.RegexExpression == regex && x.Id != request.Id,
                Application.Common.Constants.Collections.RegexInfo
            );
            return existing == null || existing.Count == 0;
        }
        async Task<bool> IsSystemPattern(Guid id, CancellationToken cancellationToken)
        {
            var pattern = await regexRepository.GetByIdAsync(id, Application.Common.Constants.Collections.RegexInfo);
            return pattern != null && !pattern.IsSystem; // System patterns can't be updated
        }

        async Task<bool> BeUniqueNameForUpdate(UpdateRegexInfoRequest request, string name, CancellationToken cancellationToken)
        {
            var existingEntities = await regexRepository.QueryAsync(
                x => x.Name == name && x.Id != request.Id,
                Application.Common.Constants.Collections.RegexInfo);

            return existingEntities == null || existingEntities.Count == 0;
        }
    }


    private bool BeValidRegex(string pattern)
    {
        if (string.IsNullOrWhiteSpace(pattern))
            return false;

        try
        {
            // Validate if the regex pattern is valid
            System.Text.RegularExpressions.Regex.IsMatch("", pattern);
            return true;
        }
        catch
        {
            return false;
        }
    }
}

