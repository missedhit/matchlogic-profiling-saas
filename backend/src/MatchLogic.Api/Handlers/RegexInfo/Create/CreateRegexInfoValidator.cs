using MatchLogic.Api.Common;
using FluentValidation;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using DomainRegex = MatchLogic.Domain.Regex.RegexInfo;
using System;

namespace MatchLogic.Api.Handlers.RegexInfo.Create;

public class CreateRegexInfoValidator : AbstractValidator<CreateRegexInfoRequest>
{
    public CreateRegexInfoValidator(IGenericRepository<DomainRegex, Guid> regexRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;

        RuleFor(x => x.Name)
            .NotNull().NotEmpty().WithMessage(ValidationMessages.Required("Name"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .MaximumLength(Common.ApiConstants.RegexFieldLength.NameMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Name", ApiConstants.RegexFieldLength.NameMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded)
            .MustAsync(BeUniqueName).WithMessage(ValidationMessages.AlreadyExists("regex pattern"))
            .WithErrorCode(ErrorCodeConstants.Error);


        RuleFor(x => x.Description)
            .MaximumLength(Constants.FieldLength.DescriptionMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Description", Constants.FieldLength.DescriptionMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);


        RuleFor(x => x.RegexExpression)
            .NotNull().NotEmpty().WithMessage(ValidationMessages.Required("Regex expression"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .Must(BeValidRegex).WithMessage(ValidationMessages.Invalid("regex expression"))
            .WithErrorCode(ErrorCodeConstants.Invalid)
            .MustAsync(BeUniqueRegex).WithMessage("This regex/pattern already exists")
            .WithErrorCode(ErrorCodeConstants.Error)
            .MaximumLength(Common.ApiConstants.RegexFieldLength.RegexMaxLength).WithMessage(ValidationMessages.MaxLength("RegexExpression", ApiConstants.RegexFieldLength.RegexMaxLength))
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);


        async Task<bool> BeUniqueName(string name, CancellationToken cancellationToken)
        {
            var existing = await regexRepository.QueryAsync(x => x.Name == name, Application.Common.Constants.Collections.RegexInfo);
            return existing == null || existing?.Count == 0;
        }

        async Task<bool> BeUniqueRegex(string regex, CancellationToken cancellationToken)
        {
            var existing = await regexRepository.QueryAsync(x => x.RegexExpression == regex, Application.Common.Constants.Collections.RegexInfo);
            return existing == null || existing?.Count == 0;
        }
    }


    private bool BeValidRegex(string pattern)
    {
        if (string.IsNullOrWhiteSpace(pattern))
            return false;

        try
        {
            // Validate if the regex pattern is valid
            System.Text.RegularExpressions.Regex.Match("", pattern);
            return true;
        }
        catch
        {
            return false;
        }
    }


}