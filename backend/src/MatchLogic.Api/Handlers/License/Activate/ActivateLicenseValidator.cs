using FluentValidation;

namespace MatchLogic.Api.Handlers.License.Activate;

public class ActivateLicenseValidator : AbstractValidator<ActivateLicenseCommand>
{
    public ActivateLicenseValidator()
    {
        RuleFor(x => x.LicenseKey)
            .NotEmpty().WithMessage("License key is required.")
            .MinimumLength(50).WithMessage("License key format is invalid.");
    }
}
