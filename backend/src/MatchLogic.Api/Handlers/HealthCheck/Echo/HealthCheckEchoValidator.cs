using MatchLogic.Application.Common;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace MatchLogic.Api.Handlers.HealthCheck.Echo;
public class HealthCheckEchoValidator : AbstractValidator<HealthCheckEchoRequest>
{
    public HealthCheckEchoValidator()
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.Text)
            .NotEmpty()
            .MaximumLength(StringSizes.Max);
    }
}
