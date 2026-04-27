
using MatchLogic.Application.Features.Upload;
using MatchLogic.Domain.Entities;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchResult.Create;
public class CreateMatchResultJobCommandValidator : AbstractValidator<CreateMatchResultJobCommand>
{
    public CreateMatchResultJobCommandValidator()
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        // Basic validations
        RuleFor(x => x.JobId)
            .NotEqual(Guid.Empty)
            .WithMessage("JobId must be provided");
    }
}
