using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.LiveSearch.Search;

public class SearchValidator : AbstractValidator<SearchRequest>
{
    public SearchValidator()
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.Record)
            .NotNull()
            .WithMessage(ValidationMessages.Required("Record"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .Must(record => record != null && record.Count > 0)
            .WithMessage("Record must contain at least one field")
            .WithErrorCode(ErrorCodeConstants.Invalid);
        
        
    }
}