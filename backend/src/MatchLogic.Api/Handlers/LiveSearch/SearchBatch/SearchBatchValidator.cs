using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.LiveSearch.SearchBatch;

public class SearchBatchValidator : AbstractValidator<SearchBatchRequest>
{
    public SearchBatchValidator()
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.Records)
            .NotNull()
            .WithMessage(ValidationMessages.Required("Records"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .Must(records => records != null && records.Count > 0)
            .WithMessage("Records list must contain at least one record")
            .WithErrorCode(ErrorCodeConstants.Invalid)
            .Must((request, records) =>
            {
                var maxBatchSize = request.Options?.MaxBatchSize ?? 50;
                return records.Count <= maxBatchSize;
            })
            .WithMessage(request =>
                $"Records list cannot exceed {request.Options?.MaxBatchSize ?? 50} records")
            .WithErrorCode(ErrorCodeConstants.LimitExceeded);

        RuleFor(x => x.Records)
            .Must(records => records.All(r => r != null && r.Count > 0))
            .When(x => x.Records != null && x.Records.Count > 0)
            .WithMessage("All records must contain at least one field")
            .WithErrorCode(ErrorCodeConstants.Invalid);

        
    }
}