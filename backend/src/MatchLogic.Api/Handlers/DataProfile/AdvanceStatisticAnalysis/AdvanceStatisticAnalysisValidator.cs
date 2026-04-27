using MatchLogic.Api.Common;
using FluentValidation;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.DataProfile.AdvanceStatisticAnalysis;

public class AdvanceStatisticAnalysisValidator : AbstractValidator<AdvanceStatisticAnalysisRequest>
{
    public AdvanceStatisticAnalysisValidator(IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.DataSourceId)
            .SetValidator(new DataSourceIdValidator(dataSourceRepository));
    }
}