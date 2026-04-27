using MatchLogic.Api.Common;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.DataProfile.StatisticAnalysis;

public class StatisticAnalysisValidator : AbstractValidator<StatisticAnalysisRequest>
{
    public StatisticAnalysisValidator(IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.DataSourceId)
            .SetValidator(new DataSourceIdValidator(dataSourceRepository));
    }
}
