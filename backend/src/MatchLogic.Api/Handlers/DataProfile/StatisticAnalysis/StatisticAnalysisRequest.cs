using System;

namespace MatchLogic.Api.Handlers.DataProfile.StatisticAnalysis;

public record StatisticAnalysisRequest(Guid DataSourceId) : IRequest<Result<StatisticAnalysisResponse>>;
