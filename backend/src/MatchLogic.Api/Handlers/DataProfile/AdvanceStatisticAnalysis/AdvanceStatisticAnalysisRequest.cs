using System;

namespace MatchLogic.Api.Handlers.DataProfile.AdvanceStatisticAnalysis;

public record AdvanceStatisticAnalysisRequest(Guid DataSourceId) : IRequest<Result<AdvanceStatisticAnalysisResponse>>;