using System;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Get;

public record GetMatchConfigurationRequest(Guid ProjectId) : IRequest<Result<BaseMatchConfigurationResponse>>;
