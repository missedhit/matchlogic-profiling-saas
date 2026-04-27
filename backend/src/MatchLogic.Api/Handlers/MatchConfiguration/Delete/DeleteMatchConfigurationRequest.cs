using System;
namespace MatchLogic.Api.Handlers.MatchConfiguration.Delete;
public record DeleteMatchConfigurationRequest(Guid MatchConfigurationId) : IRequest<Result<DeleteMatchConfigurationResponse>>;
