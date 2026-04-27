
using System;

namespace MatchLogic.Api.Handlers.MatchDefinition.Get;
public record MatchDefinitionRequest(Guid projectId) : IRequest<Result<MatchDefinitionResponse>>;

