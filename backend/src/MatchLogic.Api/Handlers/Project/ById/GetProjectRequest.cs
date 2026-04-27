using System;

namespace MatchLogic.Api.Handlers.Project.ById;

public record GetProjectRequest(Guid Id) : IRequest<Result<GetProjectResponse>>;
