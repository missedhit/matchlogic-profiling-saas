using System;

namespace MatchLogic.Api.Handlers.Project.Update;

public record UpdateProjectRequest(Guid Id, string Name, string Description) : IRequest<Result<UpdateProjectResponse>>;
