using System;
namespace MatchLogic.Api.Handlers.Project.Delete;
public record DeleteProjectRequest(Guid Guid) : IRequest<Result<DeleteProjectResponse>>;
