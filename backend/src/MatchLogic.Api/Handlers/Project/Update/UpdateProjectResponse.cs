using System;

namespace MatchLogic.Api.Handlers.Project.Update;

public record UpdateProjectResponse(Guid Id, string Name, string Description, DateTime CreatedAt, DateTime? ModifiedAt);
