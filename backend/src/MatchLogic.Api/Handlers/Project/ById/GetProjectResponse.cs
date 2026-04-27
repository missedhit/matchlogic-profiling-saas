using System;

namespace MatchLogic.Api.Handlers.Project.ById;

public record GetProjectResponse(Guid Id, string Name, string Description, DateTime CreatedAt, DateTime? ModifiedAt);