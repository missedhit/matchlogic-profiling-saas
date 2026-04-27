using System;
namespace MatchLogic.Api.Handlers.Project.Create;
public record CreateProjectResponse(Guid Id, string Name, string Description, DateTime CreatedAt, DateTime? ModifiedAt);
