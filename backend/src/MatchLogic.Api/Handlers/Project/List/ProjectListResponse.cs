using System;

namespace MatchLogic.Api.Handlers.Project.List;
public record ProjectListResponse(Guid Id, string Name, string Description, DateTime CreatedAt, DateTime? ModifiedAt);