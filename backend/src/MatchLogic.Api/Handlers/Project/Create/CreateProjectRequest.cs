namespace MatchLogic.Api.Handlers.Project.Create;
public record CreateProjectRequest(string Name, string Description) : IRequest<Result<CreateProjectResponse>>;
