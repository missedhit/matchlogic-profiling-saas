using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Project.List;
public record ProjectListRequest : IRequest<Result<List<ProjectListResponse>>>;