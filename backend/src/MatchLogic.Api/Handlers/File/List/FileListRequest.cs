using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.File.List;

public record FileListRequest() : IRequest<Result<List<FileListResponse>>>;
