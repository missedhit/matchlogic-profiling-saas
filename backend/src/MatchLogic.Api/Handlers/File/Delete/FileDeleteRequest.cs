using System;

namespace MatchLogic.Api.Handlers.File.Delete;

public record FileDeleteRequest(Guid FileId) : IRequest<Result<FileDeleteResponse>>;

