using System;

namespace MatchLogic.Api.Handlers.RegexInfo.Delete;

public record DeleteRegexInfoRequest(Guid Id) : IRequest<Result<DeleteRegexInfoResponse>>;
