
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.RegexInfo.List;
public record ListRegexInfoRequest : IRequest<Result<List<RegexInfoDTO>>>;
