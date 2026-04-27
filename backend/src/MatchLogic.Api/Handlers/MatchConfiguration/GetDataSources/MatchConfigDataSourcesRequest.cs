using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.MatchConfiguration.GetDataSources;

public record MatchConfigDataSourcesRequest(Guid ProjectId) : IRequest<Result<List<MatchConfigDataSourcesResponse>>>;
