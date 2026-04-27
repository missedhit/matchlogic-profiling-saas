using Microsoft.Graph.Models;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.LiveSearch.Search;

public record SearchRequest(
    Dictionary<string, object> Record
) : IRequest<Result<SearchResponse>>;

