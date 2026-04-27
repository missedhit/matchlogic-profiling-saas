using System;
using System.Collections.Generic;
namespace MatchLogic.Api.Handlers.DataSource.GetHeaders;
public record GetHeadersDataSourceRequest(Guid Id, bool fetchCleanse = false) : IRequest<Result<List<string>>>;