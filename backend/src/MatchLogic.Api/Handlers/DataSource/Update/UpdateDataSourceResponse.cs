
using Azure.Core;
using System;

namespace MatchLogic.Api.Handlers.DataSource.Update;

public record UpdateDataSourceResponse(Guid Id, string Name, DateTime? ModifiedAt);
