using System;
namespace MatchLogic.Api.Handlers.DataSource.Update;
public record UpdateDataSourceRequest(Guid Id, string Name) : IRequest<Result<UpdateDataSourceResponse>>;
