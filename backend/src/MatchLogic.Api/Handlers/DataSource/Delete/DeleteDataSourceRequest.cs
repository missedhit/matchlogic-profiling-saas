using System;
namespace MatchLogic.Api.Handlers.DataSource.Delete;
public record DeleteDataSourceRequest(Guid ProjectId, Guid Id) : IRequest<Result<DeleteDataSourceResponse>>;
