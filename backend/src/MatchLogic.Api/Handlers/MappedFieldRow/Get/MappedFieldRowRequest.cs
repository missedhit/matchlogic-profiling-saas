using System;

namespace MatchLogic.Api.Handlers.MappedFieldRow.Get;
public record MappedFieldRowRequest(Guid projectId) : IRequest<Result<MappedFieldRowResponse>>;

