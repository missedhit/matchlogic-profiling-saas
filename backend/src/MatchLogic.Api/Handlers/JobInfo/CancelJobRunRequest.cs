using System;
namespace MatchLogic.Api.Handlers.JobInfo;
public record CancelJobRunRequest(Guid RunId) : IRequest<Result<CancelJobRunResponse>>;
