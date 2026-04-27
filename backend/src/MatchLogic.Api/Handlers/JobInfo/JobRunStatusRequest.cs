using System;
namespace MatchLogic.Api.Handlers.JobInfo;
public record JobRunStatusRequest(Guid RunId) : IRequest<Result<JobRunStatusResponse>>;
