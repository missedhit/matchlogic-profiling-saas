namespace MatchLogic.Api.Handlers.JobInfo;

public class CancelJobRunResponse
{
    public bool Cancelled { get; set; }
    public string Message { get; set; } = string.Empty;
}
