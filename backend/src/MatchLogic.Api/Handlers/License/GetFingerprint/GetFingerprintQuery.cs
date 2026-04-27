namespace MatchLogic.Api.Handlers.License.GetFingerprint;

public class GetFingerprintQuery : IRequest<Result<GetFingerprintResponse>>
{
}

public record GetFingerprintResponse(string Fingerprint);
