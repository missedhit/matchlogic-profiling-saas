namespace MatchLogic.Api.Auth;

public sealed class CognitoOptions
{
    public const string SectionName = "Cognito";

    public string? Region { get; set; }
    public string? UserPoolId { get; set; }
    public string? ClientId { get; set; }

    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Region) &&
        !string.IsNullOrWhiteSpace(UserPoolId) &&
        !string.IsNullOrWhiteSpace(ClientId);

    public string Authority => $"https://cognito-idp.{Region}.amazonaws.com/{UserPoolId}";
}
