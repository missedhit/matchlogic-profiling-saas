using System;

namespace MatchLogic.Application.Identity;

public class IdentitySettings
{
    public const string SectionName = "Identity";

    /// <summary>
    /// Active provider name. Must match an IdentityProviderType enum value.
    /// Example values: "Keycloak", "Auth0", "AzureAD"
    /// </summary>
    public string Provider { get; set; } = "None";

    public IdentityProviderType ProviderType =>
        Enum.TryParse<IdentityProviderType>(Provider, ignoreCase: true, out var t)
            ? t
            : IdentityProviderType.None;
}
