using MatchLogic.Domain.Entities.Common;
using System;

namespace MatchLogic.Domain.Import;

/// <summary>
/// Stores encrypted OAuth tokens for cloud storage connectors (Google Drive, Dropbox, OneDrive).
/// Persisted in MongoDB collection "OAuthTokens".
/// </summary>
public class OAuthTokenStore : AuditableEntity
{
    public Guid DataSourceId { get; set; }
    public DataSourceType Provider { get; set; }
    public string AccessToken { get; set; } = string.Empty;
    public string RefreshToken { get; set; } = string.Empty;
    public DateTime AccessTokenExpiry { get; set; }
    public DateTime? RefreshTokenExpiry { get; set; }
    public string Scopes { get; set; } = string.Empty;
    public string AccountEmail { get; set; } = string.Empty;
    public string AccountDisplayName { get; set; } = string.Empty;
}

/// <summary>
/// DTO returned from OAuth token exchange operations.
/// </summary>
public class OAuthTokenResponse
{
    public string AccessToken { get; set; } = string.Empty;
    public string RefreshToken { get; set; } = string.Empty;
    public int ExpiresInSeconds { get; set; }
    public string Scopes { get; set; } = string.Empty;
    public string AccountEmail { get; set; } = string.Empty;
    public string AccountDisplayName { get; set; } = string.Empty;
}
