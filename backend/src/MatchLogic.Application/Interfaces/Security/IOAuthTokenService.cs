using MatchLogic.Domain.Import;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Security;

/// <summary>
/// Manages OAuth tokens for cloud storage connectors (Google Drive, Dropbox, OneDrive).
/// Handles token storage, automatic refresh, and revocation.
/// </summary>
public interface IOAuthTokenService
{
    /// <summary>
    /// Get a valid access token for a datasource, refreshing if expired.
    /// Returns the decrypted access token ready for API use.
    /// </summary>
    Task<string> GetValidAccessTokenAsync(Guid dataSourceId, CancellationToken ct = default);

    /// <summary>
    /// Store tokens received from an OAuth code exchange.
    /// Tokens are encrypted before storage.
    /// </summary>
    Task StoreTokensAsync(Guid dataSourceId, DataSourceType provider, OAuthTokenResponse tokens, CancellationToken ct = default);

    /// <summary>
    /// Revoke tokens and delete from storage.
    /// </summary>
    Task<bool> RevokeTokensAsync(Guid dataSourceId, CancellationToken ct = default);

    /// <summary>
    /// Generate the OAuth authorization URL for a given provider.
    /// </summary>
    Task<string> GetAuthorizationUrlAsync(DataSourceType provider, string redirectUri, string state, CancellationToken ct = default);

    /// <summary>
    /// Exchange an authorization code for access/refresh tokens.
    /// </summary>
    Task<OAuthTokenResponse> ExchangeCodeAsync(DataSourceType provider, string code, string redirectUri, CancellationToken ct = default);

    /// <summary>
    /// Check if a datasource has valid OAuth tokens stored.
    /// </summary>
    Task<bool> HasValidTokensAsync(Guid dataSourceId, CancellationToken ct = default);

    /// <summary>
    /// Get the stored token info (without decrypting sensitive fields) for display purposes.
    /// </summary>
    Task<OAuthTokenStore?> GetTokenInfoAsync(Guid dataSourceId, CancellationToken ct = default);
}
