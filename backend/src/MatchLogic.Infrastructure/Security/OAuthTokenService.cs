using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace MatchLogic.Infrastructure.Security;

public class OAuthTokenService : IOAuthTokenService
{
    private readonly IGenericRepository<OAuthTokenStore, Guid> _tokenRepository;
    private readonly IEncryptionService _encryptionService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<OAuthTokenService> _logger;
    private readonly HttpClient _httpClient;

    private const string CollectionName = "OAuthTokens";
    private const int TokenRefreshBufferSeconds = 300; // Refresh 5 min before expiry

    public OAuthTokenService(
        IGenericRepository<OAuthTokenStore, Guid> tokenRepository,
        IEncryptionService encryptionService,
        IConfiguration configuration,
        ILogger<OAuthTokenService> logger)
    {
        _tokenRepository = tokenRepository;
        _encryptionService = encryptionService;
        _configuration = configuration;
        _logger = logger;
        _httpClient = new HttpClient();
    }

    public async Task<string> GetValidAccessTokenAsync(Guid dataSourceId, CancellationToken ct = default)
    {
        var token = await _tokenRepository.GetByIdAsync(dataSourceId, CollectionName);
        if (token == null)
            throw new InvalidOperationException($"No OAuth tokens found for datasource: {dataSourceId}");

        var accessToken = await _encryptionService.DecryptAsync(token.AccessToken, dataSourceId.ToString());
        var refreshToken = await _encryptionService.DecryptAsync(token.RefreshToken, dataSourceId.ToString());

        // Check if access token is still valid
        if (token.AccessTokenExpiry > DateTime.UtcNow.AddSeconds(TokenRefreshBufferSeconds))
        {
            return accessToken;
        }

        // Token expired — refresh it
        _logger.LogInformation("OAuth access token expired for {DataSourceId}, refreshing...", dataSourceId);

        var newTokens = await RefreshAccessTokenAsync(token.Provider, refreshToken, ct);
        if (newTokens == null)
            throw new InvalidOperationException($"Failed to refresh OAuth token for datasource: {dataSourceId}");

        // Store updated tokens
        token.AccessToken = await _encryptionService.EncryptAsync(newTokens.AccessToken, dataSourceId.ToString());
        if (!string.IsNullOrEmpty(newTokens.RefreshToken))
        {
            // Some providers rotate refresh tokens
            token.RefreshToken = await _encryptionService.EncryptAsync(newTokens.RefreshToken, dataSourceId.ToString());
        }
        token.AccessTokenExpiry = DateTime.UtcNow.AddSeconds(newTokens.ExpiresInSeconds);

        await _tokenRepository.UpdateAsync(token, CollectionName);
        _logger.LogInformation("OAuth token refreshed for {DataSourceId}", dataSourceId);

        return newTokens.AccessToken;
    }

    public async Task StoreTokensAsync(Guid dataSourceId, DataSourceType provider, OAuthTokenResponse tokens, CancellationToken ct = default)
    {
        var encryptedAccessToken = await _encryptionService.EncryptAsync(tokens.AccessToken, dataSourceId.ToString());
        var encryptedRefreshToken = await _encryptionService.EncryptAsync(tokens.RefreshToken, dataSourceId.ToString());

        var existing = await _tokenRepository.GetByIdAsync(dataSourceId, CollectionName);
        if (existing != null)
        {
            existing.AccessToken = encryptedAccessToken;
            existing.RefreshToken = encryptedRefreshToken;
            existing.AccessTokenExpiry = DateTime.UtcNow.AddSeconds(tokens.ExpiresInSeconds);
            existing.Scopes = tokens.Scopes;
            existing.AccountEmail = tokens.AccountEmail;
            existing.AccountDisplayName = tokens.AccountDisplayName;
            await _tokenRepository.UpdateAsync(existing, CollectionName);
        }
        else
        {
            var tokenStore = new OAuthTokenStore
            {
                Id = dataSourceId,
                DataSourceId = dataSourceId,
                Provider = provider,
                AccessToken = encryptedAccessToken,
                RefreshToken = encryptedRefreshToken,
                AccessTokenExpiry = DateTime.UtcNow.AddSeconds(tokens.ExpiresInSeconds),
                Scopes = tokens.Scopes,
                AccountEmail = tokens.AccountEmail,
                AccountDisplayName = tokens.AccountDisplayName
            };
            await _tokenRepository.InsertAsync(tokenStore, CollectionName);
        }

        _logger.LogInformation("OAuth tokens stored for {DataSourceId} ({Provider})", dataSourceId, provider);
    }

    public async Task<bool> RevokeTokensAsync(Guid dataSourceId, CancellationToken ct = default)
    {
        var token = await _tokenRepository.GetByIdAsync(dataSourceId, CollectionName);
        if (token == null) return false;

        try
        {
            // Attempt provider-specific revocation
            var accessToken = await _encryptionService.DecryptAsync(token.AccessToken, dataSourceId.ToString());
            await RevokeProviderTokenAsync(token.Provider, accessToken, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Provider token revocation failed for {DataSourceId}, proceeding with local deletion", dataSourceId);
        }

        await _tokenRepository.DeleteAsync(dataSourceId, CollectionName);
        _logger.LogInformation("OAuth tokens revoked and deleted for {DataSourceId}", dataSourceId);
        return true;
    }

    public Task<string> GetAuthorizationUrlAsync(DataSourceType provider, string redirectUri, string state, CancellationToken ct = default)
    {
        var section = GetOAuthConfig(provider);
        var clientId = section["ClientId"];

        var url = provider switch
        {
            DataSourceType.GoogleDrive =>
                $"https://accounts.google.com/o/oauth2/v2/auth?" +
                $"client_id={Uri.EscapeDataString(clientId!)}" +
                $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
                $"&response_type=code" +
                $"&scope={Uri.EscapeDataString(section["Scopes"] ?? "https://www.googleapis.com/auth/drive.readonly")}" +
                $"&access_type=offline" +
                $"&prompt=consent" +
                $"&state={Uri.EscapeDataString(state)}",

            DataSourceType.Dropbox =>
                $"https://www.dropbox.com/oauth2/authorize?" +
                $"client_id={Uri.EscapeDataString(clientId!)}" +
                $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
                $"&response_type=code" +
                $"&token_access_type=offline" +
                $"&state={Uri.EscapeDataString(state)}",

            DataSourceType.OneDrive =>
                $"https://login.microsoftonline.com/{section["TenantId"] ?? "common"}/oauth2/v2.0/authorize?" +
                $"client_id={Uri.EscapeDataString(clientId!)}" +
                $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
                $"&response_type=code" +
                $"&scope={Uri.EscapeDataString(section["Scopes"] ?? "Files.ReadWrite offline_access")}" +
                $"&state={Uri.EscapeDataString(state)}",

            _ => throw new ArgumentException($"OAuth not supported for provider: {provider}")
        };

        return Task.FromResult(url);
    }

    public async Task<OAuthTokenResponse> ExchangeCodeAsync(DataSourceType provider, string code, string redirectUri, CancellationToken ct = default)
    {
        var section = GetOAuthConfig(provider);
        var clientId = section["ClientId"]!;
        var clientSecret = section["ClientSecret"]!;

        return provider switch
        {
            DataSourceType.GoogleDrive => await ExchangeGoogleCodeAsync(clientId, clientSecret, code, redirectUri, ct),
            DataSourceType.Dropbox => await ExchangeDropboxCodeAsync(clientId, clientSecret, code, redirectUri, ct),
            DataSourceType.OneDrive => await ExchangeOneDriveCodeAsync(section, clientId, clientSecret, code, redirectUri, ct),
            _ => throw new ArgumentException($"OAuth not supported for provider: {provider}")
        };
    }

    public async Task<bool> HasValidTokensAsync(Guid dataSourceId, CancellationToken ct = default)
    {
        var token = await _tokenRepository.GetByIdAsync(dataSourceId, CollectionName);
        return token != null && !string.IsNullOrEmpty(token.RefreshToken);
    }

    public async Task<OAuthTokenStore?> GetTokenInfoAsync(Guid dataSourceId, CancellationToken ct = default)
    {
        var token = await _tokenRepository.GetByIdAsync(dataSourceId, CollectionName);
        if (token == null) return null;

        // Return info without sensitive fields
        return new OAuthTokenStore
        {
            Id = token.Id,
            DataSourceId = token.DataSourceId,
            Provider = token.Provider,
            AccessToken = "[encrypted]",
            RefreshToken = "[encrypted]",
            AccessTokenExpiry = token.AccessTokenExpiry,
            Scopes = token.Scopes,
            AccountEmail = token.AccountEmail,
            AccountDisplayName = token.AccountDisplayName,
            CreatedAt = token.CreatedAt,
            ModifiedAt = token.ModifiedAt
        };
    }

    #region Private — Token Exchange

    private async Task<OAuthTokenResponse> ExchangeGoogleCodeAsync(
        string clientId, string clientSecret, string code, string redirectUri, CancellationToken ct)
    {
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["code"] = code,
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["redirect_uri"] = redirectUri,
            ["grant_type"] = "authorization_code"
        });

        var response = await _httpClient.PostAsync("https://oauth2.googleapis.com/token", content, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);

        return new OAuthTokenResponse
        {
            AccessToken = json.GetProperty("access_token").GetString()!,
            RefreshToken = json.TryGetProperty("refresh_token", out var rt) ? rt.GetString() ?? "" : "",
            ExpiresInSeconds = json.GetProperty("expires_in").GetInt32(),
            Scopes = json.TryGetProperty("scope", out var sc) ? sc.GetString() ?? "" : ""
        };
    }

    private async Task<OAuthTokenResponse> ExchangeDropboxCodeAsync(
        string clientId, string clientSecret, string code, string redirectUri, CancellationToken ct)
    {
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["code"] = code,
            ["grant_type"] = "authorization_code",
            ["redirect_uri"] = redirectUri
        });

        var request = new HttpRequestMessage(HttpMethod.Post, "https://api.dropboxapi.com/oauth2/token")
        {
            Content = content
        };
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}")));

        var response = await _httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);

        return new OAuthTokenResponse
        {
            AccessToken = json.GetProperty("access_token").GetString()!,
            RefreshToken = json.TryGetProperty("refresh_token", out var rt) ? rt.GetString() ?? "" : "",
            ExpiresInSeconds = json.TryGetProperty("expires_in", out var ei) ? ei.GetInt32() : 14400,
            AccountEmail = json.TryGetProperty("account_id", out var ai) ? ai.GetString() ?? "" : ""
        };
    }

    private async Task<OAuthTokenResponse> ExchangeOneDriveCodeAsync(
        IConfigurationSection section, string clientId, string clientSecret, string code, string redirectUri, CancellationToken ct)
    {
        var tenantId = section["TenantId"] ?? "common";
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["code"] = code,
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["redirect_uri"] = redirectUri,
            ["grant_type"] = "authorization_code",
            ["scope"] = section["Scopes"] ?? "Files.ReadWrite offline_access"
        });

        var response = await _httpClient.PostAsync(
            $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token", content, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);

        return new OAuthTokenResponse
        {
            AccessToken = json.GetProperty("access_token").GetString()!,
            RefreshToken = json.TryGetProperty("refresh_token", out var rt) ? rt.GetString() ?? "" : "",
            ExpiresInSeconds = json.TryGetProperty("expires_in", out var ei) ? ei.GetInt32() : 3600,
            Scopes = json.TryGetProperty("scope", out var sc) ? sc.GetString() ?? "" : ""
        };
    }

    #endregion

    #region Private — Token Refresh

    private async Task<OAuthTokenResponse?> RefreshAccessTokenAsync(DataSourceType provider, string refreshToken, CancellationToken ct)
    {
        var section = GetOAuthConfig(provider);
        var clientId = section["ClientId"]!;
        var clientSecret = section["ClientSecret"]!;

        try
        {
            return provider switch
            {
                DataSourceType.GoogleDrive => await RefreshGoogleTokenAsync(clientId, clientSecret, refreshToken, ct),
                DataSourceType.Dropbox => await RefreshDropboxTokenAsync(clientId, clientSecret, refreshToken, ct),
                DataSourceType.OneDrive => await RefreshOneDriveTokenAsync(section, clientId, clientSecret, refreshToken, ct),
                _ => null
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to refresh OAuth token for provider: {Provider}", provider);
            return null;
        }
    }

    private async Task<OAuthTokenResponse> RefreshGoogleTokenAsync(
        string clientId, string clientSecret, string refreshToken, CancellationToken ct)
    {
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["refresh_token"] = refreshToken,
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["grant_type"] = "refresh_token"
        });

        var response = await _httpClient.PostAsync("https://oauth2.googleapis.com/token", content, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);

        return new OAuthTokenResponse
        {
            AccessToken = json.GetProperty("access_token").GetString()!,
            RefreshToken = refreshToken, // Google doesn't rotate refresh tokens
            ExpiresInSeconds = json.GetProperty("expires_in").GetInt32()
        };
    }

    private async Task<OAuthTokenResponse> RefreshDropboxTokenAsync(
        string clientId, string clientSecret, string refreshToken, CancellationToken ct)
    {
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["refresh_token"] = refreshToken,
            ["grant_type"] = "refresh_token"
        });

        var request = new HttpRequestMessage(HttpMethod.Post, "https://api.dropboxapi.com/oauth2/token")
        {
            Content = content
        };
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}")));

        var response = await _httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);

        return new OAuthTokenResponse
        {
            AccessToken = json.GetProperty("access_token").GetString()!,
            RefreshToken = refreshToken,
            ExpiresInSeconds = json.TryGetProperty("expires_in", out var ei) ? ei.GetInt32() : 14400
        };
    }

    private async Task<OAuthTokenResponse> RefreshOneDriveTokenAsync(
        IConfigurationSection section, string clientId, string clientSecret, string refreshToken, CancellationToken ct)
    {
        var tenantId = section["TenantId"] ?? "common";
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["refresh_token"] = refreshToken,
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["grant_type"] = "refresh_token",
            ["scope"] = section["Scopes"] ?? "Files.ReadWrite offline_access"
        });

        var response = await _httpClient.PostAsync(
            $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token", content, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);

        return new OAuthTokenResponse
        {
            AccessToken = json.GetProperty("access_token").GetString()!,
            RefreshToken = json.TryGetProperty("refresh_token", out var rt) ? rt.GetString() ?? refreshToken : refreshToken,
            ExpiresInSeconds = json.TryGetProperty("expires_in", out var ei) ? ei.GetInt32() : 3600
        };
    }

    #endregion

    #region Private — Provider Revocation

    private async Task RevokeProviderTokenAsync(DataSourceType provider, string accessToken, CancellationToken ct)
    {
        switch (provider)
        {
            case DataSourceType.GoogleDrive:
                await _httpClient.PostAsync(
                    $"https://oauth2.googleapis.com/revoke?token={Uri.EscapeDataString(accessToken)}", null, ct);
                break;

            case DataSourceType.Dropbox:
                var request = new HttpRequestMessage(HttpMethod.Post, "https://api.dropboxapi.com/2/auth/token/revoke");
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);
                await _httpClient.SendAsync(request, ct);
                break;

            case DataSourceType.OneDrive:
                // Microsoft Graph doesn't support token revocation via API — tokens expire naturally
                _logger.LogDebug("OneDrive tokens will expire naturally (no revocation API)");
                break;
        }
    }

    #endregion

    private IConfigurationSection GetOAuthConfig(DataSourceType provider)
    {
        var providerName = provider switch
        {
            DataSourceType.GoogleDrive => "GoogleDrive",
            DataSourceType.Dropbox => "Dropbox",
            DataSourceType.OneDrive => "OneDrive",
            _ => throw new ArgumentException($"No OAuth config for provider: {provider}")
        };

        var section = _configuration.GetSection($"OAuth:{providerName}");
        if (!section.Exists())
            throw new InvalidOperationException($"OAuth configuration missing for {providerName}. Add OAuth:{providerName} section to appsettings.json");

        var clientId = section["ClientId"];
        var clientSecret = section["ClientSecret"];
        if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret))
            throw new InvalidOperationException(
                $"OAuth not configured for {providerName}. " +
                $"Please set ClientId and ClientSecret in the OAuth:{providerName} section of appsettings.json. " +
                $"Register your app at {GetRegistrationUrl(provider)} to obtain credentials.");

        return section;
    }

    private static string GetRegistrationUrl(DataSourceType provider) => provider switch
    {
        DataSourceType.GoogleDrive => "https://console.cloud.google.com/apis/credentials",
        DataSourceType.Dropbox => "https://www.dropbox.com/developers/apps",
        DataSourceType.OneDrive => "https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps",
        _ => "the provider's developer console"
    };
}
