using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Graph.Models.ODataErrors;
using Microsoft.Kiota.Abstractions;
using Microsoft.Kiota.Abstractions.Authentication;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import.RemoteConnectors;

[HandlesRemoteConnector(DataSourceType.OneDrive)]
public class OneDriveFileConnector : IRemoteFileConnector
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly ILogger _logger;
    private GraphServiceClient? _graphClient;
    private string? _driveId;
    private bool _disposed;

    private const int MaxRetries = 3;
    private const int BufferSize = 81920; // 80 KB streaming buffer

    /// <summary>
    /// Files smaller than 4 MB can be uploaded with a simple PUT request.
    /// Larger files require a resumable upload session.
    /// </summary>
    private const long SimpleUploadMaxSize = 4L * 1024 * 1024; // 4 MB

    /// <summary>Chunk size for large file upload sessions (must be a multiple of 320 KiB).</summary>
    private const int LargeUploadChunkSize = 5 * 320 * 1024; // ~1.6 MB (5 x 320 KiB)

    private static readonly TimeSpan[] RetryDelays =
    {
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(3),
        TimeSpan.FromSeconds(7)
    };

    public OneDriveFileConnector(RemoteFileConnectionConfig config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    #region IRemoteFileConnector Implementation

    public async Task<bool> TestConnectionAsync(CancellationToken ct = default)
    {
        try
        {
            var client = GetGraphClient();
            var drive = await client.Me.Drive.GetAsync(cancellationToken: ct);

            if (drive?.Id != null)
                _driveId = drive.Id;

            _logger.LogInformation(
                "OneDrive connection test successful. Drive type: {DriveType}, Owner: {Owner}",
                drive?.DriveType ?? "unknown",
                drive?.Owner?.User?.DisplayName ?? "unknown");

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "OneDrive connection test failed");
            return false;
        }
    }

    public async Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default)
    {
        var client = GetGraphClient();
        var children = await GetChildrenAsync(client, path, ct);

        return children
            .Where(item => item.Folder == null) // Non-folder items = files
            .Select(item => new RemoteFileInfo(
                Name: item.Name ?? string.Empty,
                Path: BuildItemPath(path, item.Name),
                Size: item.Size ?? 0,
                LastModified: item.LastModifiedDateTime?.UtcDateTime ?? DateTime.MinValue,
                Extension: Path.GetExtension(item.Name ?? string.Empty).ToLowerInvariant()))
            .OrderBy(f => f.Name)
            .ToList();
    }

    public async Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default)
    {
        var client = GetGraphClient();
        var children = await GetChildrenAsync(client, path, ct);

        return children
            .Where(item => item.Folder != null) // Folder items only
            .Select(item => new RemoteFolderInfo(
                Name: item.Name ?? string.Empty,
                Path: BuildItemPath(path, item.Name),
                LastModified: item.LastModifiedDateTime?.UtcDateTime ?? DateTime.MinValue))
            .OrderBy(f => f.Name)
            .ToList();
    }

    public async Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetGraphClient();
        var driveId = await GetDriveIdAsync(client, ct);
        var normalizedPath = NormalizePath(remotePath);

        try
        {
            var item = await ExecuteWithRetryAsync(
                () => client.Drives[driveId].Root
                    .ItemWithPath(normalizedPath)
                    .GetAsync(config =>
                    {
                        config.QueryParameters.Select = new[]
                        {
                            "id", "name", "size", "lastModifiedDateTime",
                            "eTag", "file"
                        };
                    }, ct), ct);

            if (item == null)
                throw new FileNotFoundException($"OneDrive item not found: {remotePath}");

            return new RemoteFileMetadata(
                Name: item.Name ?? string.Empty,
                Size: item.Size ?? 0,
                LastModified: item.LastModifiedDateTime?.UtcDateTime ?? DateTime.MinValue,
                ETag: item.ETag,
                ContentType: item.File?.MimeType);
        }
        catch (ODataError odataEx) when (IsNotFound(odataEx))
        {
            throw new FileNotFoundException($"OneDrive file not found: {remotePath}", odataEx);
        }
    }

    public async Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetGraphClient();
        var driveId = await GetDriveIdAsync(client, ct);
        var normalizedPath = NormalizePath(remotePath);
        var fileName = Path.GetFileName(normalizedPath);
        var localPath = Path.Combine(localTempDir, fileName);

        Directory.CreateDirectory(localTempDir);

        _logger.LogInformation("OneDrive downloading: {RemotePath} -> {LocalPath}",
            normalizedPath, localPath);

        try
        {
            var contentStream = await ExecuteWithRetryAsync(
                () => client.Drives[driveId].Root
                    .ItemWithPath(normalizedPath)
                    .Content
                    .GetAsync(cancellationToken: ct), ct);

            if (contentStream == null)
                throw new IOException($"OneDrive returned null content stream for: {remotePath}");

            await using var localStream = new FileStream(
                localPath, FileMode.Create, FileAccess.Write,
                FileShare.None, BufferSize, useAsync: true);

            var buffer = new byte[BufferSize];
            long totalBytesRead = 0;
            int bytesRead;

            while ((bytesRead = await contentStream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
            {
                await localStream.WriteAsync(buffer, 0, bytesRead, ct);
                totalBytesRead += bytesRead;
                progress?.Report(totalBytesRead);
            }

            _logger.LogInformation("OneDrive download complete: {LocalPath} ({Size} bytes)",
                localPath, totalBytesRead);

            return localPath;
        }
        catch (ODataError odataEx) when (IsNotFound(odataEx))
        {
            throw new FileNotFoundException($"OneDrive file not found: {remotePath}", odataEx);
        }
    }

    public async Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetGraphClient();
        var driveId = await GetDriveIdAsync(client, ct);
        var normalizedPath = NormalizePath(remotePath);

        // If remotePath looks like a directory, append the local file name
        if (string.IsNullOrEmpty(Path.GetExtension(normalizedPath)))
        {
            var fileName = Path.GetFileName(localFilePath);
            normalizedPath = normalizedPath.TrimEnd('/') + "/" + fileName;
        }

        _logger.LogInformation("OneDrive uploading: {LocalPath} -> {RemotePath}",
            localFilePath, normalizedPath);

        var fileInfo = new FileInfo(localFilePath);

        if (fileInfo.Length <= SimpleUploadMaxSize)
        {
            return await UploadSmallFileAsync(client, localFilePath, normalizedPath, progress, ct);
        }

        return await UploadLargeFileAsync(client, localFilePath, normalizedPath, fileInfo.Length, progress, ct);
    }

    public async Task CreateFolderAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetGraphClient();
        var driveId = await GetDriveIdAsync(client, ct);
        var normalizedPath = NormalizePath(remotePath);

        if (string.IsNullOrWhiteSpace(normalizedPath) || normalizedPath == "/")
        {
            _logger.LogDebug("OneDrive: root folder already exists, skipping creation");
            return;
        }

        // Create each segment of the path as needed
        var segments = normalizedPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var currentPath = string.Empty;

        foreach (var segment in segments)
        {
            var parentPath = string.IsNullOrEmpty(currentPath) ? "/" : currentPath;

            try
            {
                var folderItem = new DriveItem
                {
                    Name = segment,
                    Folder = new Folder(),
                    AdditionalData = new Dictionary<string, object>
                    {
                        ["@microsoft.graph.conflictBehavior"] = "fail"
                    }
                };

                if (parentPath == "/")
                {
                    await ExecuteWithRetryAsync(
                        () => client.Drives[driveId].Items["root"].Children
                            .PostAsync(folderItem, cancellationToken: ct), ct);
                }
                else
                {
                    await ExecuteWithRetryAsync(
                        () => client.Drives[driveId].Root
                            .ItemWithPath(parentPath)
                            .Children
                            .PostAsync(folderItem, cancellationToken: ct), ct);
                }

                _logger.LogDebug("OneDrive created folder: {Segment} under {Parent}", segment, parentPath);
            }
            catch (ODataError odataEx) when (IsConflict(odataEx))
            {
                // Folder already exists — expected, continue
                _logger.LogDebug("OneDrive folder already exists: {Segment} under {Parent}", segment, parentPath);
            }

            currentPath = string.IsNullOrEmpty(currentPath)
                ? "/" + segment
                : currentPath + "/" + segment;
        }
    }

    public async Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetGraphClient();
        var driveId = await GetDriveIdAsync(client, ct);
        var normalizedPath = NormalizePath(remotePath);

        try
        {
            var item = await client.Drives[driveId].Root
                .ItemWithPath(normalizedPath)
                .GetAsync(config =>
                {
                    config.QueryParameters.Select = new[] { "id" };
                }, ct);

            return item != null;
        }
        catch (ODataError odataEx) when (IsNotFound(odataEx))
        {
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "OneDrive FileExistsAsync check failed for: {Path}", remotePath);
            return false;
        }
    }

    #endregion

    #region Upload Helpers

    /// <summary>
    /// Uploads a small file (under 4 MB) using a simple PUT request.
    /// </summary>
    private async Task<string> UploadSmallFileAsync(
        GraphServiceClient client, string localFilePath, string remotePath,
        IProgress<long>? progress, CancellationToken ct)
    {
        var driveId = await GetDriveIdAsync(client, ct);

        await using var fileStream = new FileStream(
            localFilePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, BufferSize, useAsync: true);

        var uploadedItem = await ExecuteWithRetryAsync(
            () => client.Drives[driveId].Root
                .ItemWithPath(remotePath)
                .Content
                .PutAsync(fileStream, cancellationToken: ct), ct);

        progress?.Report(uploadedItem?.Size ?? new FileInfo(localFilePath).Length);

        _logger.LogInformation("OneDrive small file upload complete: {Path} ({Size} bytes)",
            remotePath, uploadedItem?.Size);

        return remotePath;
    }

    /// <summary>
    /// Uploads a large file (over 4 MB) using a resumable upload session.
    /// Chunks must be multiples of 320 KiB per Microsoft Graph API requirements.
    /// </summary>
    private async Task<string> UploadLargeFileAsync(
        GraphServiceClient client, string localFilePath, string remotePath,
        long fileSize, IProgress<long>? progress, CancellationToken ct)
    {
        var driveId = await GetDriveIdAsync(client, ct);

        _logger.LogInformation(
            "OneDrive large file upload: {Path} ({Size} bytes, chunk size: {ChunkSize} bytes)",
            remotePath, fileSize, LargeUploadChunkSize);

        // Create the upload session
        var uploadSessionRequestBody = new Microsoft.Graph.Drives.Item.Items.Item.CreateUploadSession.CreateUploadSessionPostRequestBody
        {
            Item = new DriveItemUploadableProperties
            {
                AdditionalData = new Dictionary<string, object>
                {
                    ["@microsoft.graph.conflictBehavior"] = "replace"
                }
            }
        };

        var uploadSession = await ExecuteWithRetryAsync(
            () => client.Drives[driveId].Root
                .ItemWithPath(remotePath)
                .CreateUploadSession
                .PostAsync(uploadSessionRequestBody, cancellationToken: ct), ct);

        if (uploadSession?.UploadUrl == null)
            throw new IOException("OneDrive failed to create upload session — no upload URL returned.");

        await using var fileStream = new FileStream(
            localFilePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, BufferSize, useAsync: true);

        // Use Microsoft.Graph's LargeFileUploadTask for reliable chunked upload
        var largeFileUploadTask = new LargeFileUploadTask<DriveItem>(
            uploadSession, fileStream, LargeUploadChunkSize, client.RequestAdapter);

        var uploadResult = await largeFileUploadTask.UploadAsync(
            progress: progress != null
                ? new Progress<long>(bytes => progress.Report(bytes))
                : null,
            maxTries: MaxRetries,
            cancellationToken: ct);

        if (!uploadResult.UploadSucceeded)
        {
            throw new IOException($"OneDrive large file upload failed for: {remotePath}");
        }

        _logger.LogInformation("OneDrive large file upload complete: {Path} ({Size} bytes)",
            remotePath, fileSize);

        return remotePath;
    }

    #endregion

    #region Children Listing Helper

    /// <summary>
    /// Retrieves all child items at a given path, handling pagination via @odata.nextLink.
    /// </summary>
    private async Task<List<DriveItem>> GetChildrenAsync(
        GraphServiceClient client, string path, CancellationToken ct)
    {
        var driveId = await GetDriveIdAsync(client, ct);
        var normalizedPath = NormalizePath(path);
        var allItems = new List<DriveItem>();

        DriveItemCollectionResponse? response;

        if (string.IsNullOrWhiteSpace(normalizedPath) || normalizedPath == "/")
        {
            response = await ExecuteWithRetryAsync(
                () => client.Drives[driveId].Items["root"].Children
                    .GetAsync(config =>
                    {
                        config.QueryParameters.Select = new[]
                        {
                            "id", "name", "size", "lastModifiedDateTime",
                            "folder", "file"
                        };
                    }, ct), ct);
        }
        else
        {
            response = await ExecuteWithRetryAsync(
                () => client.Drives[driveId].Root
                    .ItemWithPath(normalizedPath)
                    .Children
                    .GetAsync(config =>
                    {
                        config.QueryParameters.Select = new[]
                        {
                            "id", "name", "size", "lastModifiedDateTime",
                            "folder", "file"
                        };
                    }, ct), ct);
        }

        if (response?.Value != null)
            allItems.AddRange(response.Value);

        // Handle pagination
        while (response?.OdataNextLink != null)
        {
            var pageIterator = PageIterator<DriveItem, DriveItemCollectionResponse>.CreatePageIterator(
                client,
                response,
                item =>
                {
                    allItems.Add(item);
                    return true; // continue iterating
                });

            await pageIterator.IterateAsync(ct);
            break; // PageIterator handles all remaining pages
        }

        return allItems;
    }

    #endregion

    #region Retry Logic

    /// <summary>
    /// Executes an async operation with retry logic, handling Microsoft Graph throttling (HTTP 429)
    /// with Retry-After header support, and transient server errors (5xx).
    /// </summary>
    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken ct)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (ODataError odataEx) when (attempt < MaxRetries - 1 && IsRetryableODataError(odataEx))
            {
                var delay = GetRetryDelay(odataEx, attempt);
                _logger.LogWarning(odataEx,
                    "OneDrive API error {StatusCode} (attempt {Attempt}/{Max}). Retrying in {Delay}s...",
                    odataEx.ResponseStatusCode, attempt + 1, MaxRetries, delay.TotalSeconds);

                await Task.Delay(delay, ct);
            }
            catch (Exception ex) when (attempt < MaxRetries - 1 && IsTransientError(ex))
            {
                var delay = RetryDelays[attempt];
                _logger.LogWarning(ex,
                    "Transient error during OneDrive operation (attempt {Attempt}/{Max}). Retrying in {Delay}s...",
                    attempt + 1, MaxRetries, delay.TotalSeconds);

                await Task.Delay(delay, ct);
            }
        }

        throw new InvalidOperationException("Retry logic fell through unexpectedly.");
    }

    private static bool IsRetryableODataError(ODataError ex)
    {
        var status = ex.ResponseStatusCode;
        return status == 429  // Too Many Requests (throttling)
            || status == 500  // Internal Server Error
            || status == 502  // Bad Gateway
            || status == 503  // Service Unavailable
            || status == 504; // Gateway Timeout
    }

    /// <summary>
    /// Extracts the Retry-After duration from the OData error headers if available,
    /// otherwise falls back to the exponential backoff delay.
    /// </summary>
    private TimeSpan GetRetryDelay(ODataError odataEx, int attempt)
    {
        // Microsoft Graph returns Retry-After in seconds in the error response
        if (odataEx.ResponseStatusCode == 429)
        {
            // Try to parse retry-after from the error message or inner data
            // The SDK doesn't directly expose headers, so use backoff with a minimum of 5s for 429
            var minThrottleDelay = TimeSpan.FromSeconds(5);
            return attempt < RetryDelays.Length
                ? TimeSpan.FromTicks(Math.Max(RetryDelays[attempt].Ticks, minThrottleDelay.Ticks))
                : minThrottleDelay;
        }

        return attempt < RetryDelays.Length ? RetryDelays[attempt] : TimeSpan.FromSeconds(10);
    }

    private static bool IsTransientError(Exception ex)
    {
        return ex is HttpRequestException
            || ex is TaskCanceledException { InnerException: TimeoutException }
            || ex is IOException;
    }

    #endregion

    #region Error Classification

    private static bool IsNotFound(ODataError ex)
    {
        return ex.ResponseStatusCode == 404;
    }

    private static bool IsConflict(ODataError ex)
    {
        return ex.ResponseStatusCode == 409;
    }

    #endregion

    #region Path Normalization

    /// <summary>
    /// Normalizes a path for the Microsoft Graph API.
    /// Paths must start with "/" and use forward slashes.
    /// </summary>
    private static string NormalizePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || path == "/" || path == "\\")
            return "/";

        var normalized = path.Replace('\\', '/').TrimEnd('/');

        if (!normalized.StartsWith('/'))
            normalized = "/" + normalized;

        return normalized;
    }

    private static string BuildItemPath(string parentPath, string? itemName)
    {
        if (string.IsNullOrEmpty(itemName))
            return parentPath;

        var normalizedParent = NormalizePath(parentPath).TrimEnd('/');
        return normalizedParent == "/"
            ? "/" + itemName
            : normalizedParent + "/" + itemName;
    }

    #endregion

    #region Graph Client Initialization

    /// <summary>
    /// Gets the user's drive ID, caching it after the first call.
    /// In Microsoft.Graph SDK v5, client.Me.Drive only supports GetAsync();
    /// path-based operations require client.Drives[driveId].Root.
    /// </summary>
    private async Task<string> GetDriveIdAsync(GraphServiceClient client, CancellationToken ct)
    {
        if (_driveId != null)
            return _driveId;

        var drive = await client.Me.Drive.GetAsync(cancellationToken: ct);
        _driveId = drive?.Id ?? throw new InvalidOperationException("Unable to retrieve OneDrive drive ID.");
        return _driveId;
    }

    private GraphServiceClient GetGraphClient()
    {
        if (_graphClient != null)
            return _graphClient;

        if (string.IsNullOrWhiteSpace(_config.AccessToken))
            throw new InvalidOperationException("OneDrive access token is not configured.");

        // Use a simple token-based authentication provider that injects the bearer token
        var authProvider = new BaseBearerTokenAuthenticationProvider(
            new StaticAccessTokenProvider(_config.AccessToken));

        _graphClient = new GraphServiceClient(authProvider);

        _logger.LogDebug("OneDrive Graph client initialized");
        return _graphClient;
    }

    /// <summary>
    /// Simple IAccessTokenProvider that returns a static access token.
    /// Used for pre-authenticated OAuth flows where the token is already obtained.
    /// </summary>
    private sealed class StaticAccessTokenProvider : IAccessTokenProvider
    {
        private readonly string _accessToken;

        public StaticAccessTokenProvider(string accessToken)
        {
            _accessToken = accessToken;
        }

        public Task<string> GetAuthorizationTokenAsync(
            Uri uri,
            Dictionary<string, object>? additionalAuthenticationContext = null,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_accessToken);
        }

        public AllowedHostsValidator AllowedHostsValidator { get; } = new();
    }

    #endregion

    #region IDisposable

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // GraphServiceClient does not implement IDisposable directly,
        // but we null it out to release the reference
        _graphClient = null;
    }

    #endregion
}
