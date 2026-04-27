using Dropbox.Api;
using Dropbox.Api.Files;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import.RemoteConnectors;

[HandlesRemoteConnector(DataSourceType.Dropbox)]
public class DropboxFileConnector : IRemoteFileConnector
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly ILogger _logger;
    private DropboxClient? _client;
    private bool _disposed;

    private const int MaxRetries = 3;
    private const int BufferSize = 81920; // 80 KB streaming buffer

    /// <summary>
    /// Dropbox chunk upload threshold. Files larger than this use session-based chunked upload.
    /// Dropbox recommends 150 MB as the boundary.
    /// </summary>
    private const long ChunkedUploadThreshold = 150L * 1024 * 1024; // 150 MB

    /// <summary>Chunk size for session-based uploads (8 MB per Dropbox recommendation).</summary>
    private const int UploadChunkSize = 8 * 1024 * 1024; // 8 MB

    private static readonly TimeSpan[] RetryDelays =
    {
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(3),
        TimeSpan.FromSeconds(7)
    };

    public DropboxFileConnector(RemoteFileConnectionConfig config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    #region IRemoteFileConnector Implementation

    public async Task<bool> TestConnectionAsync(CancellationToken ct = default)
    {
        try
        {
            var client = GetClient();
            var account = await client.Users.GetCurrentAccountAsync();

            _logger.LogInformation(
                "Dropbox connection test successful. Account: {DisplayName} ({Email})",
                account.Name?.DisplayName ?? "unknown",
                account.Email ?? "unknown");

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Dropbox connection test failed");
            return false;
        }
    }

    public async Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default)
    {
        var client = GetClient();
        var normalizedPath = NormalizePath(path);

        var entries = await ListAllEntriesAsync(client, normalizedPath);

        var files = entries
            .OfType<FileMetadata>()
            .Select(f => new RemoteFileInfo(
                Name: f.Name,
                Path: f.PathDisplay ?? f.PathLower ?? $"{normalizedPath}/{f.Name}",
                Size: (long)f.Size,
                LastModified: f.ServerModified,
                Extension: System.IO.Path.GetExtension(f.Name).ToLowerInvariant()))
            .OrderBy(f => f.Name)
            .ToList();

        return files;
    }

    public async Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default)
    {
        var client = GetClient();
        var normalizedPath = NormalizePath(path);

        var entries = await ListAllEntriesAsync(client, normalizedPath);

        var folders = entries
            .OfType<FolderMetadata>()
            .Select(f => new RemoteFolderInfo(
                Name: f.Name,
                Path: f.PathDisplay ?? f.PathLower ?? $"{normalizedPath}/{f.Name}",
                // Dropbox folder metadata does not include modification time
                LastModified: DateTime.MinValue))
            .OrderBy(f => f.Name)
            .ToList();

        return folders;
    }

    public async Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetClient();
        var normalizedPath = NormalizePath(remotePath);

        try
        {
            var metadata = await ExecuteWithRetryAsync(
                () => client.Files.GetMetadataAsync(normalizedPath), ct);

            if (metadata is FileMetadata fileMeta)
            {
                return new RemoteFileMetadata(
                    Name: fileMeta.Name,
                    Size: (long)fileMeta.Size,
                    LastModified: fileMeta.ServerModified,
                    ETag: fileMeta.Rev,
                    ContentType: null);
            }

            throw new InvalidOperationException(
                $"Path does not point to a file: {remotePath}");
        }
        catch (ApiException<GetMetadataError> ex)
            when (ex.ErrorResponse.IsPath && ex.ErrorResponse.AsPath.Value.IsNotFound)
        {
            throw new FileNotFoundException($"Dropbox file not found: {remotePath}", ex);
        }
    }

    public async Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetClient();
        var normalizedPath = NormalizePath(remotePath);
        var fileName = System.IO.Path.GetFileName(normalizedPath);
        var localPath = System.IO.Path.Combine(localTempDir, fileName);

        Directory.CreateDirectory(localTempDir);

        _logger.LogInformation("Dropbox downloading: {RemotePath} -> {LocalPath}",
            normalizedPath, localPath);

        var response = await ExecuteWithRetryAsync(
            () => client.Files.DownloadAsync(normalizedPath), ct);

        await using var remoteStream = await response.GetContentAsStreamAsync();
        await using var localStream = new FileStream(
            localPath, FileMode.Create, FileAccess.Write,
            FileShare.None, BufferSize, useAsync: true);

        var buffer = new byte[BufferSize];
        long totalBytesRead = 0;
        int bytesRead;

        while ((bytesRead = await remoteStream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
        {
            await localStream.WriteAsync(buffer, 0, bytesRead, ct);
            totalBytesRead += bytesRead;
            progress?.Report(totalBytesRead);
        }

        _logger.LogInformation("Dropbox download complete: {LocalPath} ({Size} bytes)",
            localPath, totalBytesRead);

        return localPath;
    }

    public async Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetClient();
        var normalizedPath = NormalizePath(remotePath);

        // If remotePath is a directory (no extension), append the file name
        if (string.IsNullOrEmpty(System.IO.Path.GetExtension(normalizedPath)))
        {
            var fileName = System.IO.Path.GetFileName(localFilePath);
            normalizedPath = normalizedPath.TrimEnd('/') + "/" + fileName;
        }

        _logger.LogInformation("Dropbox uploading: {LocalPath} -> {RemotePath}",
            localFilePath, normalizedPath);

        var fileInfo = new FileInfo(localFilePath);

        if (fileInfo.Length > ChunkedUploadThreshold)
        {
            return await UploadLargeFileAsync(client, localFilePath, normalizedPath, fileInfo.Length, progress, ct);
        }

        return await UploadSmallFileAsync(client, localFilePath, normalizedPath, progress, ct);
    }

    public async Task CreateFolderAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetClient();
        var normalizedPath = NormalizePath(remotePath);

        if (string.IsNullOrWhiteSpace(normalizedPath) || normalizedPath == "/")
        {
            _logger.LogDebug("Dropbox: root folder already exists, skipping creation");
            return;
        }

        try
        {
            await ExecuteWithRetryAsync(
                () => client.Files.CreateFolderV2Async(normalizedPath), ct);

            _logger.LogDebug("Dropbox created folder: {Path}", normalizedPath);
        }
        catch (ApiException<CreateFolderError> ex)
            when (ex.ErrorResponse.IsPath
                  && ex.ErrorResponse.AsPath.Value.IsConflict)
        {
            // Folder already exists — not an error
            _logger.LogDebug("Dropbox folder already exists: {Path}", normalizedPath);
        }
    }

    public async Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetClient();
        var normalizedPath = NormalizePath(remotePath);

        try
        {
            var metadata = await client.Files.GetMetadataAsync(normalizedPath);
            return metadata is FileMetadata;
        }
        catch (ApiException<GetMetadataError> ex)
            when (ex.ErrorResponse.IsPath && ex.ErrorResponse.AsPath.Value.IsNotFound)
        {
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Dropbox FileExistsAsync check failed for: {Path}", normalizedPath);
            return false;
        }
    }

    #endregion

    #region Upload Helpers

    private async Task<string> UploadSmallFileAsync(
        DropboxClient client, string localFilePath, string remotePath,
        IProgress<long>? progress, CancellationToken ct)
    {
        await using var fileStream = new FileStream(
            localFilePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, BufferSize, useAsync: true);

        var result = await ExecuteWithRetryAsync(
            () => client.Files.UploadAsync(
                remotePath,
                WriteMode.Overwrite.Instance,
                body: fileStream), ct);

        progress?.Report((long)result.Size);

        _logger.LogInformation("Dropbox upload complete: {Path} ({Size} bytes)",
            result.PathDisplay, result.Size);

        return result.PathDisplay ?? remotePath;
    }

    /// <summary>
    /// Uploads a large file using Dropbox's session-based chunked upload API.
    /// Splits the file into 8 MB chunks and uploads them sequentially.
    /// </summary>
    private async Task<string> UploadLargeFileAsync(
        DropboxClient client, string localFilePath, string remotePath,
        long fileSize, IProgress<long>? progress, CancellationToken ct)
    {
        _logger.LogInformation(
            "Dropbox large file upload: {Path} ({Size} bytes, chunk size: {ChunkSize} bytes)",
            remotePath, fileSize, UploadChunkSize);

        await using var fileStream = new FileStream(
            localFilePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, BufferSize, useAsync: true);

        // Start session
        var buffer = new byte[UploadChunkSize];
        int bytesRead = await fileStream.ReadAsync(buffer, 0, UploadChunkSize, ct);
        long totalUploaded = bytesRead;

        using var firstChunkStream = new MemoryStream(buffer, 0, bytesRead);
        var sessionStart = await ExecuteWithRetryAsync(
            () => client.Files.UploadSessionStartAsync(body: firstChunkStream), ct);

        var sessionId = sessionStart.SessionId;
        progress?.Report(totalUploaded);

        // Append remaining chunks
        while (totalUploaded < fileSize)
        {
            bytesRead = await fileStream.ReadAsync(buffer, 0, UploadChunkSize, ct);
            if (bytesRead <= 0) break;

            bool isLastChunk = (totalUploaded + bytesRead) >= fileSize;

            using var chunkStream = new MemoryStream(buffer, 0, bytesRead);

            if (isLastChunk)
            {
                // Finish the session with the last chunk
                var cursor = new UploadSessionCursor(sessionId, (ulong)totalUploaded);
                var commitInfo = new CommitInfo(
                    remotePath,
                    WriteMode.Overwrite.Instance);

                var result = await ExecuteWithRetryAsync(
                    () => client.Files.UploadSessionFinishAsync(
                        cursor, commitInfo, body: chunkStream), ct);

                totalUploaded += bytesRead;
                progress?.Report(totalUploaded);

                _logger.LogInformation(
                    "Dropbox large file upload complete: {Path} ({Size} bytes)",
                    result.PathDisplay, result.Size);

                return result.PathDisplay ?? remotePath;
            }
            else
            {
                var cursor = new UploadSessionCursor(sessionId, (ulong)totalUploaded);
                await ExecuteWithRetryAsync(async () =>
                {
                    await client.Files.UploadSessionAppendV2Async(cursor, body: chunkStream);
                    return true;
                }, ct);

                totalUploaded += bytesRead;
                progress?.Report(totalUploaded);
            }
        }

        // Should not reach here if the file was non-empty
        throw new IOException(
            $"Dropbox chunked upload finished unexpectedly for: {remotePath}");
    }

    #endregion

    #region Listing Helper

    /// <summary>
    /// Lists all entries (files and folders) at a given path, handling pagination via cursor.
    /// </summary>
    private async Task<List<Metadata>> ListAllEntriesAsync(
        DropboxClient client, string path)
    {
        var allEntries = new List<Metadata>();

        var listResult = await ExecuteWithRetryAsync(
            () => client.Files.ListFolderAsync(path), CancellationToken.None);

        allEntries.AddRange(listResult.Entries);

        while (listResult.HasMore)
        {
            listResult = await ExecuteWithRetryAsync(
                () => client.Files.ListFolderContinueAsync(listResult.Cursor),
                CancellationToken.None);
            allEntries.AddRange(listResult.Entries);
        }

        return allEntries;
    }

    #endregion

    #region Retry Logic

    /// <summary>
    /// Executes an async operation with retry logic, handling Dropbox rate limits (HTTP 429)
    /// and transient errors.
    /// </summary>
    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken ct)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (RateLimitException rlEx) when (attempt < MaxRetries - 1)
            {
                // Dropbox provides a Retry-After value in the exception
                var retryAfter = rlEx.RetryAfter > 0
                    ? TimeSpan.FromSeconds(rlEx.RetryAfter)
                    : RetryDelays[attempt];

                _logger.LogWarning(
                    "Dropbox rate limited (attempt {Attempt}/{Max}). Retrying in {Delay}s...",
                    attempt + 1, MaxRetries, retryAfter.TotalSeconds);

                await Task.Delay(retryAfter, ct);
            }
            catch (HttpException httpEx)
                when (attempt < MaxRetries - 1 && IsTransientHttpStatus(httpEx.StatusCode))
            {
                var delay = RetryDelays[attempt];
                _logger.LogWarning(httpEx,
                    "Dropbox transient HTTP error {StatusCode} (attempt {Attempt}/{Max}). Retrying in {Delay}s...",
                    httpEx.StatusCode, attempt + 1, MaxRetries, delay.TotalSeconds);

                await Task.Delay(delay, ct);
            }
            catch (Exception ex)
                when (attempt < MaxRetries - 1 && IsTransientError(ex))
            {
                var delay = RetryDelays[attempt];
                _logger.LogWarning(ex,
                    "Transient error during Dropbox operation (attempt {Attempt}/{Max}). Retrying in {Delay}s...",
                    attempt + 1, MaxRetries, delay.TotalSeconds);

                await Task.Delay(delay, ct);
            }
        }

        throw new InvalidOperationException("Retry logic fell through unexpectedly.");
    }

    private static bool IsTransientHttpStatus(int statusCode)
    {
        return statusCode == 429 // Rate limited
            || statusCode == 500 // Internal server error
            || statusCode == 502 // Bad gateway
            || statusCode == 503 // Service unavailable
            || statusCode == 504; // Gateway timeout
    }

    private static bool IsTransientError(Exception ex)
    {
        return ex is System.Net.Http.HttpRequestException
            || ex is TaskCanceledException { InnerException: TimeoutException }
            || ex is IOException;
    }

    #endregion

    #region Path Normalization

    /// <summary>
    /// Normalizes a path for the Dropbox API.
    /// Dropbox paths must start with "/" or be empty string (for root).
    /// </summary>
    private static string NormalizePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || path == "/" || path == "\\")
            return string.Empty; // Dropbox root is represented as empty string

        var normalized = path.Replace('\\', '/').TrimEnd('/');

        if (!normalized.StartsWith('/'))
            normalized = "/" + normalized;

        return normalized;
    }

    #endregion

    #region Client Initialization

    private DropboxClient GetClient()
    {
        if (_client != null)
            return _client;

        if (string.IsNullOrWhiteSpace(_config.AccessToken))
            throw new InvalidOperationException("Dropbox access token is not configured.");

        _client = new DropboxClient(_config.AccessToken);

        _logger.LogDebug("Dropbox client initialized");
        return _client;
    }

    #endregion

    #region IDisposable

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _client?.Dispose();
        _client = null;
    }

    #endregion
}
