using FluentFTP;
using FluentFTP.Exceptions;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import.RemoteConnectors;

[HandlesRemoteConnector(DataSourceType.FTP)]
public class FtpFileConnector : IRemoteFileConnector
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly ILogger _logger;
    private AsyncFtpClient? _client;
    private bool _disposed;

    private const int MaxRetries = 3;
    private static readonly TimeSpan[] RetryDelays = { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4) };

    public FtpFileConnector(RemoteFileConnectionConfig config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<bool> TestConnectionAsync(CancellationToken ct = default)
    {
        try
        {
            var client = await GetConnectedClientAsync(ct);
            // Use GetWorkingDirectory as a lightweight round-trip check.
            // GetReply() blocks when no reply is pending after Connect.
            await client.GetWorkingDirectory(ct);
            _logger.LogInformation("FTP connection test successful: {Host}:{Port}", _config.Host, _config.Port);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FTP connection test failed: {Host}:{Port}", _config.Host, _config.Port);
            return false;
        }
    }

    public async Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default)
    {
        var client = await GetConnectedClientAsync(ct);
        var items = await client.GetListing(NormalizePath(path), FtpListOption.Auto, ct);

        return items
            .Where(i => i.Type == FtpObjectType.File)
            .Select(i => new RemoteFileInfo(
                Name: i.Name,
                Path: i.FullName,
                Size: i.Size,
                LastModified: i.Modified,
                Extension: Path.GetExtension(i.Name).ToLowerInvariant()
            ))
            .OrderBy(f => f.Name)
            .ToList();
    }

    public async Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default)
    {
        var client = await GetConnectedClientAsync(ct);
        var items = await client.GetListing(NormalizePath(path), FtpListOption.Auto, ct);

        return items
            .Where(i => i.Type == FtpObjectType.Directory)
            .Select(i => new RemoteFolderInfo(
                Name: i.Name,
                Path: i.FullName,
                LastModified: i.Modified
            ))
            .OrderBy(f => f.Name)
            .ToList();
    }

    public async Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default)
    {
        var client = await GetConnectedClientAsync(ct);
        var info = await client.GetObjectInfo(NormalizePath(remotePath), true, ct);
        if (info == null)
            throw new FileNotFoundException($"Remote file not found: {remotePath}");

        return new RemoteFileMetadata(
            Name: info.Name,
            Size: info.Size,
            LastModified: info.Modified,
            ETag: null,
            ContentType: null
        );
    }

    public async Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = await GetConnectedClientAsync(ct);
        var normalizedPath = NormalizePath(remotePath);
        var fileName = Path.GetFileName(normalizedPath);
        var localPath = Path.Combine(localTempDir, fileName);

        _logger.LogInformation("FTP downloading: {RemotePath} → {LocalPath}", normalizedPath, localPath);

        var status = await RetryAsync(async () =>
        {
            return await client.DownloadFile(
                localPath, normalizedPath,
                FtpLocalExists.Overwrite,
                FtpVerify.None,
                progress != null ? new Progress<FtpProgress>(p => progress.Report((long)p.TransferredBytes)) : null,
                ct);
        }, ct);

        if (status == FtpStatus.Failed)
            throw new IOException($"FTP download failed for: {remotePath}");

        _logger.LogInformation("FTP download complete: {LocalPath} ({Size} bytes)",
            localPath, new FileInfo(localPath).Length);
        return localPath;
    }

    public async Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = await GetConnectedClientAsync(ct);
        var normalizedPath = NormalizePath(remotePath);

        _logger.LogInformation("FTP uploading: {LocalPath} → {RemotePath}", localFilePath, normalizedPath);

        var status = await RetryAsync(async () =>
        {
            return await client.UploadFile(
                localFilePath, normalizedPath,
                FtpRemoteExists.Overwrite,
                createRemoteDir: true,
                progress: progress != null ? new Progress<FtpProgress>(p => progress.Report((long)p.TransferredBytes)) : null,
                token: ct);
        }, ct);

        if (status == FtpStatus.Failed)
            throw new IOException($"FTP upload failed for: {remotePath}");

        _logger.LogInformation("FTP upload complete: {RemotePath}", normalizedPath);
        return normalizedPath;
    }

    public async Task CreateFolderAsync(string remotePath, CancellationToken ct = default)
    {
        var client = await GetConnectedClientAsync(ct);
        await client.CreateDirectory(NormalizePath(remotePath), true, ct);
    }

    public async Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        var client = await GetConnectedClientAsync(ct);
        return await client.FileExists(NormalizePath(remotePath), ct);
    }

    private async Task<AsyncFtpClient> GetConnectedClientAsync(CancellationToken ct)
    {
        if (_client != null && _client.IsConnected)
            return _client;

        _client?.Dispose();

        _client = new AsyncFtpClient(
            _config.Host,
            string.IsNullOrEmpty(_config.Username) ? "anonymous" : _config.Username,
            string.IsNullOrEmpty(_config.Password) ? "" : _config.Password,
            _config.Port);

        _client.Config.ConnectTimeout = _config.ConnectionTimeout * 1000;
        _client.Config.DataConnectionConnectTimeout = _config.ConnectionTimeout * 1000;
        _client.Config.DataConnectionReadTimeout = 30 * 60 * 1000; // 30 min for large files

        // Passive mode (recommended for most firewalls)
        _client.Config.DataConnectionType = _config.PassiveMode
            ? FtpDataConnectionType.AutoPassive
            : FtpDataConnectionType.AutoActive;

        // TLS/SSL configuration
        if (_config.UseTls)
        {
            _client.Config.EncryptionMode = FtpEncryptionMode.Explicit;
            _client.Config.SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13;
            _client.Config.ValidateAnyCertificate = true; // Allow self-signed in dev
        }

        _client.Config.RetryAttempts = MaxRetries;

        await _client.Connect(ct);
        _logger.LogInformation("FTP connected to {Host}:{Port}", _config.Host, _config.Port);
        return _client;
    }

    private async Task<T> RetryAsync<T>(Func<Task<T>> operation, CancellationToken ct)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (FtpException ex) when (attempt < MaxRetries - 1)
            {
                _logger.LogWarning(ex, "FTP operation failed (attempt {Attempt}/{Max}), retrying...",
                    attempt + 1, MaxRetries);
                await Task.Delay(RetryDelays[attempt], ct);

                // Reconnect if connection was lost
                if (_client != null && !_client.IsConnected)
                {
                    _client.Dispose();
                    _client = null;
                    await GetConnectedClientAsync(ct);
                }
            }
        }

        throw new InvalidOperationException("Retry logic fell through unexpectedly");
    }

    private static string NormalizePath(string path)
    {
        if (string.IsNullOrEmpty(path)) return "/";
        return path.Replace('\\', '/');
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _client?.Dispose();
        _client = null;
    }
}
