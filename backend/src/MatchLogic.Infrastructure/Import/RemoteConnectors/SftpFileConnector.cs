using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import.RemoteConnectors;

[HandlesRemoteConnector(DataSourceType.SFTP)]
public class SftpFileConnector : IRemoteFileConnector
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly ILogger _logger;
    private SftpClient? _client;
    private bool _disposed;

    private const int MaxRetries = 3;
    private const int BufferSize = 81920; // 80 KB streaming buffer

    public SftpFileConnector(RemoteFileConnectionConfig config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task<bool> TestConnectionAsync(CancellationToken ct = default)
    {
        try
        {
            var client = GetConnectedClient();
            _logger.LogInformation("SFTP connection test successful: {Host}:{Port}", _config.Host, _config.Port);
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SFTP connection test failed: {Host}:{Port}", _config.Host, _config.Port);
            return Task.FromResult(false);
        }
    }

    public Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default)
    {
        var client = GetConnectedClient();
        var normalizedPath = NormalizePath(path);
        var items = client.ListDirectory(normalizedPath);

        var files = items
            .Where(i => i.IsRegularFile)
            .Select(i => new RemoteFileInfo(
                Name: i.Name,
                Path: i.FullName,
                Size: i.Length,
                LastModified: i.LastWriteTimeUtc,
                Extension: Path.GetExtension(i.Name).ToLowerInvariant()
            ))
            .OrderBy(f => f.Name)
            .ToList();

        return Task.FromResult(files);
    }

    public Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default)
    {
        var client = GetConnectedClient();
        var normalizedPath = NormalizePath(path);
        var items = client.ListDirectory(normalizedPath);

        var folders = items
            .Where(i => i.IsDirectory && i.Name != "." && i.Name != "..")
            .Select(i => new RemoteFolderInfo(
                Name: i.Name,
                Path: i.FullName,
                LastModified: i.LastWriteTimeUtc
            ))
            .OrderBy(f => f.Name)
            .ToList();

        return Task.FromResult(folders);
    }

    public Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetConnectedClient();
        var normalizedPath = NormalizePath(remotePath);

        if (!client.Exists(normalizedPath))
            throw new FileNotFoundException($"Remote file not found: {remotePath}");

        var attrs = client.GetAttributes(normalizedPath);
        return Task.FromResult(new RemoteFileMetadata(
            Name: Path.GetFileName(normalizedPath),
            Size: attrs.Size,
            LastModified: attrs.LastWriteTimeUtc,
            ETag: null,
            ContentType: null
        ));
    }

    public async Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetConnectedClient();
        var normalizedPath = NormalizePath(remotePath);
        var fileName = Path.GetFileName(normalizedPath);
        var localPath = Path.Combine(localTempDir, fileName);

        _logger.LogInformation("SFTP downloading: {RemotePath} → {LocalPath}", normalizedPath, localPath);

        await using var localStream = new FileStream(localPath, FileMode.Create, FileAccess.Write,
            FileShare.None, BufferSize, useAsync: true);

        long totalBytesRead = 0;
        using var remoteStream = client.OpenRead(normalizedPath);
        var buffer = new byte[BufferSize];

        int bytesRead;
        while ((bytesRead = await remoteStream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
        {
            await localStream.WriteAsync(buffer, 0, bytesRead, ct);
            totalBytesRead += bytesRead;
            progress?.Report(totalBytesRead);
        }

        _logger.LogInformation("SFTP download complete: {LocalPath} ({Size} bytes)", localPath, totalBytesRead);
        return localPath;
    }

    public async Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetConnectedClient();
        var normalizedPath = NormalizePath(remotePath);

        _logger.LogInformation("SFTP uploading: {LocalPath} → {RemotePath}", localFilePath, normalizedPath);

        // Ensure parent directory exists
        var parentDir = Path.GetDirectoryName(normalizedPath)?.Replace('\\', '/');
        if (!string.IsNullOrEmpty(parentDir) && parentDir != "/" && !client.Exists(parentDir))
        {
            client.CreateDirectory(parentDir);
        }

        await using var localStream = new FileStream(localFilePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, BufferSize, useAsync: true);

        using var remoteStream = client.Create(normalizedPath);
        var buffer = new byte[BufferSize];
        long totalBytesWritten = 0;

        int bytesRead;
        while ((bytesRead = await localStream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
        {
            remoteStream.Write(buffer, 0, bytesRead);
            totalBytesWritten += bytesRead;
            progress?.Report(totalBytesWritten);
        }

        remoteStream.Flush();
        _logger.LogInformation("SFTP upload complete: {RemotePath} ({Size} bytes)", normalizedPath, totalBytesWritten);
        return normalizedPath;
    }

    public Task CreateFolderAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetConnectedClient();
        var normalizedPath = NormalizePath(remotePath);

        if (!client.Exists(normalizedPath))
        {
            client.CreateDirectory(normalizedPath);
            _logger.LogDebug("SFTP created directory: {Path}", normalizedPath);
        }

        return Task.CompletedTask;
    }

    public Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetConnectedClient();
        var normalizedPath = NormalizePath(remotePath);
        return Task.FromResult(client.Exists(normalizedPath));
    }

    private SftpClient GetConnectedClient()
    {
        if (_client != null && _client.IsConnected)
            return _client;

        _client?.Dispose();

        var authMethods = BuildAuthMethods();
        var connectionInfo = new Renci.SshNet.ConnectionInfo(
            _config.Host,
            _config.Port,
            string.IsNullOrEmpty(_config.Username) ? "anonymous" : _config.Username,
            authMethods.ToArray())
        {
            Timeout = TimeSpan.FromSeconds(_config.ConnectionTimeout)
        };

        _client = new SftpClient(connectionInfo);
        _client.OperationTimeout = TimeSpan.FromMinutes(30); // 30 min for large files
        _client.BufferSize = (uint)BufferSize;

        // Host key verification
        if (!string.IsNullOrEmpty(_config.HostFingerprint))
        {
            _client.HostKeyReceived += (sender, e) =>
            {
                var receivedFingerprint = BitConverter.ToString(e.FingerPrint).Replace("-", ":");
                e.CanTrust = string.Equals(receivedFingerprint, _config.HostFingerprint,
                    StringComparison.OrdinalIgnoreCase);

                if (!e.CanTrust)
                    _logger.LogWarning("SFTP host key mismatch. Expected: {Expected}, Got: {Got}",
                        _config.HostFingerprint, receivedFingerprint);
            };
        }

        _client.Connect();
        _logger.LogInformation("SFTP connected to {Host}:{Port}", _config.Host, _config.Port);
        return _client;
    }

    private List<AuthenticationMethod> BuildAuthMethods()
    {
        var methods = new List<AuthenticationMethod>();

        // Private key authentication
        if (!string.IsNullOrEmpty(_config.PrivateKey))
        {
            try
            {
                PrivateKeyFile keyFile;
                if (File.Exists(_config.PrivateKey))
                {
                    keyFile = string.IsNullOrEmpty(_config.PrivateKeyPassphrase)
                        ? new PrivateKeyFile(_config.PrivateKey)
                        : new PrivateKeyFile(_config.PrivateKey, _config.PrivateKeyPassphrase);
                }
                else
                {
                    // Assume it's the key content itself (stored encrypted in Parameters)
                    var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(_config.PrivateKey));
                    keyFile = string.IsNullOrEmpty(_config.PrivateKeyPassphrase)
                        ? new PrivateKeyFile(keyStream)
                        : new PrivateKeyFile(keyStream, _config.PrivateKeyPassphrase);
                }
                methods.Add(new PrivateKeyAuthenticationMethod(_config.Username, keyFile));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load private key, falling back to password auth");
            }
        }

        // Password authentication
        if (!string.IsNullOrEmpty(_config.Password))
        {
            methods.Add(new PasswordAuthenticationMethod(_config.Username, _config.Password));
        }

        if (methods.Count == 0)
            throw new ArgumentException("No authentication method configured for SFTP. Provide either a password or private key.");

        return methods;
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
