using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
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

[HandlesRemoteConnector(DataSourceType.AzureBlob)]
public class AzureBlobFileConnector : IRemoteFileConnector
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly ILogger _logger;
    private BlobServiceClient? _serviceClient;
    private BlobContainerClient? _containerClient;
    private bool _disposed;

    private const int MaxRetries = 3;
    private static readonly TimeSpan[] RetryDelays = { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4) };

    public AzureBlobFileConnector(RemoteFileConnectionConfig config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<bool> TestConnectionAsync(CancellationToken ct = default)
    {
        try
        {
            var container = GetContainerClient();
            var properties = await container.GetPropertiesAsync(cancellationToken: ct);
            _logger.LogInformation("Azure Blob connection test successful: container {Container}, account {Account}",
                _config.ContainerName, _config.AccountName);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Azure Blob connection test failed: container {Container}", _config.ContainerName);
            return false;
        }
    }

    public async Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default)
    {
        var container = GetContainerClient();
        var prefix = NormalizePrefix(path);
        var files = new List<RemoteFileInfo>();

        await foreach (var blobItem in container.GetBlobsAsync(
            prefix: string.IsNullOrEmpty(prefix) ? null : prefix,
            cancellationToken: ct))
        {
            // Skip folder markers (zero-byte blobs ending with /)
            if (blobItem.Name.EndsWith("/"))
                continue;

            // If a prefix is specified, only include direct children (not nested blobs)
            if (!string.IsNullOrEmpty(prefix))
            {
                var relativePath = blobItem.Name[prefix.Length..];
                if (relativePath.Contains('/'))
                    continue; // This blob is in a subfolder
            }

            files.Add(new RemoteFileInfo(
                Name: GetBlobName(blobItem.Name),
                Path: blobItem.Name,
                Size: blobItem.Properties.ContentLength ?? 0,
                LastModified: blobItem.Properties.LastModified?.UtcDateTime ?? DateTime.MinValue,
                Extension: Path.GetExtension(blobItem.Name).ToLowerInvariant()
            ));
        }

        _logger.LogInformation("Azure Blob listed {Count} files at prefix {Prefix} in container {Container}",
            files.Count, prefix, _config.ContainerName);

        return files.OrderBy(f => f.Name).ToList();
    }

    public async Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default)
    {
        var container = GetContainerClient();
        var prefix = NormalizePrefix(path);
        var folders = new List<RemoteFolderInfo>();

        await foreach (var hierarchyItem in container.GetBlobsByHierarchyAsync(
            delimiter: "/",
            prefix: string.IsNullOrEmpty(prefix) ? null : prefix,
            cancellationToken: ct))
        {
            if (hierarchyItem.IsPrefix)
            {
                folders.Add(new RemoteFolderInfo(
                    Name: GetFolderName(hierarchyItem.Prefix),
                    Path: hierarchyItem.Prefix,
                    LastModified: DateTime.MinValue // Virtual folders have no timestamp
                ));
            }
        }

        _logger.LogInformation("Azure Blob listed {Count} folders at prefix {Prefix} in container {Container}",
            folders.Count, prefix, _config.ContainerName);

        return folders.OrderBy(f => f.Name).ToList();
    }

    public async Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default)
    {
        var container = GetContainerClient();
        var blobName = NormalizeBlobName(remotePath);
        var blobClient = container.GetBlobClient(blobName);

        try
        {
            var properties = await RetryAsync(
                async () => await blobClient.GetPropertiesAsync(cancellationToken: ct), ct);

            return new RemoteFileMetadata(
                Name: GetBlobName(blobName),
                Size: properties.Value.ContentLength,
                LastModified: properties.Value.LastModified.UtcDateTime,
                ETag: properties.Value.ETag.ToString(),
                ContentType: properties.Value.ContentType
            );
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            throw new FileNotFoundException($"Azure blob not found: {_config.ContainerName}/{blobName}", blobName);
        }
    }

    public async Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var container = GetContainerClient();
        var blobName = NormalizeBlobName(remotePath);
        var blobClient = container.GetBlobClient(blobName);
        var fileName = GetBlobName(blobName);
        var localPath = Path.Combine(localTempDir, fileName);

        Directory.CreateDirectory(localTempDir);

        _logger.LogInformation("Azure Blob downloading: {Container}/{BlobName} -> {LocalPath}",
            _config.ContainerName, blobName, localPath);

        await RetryAsync(async () =>
        {
            if (progress != null)
            {
                // Stream download with progress tracking
                var properties = await blobClient.GetPropertiesAsync(cancellationToken: ct);
                var totalSize = properties.Value.ContentLength;

                using var downloadStream = await blobClient.OpenReadAsync(cancellationToken: ct);
                using var fileStream = File.Create(localPath);

                var buffer = new byte[81920]; // 80KB buffer
                long totalBytesRead = 0;
                int bytesRead;

                while ((bytesRead = await downloadStream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
                {
                    await fileStream.WriteAsync(buffer, 0, bytesRead, ct);
                    totalBytesRead += bytesRead;
                    progress.Report(totalBytesRead);
                }
            }
            else
            {
                await blobClient.DownloadToAsync(localPath, ct);
            }

            return true;
        }, ct);

        var fileSize = new FileInfo(localPath).Length;
        _logger.LogInformation("Azure Blob download complete: {LocalPath} ({Size} bytes)", localPath, fileSize);

        return localPath;
    }

    public async Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var container = GetContainerClient();
        var blobName = NormalizeBlobName(remotePath);
        var blobClient = container.GetBlobClient(blobName);

        _logger.LogInformation("Azure Blob uploading: {LocalPath} -> {Container}/{BlobName}",
            localFilePath, _config.ContainerName, blobName);

        await RetryAsync(async () =>
        {
            var uploadOptions = new BlobUploadOptions
            {
                TransferOptions = new Azure.Storage.StorageTransferOptions
                {
                    MaximumConcurrency = 4,
                    MaximumTransferSize = 8 * 1024 * 1024 // 8MB per transfer
                }
            };

            if (progress != null)
            {
                uploadOptions.ProgressHandler = new Progress<long>(bytesTransferred =>
                {
                    progress.Report(bytesTransferred);
                });
            }

            using var fileStream = File.OpenRead(localFilePath);
            await blobClient.UploadAsync(fileStream, uploadOptions, ct);
            return true;
        }, ct);

        _logger.LogInformation("Azure Blob upload complete: {Container}/{BlobName}",
            _config.ContainerName, blobName);

        return $"https://{_config.AccountName}.blob.core.windows.net/{_config.ContainerName}/{blobName}";
    }

    public Task CreateFolderAsync(string remotePath, CancellationToken ct = default)
    {
        // Azure Blob Storage uses virtual folders — no explicit folder creation needed.
        // Folders are inferred from blob name prefixes (e.g., "folder1/folder2/file.csv").
        _logger.LogInformation("Azure Blob CreateFolderAsync: no-op (virtual folders). Path: {Path}", remotePath);
        return Task.CompletedTask;
    }

    public async Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        var container = GetContainerClient();
        var blobName = NormalizeBlobName(remotePath);
        var blobClient = container.GetBlobClient(blobName);

        try
        {
            var response = await blobClient.ExistsAsync(ct);
            return response.Value;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Azure Blob FileExistsAsync error for blob {BlobName} in container {Container}",
                blobName, _config.ContainerName);
            return false;
        }
    }

    private BlobContainerClient GetContainerClient()
    {
        if (_containerClient != null)
            return _containerClient;

        _serviceClient = CreateServiceClient();
        _containerClient = _serviceClient.GetBlobContainerClient(_config.ContainerName);

        _logger.LogInformation("Azure Blob client created for container {Container} with auth mode {AuthMode}",
            _config.ContainerName, _config.AzureAuthMode);

        return _containerClient;
    }

    private BlobServiceClient CreateServiceClient()
    {
        var authMode = _config.AzureAuthMode.ToLowerInvariant();

        return authMode switch
        {
            "connectionstring" => CreateFromConnectionString(),
            "accountkey" => CreateFromAccountKey(),
            "sastoken" => CreateFromSasToken(),
            _ => throw new ArgumentException(
                $"Unsupported Azure auth mode: {_config.AzureAuthMode}. Supported modes: connectionstring, accountkey, sastoken")
        };
    }

    private BlobServiceClient CreateFromConnectionString()
    {
        if (string.IsNullOrEmpty(_config.AzureConnectionString))
            throw new ArgumentException("Azure connection string is required for 'connectionstring' auth mode");

        return new BlobServiceClient(_config.AzureConnectionString);
    }

    private BlobServiceClient CreateFromAccountKey()
    {
        if (string.IsNullOrEmpty(_config.AccountName))
            throw new ArgumentException("Account name is required for 'accountkey' auth mode");
        if (string.IsNullOrEmpty(_config.AccountKey))
            throw new ArgumentException("Account key is required for 'accountkey' auth mode");

        var serviceUri = new Uri($"https://{_config.AccountName}.blob.core.windows.net");
        var credential = new StorageSharedKeyCredential(_config.AccountName, _config.AccountKey);
        return new BlobServiceClient(serviceUri, credential);
    }

    private BlobServiceClient CreateFromSasToken()
    {
        if (string.IsNullOrEmpty(_config.AccountName))
            throw new ArgumentException("Account name is required for 'sastoken' auth mode");
        if (string.IsNullOrEmpty(_config.SasToken))
            throw new ArgumentException("SAS token is required for 'sastoken' auth mode");

        var sasToken = _config.SasToken.StartsWith("?") ? _config.SasToken : $"?{_config.SasToken}";
        var serviceUri = new Uri($"https://{_config.AccountName}.blob.core.windows.net{sasToken}");
        return new BlobServiceClient(serviceUri);
    }

    private async Task<T> RetryAsync<T>(Func<Task<T>> operation, CancellationToken ct)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (RequestFailedException ex) when (attempt < MaxRetries - 1 && IsRetryable(ex))
            {
                _logger.LogWarning(ex, "Azure Blob operation failed (attempt {Attempt}/{Max}), retrying...",
                    attempt + 1, MaxRetries);
                await Task.Delay(RetryDelays[attempt], ct);
            }
            catch (IOException ex) when (attempt < MaxRetries - 1)
            {
                _logger.LogWarning(ex, "Azure Blob I/O error (attempt {Attempt}/{Max}), retrying...",
                    attempt + 1, MaxRetries);
                await Task.Delay(RetryDelays[attempt], ct);
            }
        }

        throw new InvalidOperationException("Retry logic fell through unexpectedly");
    }

    private static bool IsRetryable(RequestFailedException ex)
    {
        // Retry on transient Azure errors: 408, 429, 500, 502, 503, 504
        return ex.Status == 408  // Request Timeout
            || ex.Status == 429  // Too Many Requests
            || ex.Status == 500  // Internal Server Error
            || ex.Status == 502  // Bad Gateway
            || ex.Status == 503  // Service Unavailable
            || ex.Status == 504; // Gateway Timeout
    }

    /// <summary>
    /// Normalizes a path to an Azure Blob prefix for listing. Ensures trailing slash, no leading slash.
    /// </summary>
    private static string NormalizePrefix(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || path == "/" || path == "\\")
            return string.Empty;

        var normalized = path.Replace('\\', '/').TrimStart('/');

        if (!normalized.EndsWith("/"))
            normalized += "/";

        return normalized;
    }

    /// <summary>
    /// Normalizes a path to an Azure Blob name. No leading slash.
    /// </summary>
    private static string NormalizeBlobName(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Blob name cannot be empty", nameof(path));

        return path.Replace('\\', '/').TrimStart('/');
    }

    /// <summary>Extracts the blob name (file name) from a full blob path.</summary>
    private static string GetBlobName(string blobPath)
    {
        if (string.IsNullOrEmpty(blobPath)) return string.Empty;
        var trimmed = blobPath.TrimEnd('/');
        var lastSlash = trimmed.LastIndexOf('/');
        return lastSlash >= 0 ? trimmed[(lastSlash + 1)..] : trimmed;
    }

    /// <summary>Extracts the folder name from an Azure Blob hierarchy prefix.</summary>
    private static string GetFolderName(string prefix)
    {
        if (string.IsNullOrEmpty(prefix)) return string.Empty;
        var trimmed = prefix.TrimEnd('/');
        var lastSlash = trimmed.LastIndexOf('/');
        return lastSlash >= 0 ? trimmed[(lastSlash + 1)..] : trimmed;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // BlobServiceClient and BlobContainerClient do not implement IDisposable,
        // but we null them out to release references.
        _containerClient = null;
        _serviceClient = null;
    }
}
