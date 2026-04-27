using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
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

[HandlesRemoteConnector(DataSourceType.S3)]
public class S3FileConnector : IRemoteFileConnector
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly ILogger _logger;
    private AmazonS3Client? _client;
    private bool _disposed;

    private const int MaxRetries = 3;
    private static readonly TimeSpan[] RetryDelays = { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4) };

    public S3FileConnector(RemoteFileConnectionConfig config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<bool> TestConnectionAsync(CancellationToken ct = default)
    {
        try
        {
            var client = GetClient();
            var bucketName = _config.BucketName;

            // If a specific bucket is configured, verify access to that bucket
            if (!string.IsNullOrEmpty(bucketName))
            {
                var request = new GetBucketLocationRequest { BucketName = bucketName };
                await client.GetBucketLocationAsync(request, ct);
                _logger.LogInformation("S3 connection test successful: bucket {Bucket} accessible", bucketName);
            }
            else
            {
                // Fallback: list buckets to verify credentials
                await client.ListBucketsAsync(ct);
                _logger.LogInformation("S3 connection test successful: credentials valid");
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "S3 connection test failed for bucket {Bucket}", _config.BucketName);
            return false;
        }
    }

    public async Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default)
    {
        var client = GetClient();
        var prefix = NormalizePrefix(path);
        var files = new List<RemoteFileInfo>();

        var request = new ListObjectsV2Request
        {
            BucketName = _config.BucketName,
            Prefix = prefix,
            Delimiter = "/"
        };

        ListObjectsV2Response response;
        do
        {
            response = await RetryAsync(async () => await client.ListObjectsV2Async(request, ct), ct);

            var pageFiles = response.S3Objects
                .Where(obj => obj.Key != prefix) // Exclude the "folder" object itself
                .Where(obj => !obj.Key.EndsWith("/")) // Exclude folder markers
                .Select(obj => new RemoteFileInfo(
                    Name: GetFileName(obj.Key),
                    Path: obj.Key,
                    Size: obj.Size,
                    LastModified: obj.LastModified,
                    Extension: Path.GetExtension(obj.Key).ToLowerInvariant()
                ));

            files.AddRange(pageFiles);
            request.ContinuationToken = response.NextContinuationToken;

        } while (response.IsTruncated);

        _logger.LogInformation("S3 listed {Count} files at prefix {Prefix} in bucket {Bucket}",
            files.Count, prefix, _config.BucketName);

        return files.OrderBy(f => f.Name).ToList();
    }

    public async Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default)
    {
        var client = GetClient();
        var prefix = NormalizePrefix(path);
        var folders = new List<RemoteFolderInfo>();

        var request = new ListObjectsV2Request
        {
            BucketName = _config.BucketName,
            Prefix = prefix,
            Delimiter = "/"
        };

        ListObjectsV2Response response;
        do
        {
            response = await RetryAsync(async () => await client.ListObjectsV2Async(request, ct), ct);

            var pageFolders = response.CommonPrefixes
                .Select(cp => new RemoteFolderInfo(
                    Name: GetFolderName(cp),
                    Path: cp,
                    LastModified: DateTime.MinValue // S3 CommonPrefixes don't have timestamps
                ));

            folders.AddRange(pageFolders);
            request.ContinuationToken = response.NextContinuationToken;

        } while (response.IsTruncated);

        _logger.LogInformation("S3 listed {Count} folders at prefix {Prefix} in bucket {Bucket}",
            folders.Count, prefix, _config.BucketName);

        return folders.OrderBy(f => f.Name).ToList();
    }

    public async Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetClient();
        var key = NormalizeKey(remotePath);

        try
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = _config.BucketName,
                Key = key
            };

            var metadata = await RetryAsync(async () => await client.GetObjectMetadataAsync(request, ct), ct);

            return new RemoteFileMetadata(
                Name: GetFileName(key),
                Size: metadata.ContentLength,
                LastModified: metadata.LastModified,
                ETag: metadata.ETag,
                ContentType: metadata.Headers.ContentType
            );
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new FileNotFoundException($"S3 object not found: s3://{_config.BucketName}/{key}", key);
        }
    }

    public async Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetClient();
        var key = NormalizeKey(remotePath);
        var fileName = GetFileName(key);
        var localPath = Path.Combine(localTempDir, fileName);

        Directory.CreateDirectory(localTempDir);

        _logger.LogInformation("S3 downloading: s3://{Bucket}/{Key} -> {LocalPath}",
            _config.BucketName, key, localPath);

        await RetryAsync(async () =>
        {
            using var transferUtility = new TransferUtility(client);
            var request = new TransferUtilityDownloadRequest
            {
                BucketName = _config.BucketName,
                Key = key,
                FilePath = localPath
            };

            if (progress != null)
            {
                request.WriteObjectProgressEvent += (sender, args) =>
                {
                    progress.Report(args.TransferredBytes);
                };
            }

            await transferUtility.DownloadAsync(request, ct);
            return true; // Return value to satisfy RetryAsync<T>
        }, ct);

        var fileSize = new FileInfo(localPath).Length;
        _logger.LogInformation("S3 download complete: {LocalPath} ({Size} bytes)", localPath, fileSize);

        return localPath;
    }

    public async Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var client = GetClient();
        var key = NormalizeKey(remotePath);

        _logger.LogInformation("S3 uploading: {LocalPath} -> s3://{Bucket}/{Key}",
            localFilePath, _config.BucketName, key);

        await RetryAsync(async () =>
        {
            using var transferUtility = new TransferUtility(client);
            var request = new TransferUtilityUploadRequest
            {
                BucketName = _config.BucketName,
                Key = key,
                FilePath = localFilePath
            };

            if (progress != null)
            {
                request.UploadProgressEvent += (sender, args) =>
                {
                    progress.Report(args.TransferredBytes);
                };
            }

            await transferUtility.UploadAsync(request, ct);
            return true;
        }, ct);

        _logger.LogInformation("S3 upload complete: s3://{Bucket}/{Key}", _config.BucketName, key);
        return $"s3://{_config.BucketName}/{key}";
    }

    public async Task CreateFolderAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetClient();
        var key = NormalizePrefix(remotePath); // Ensure trailing slash for folder marker

        if (string.IsNullOrEmpty(key) || key == "/")
        {
            _logger.LogWarning("S3 CreateFolderAsync skipped: cannot create root folder marker");
            return;
        }

        var request = new PutObjectRequest
        {
            BucketName = _config.BucketName,
            Key = key,
            ContentBody = string.Empty // Zero-byte object as folder marker
        };

        await RetryAsync(async () =>
        {
            await client.PutObjectAsync(request, ct);
            return true;
        }, ct);

        _logger.LogInformation("S3 folder marker created: s3://{Bucket}/{Key}", _config.BucketName, key);
    }

    public async Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        var client = GetClient();
        var key = NormalizeKey(remotePath);

        try
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = _config.BucketName,
                Key = key
            };

            await client.GetObjectMetadataAsync(request, ct);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "S3 FileExistsAsync error for key {Key} in bucket {Bucket}",
                key, _config.BucketName);
            return false;
        }
    }

    private AmazonS3Client GetClient()
    {
        if (_client != null)
            return _client;

        var credentials = string.IsNullOrEmpty(_config.SessionToken)
            ? (AWSCredentials)new BasicAWSCredentials(_config.AccessKeyId, _config.SecretAccessKey)
            : new SessionAWSCredentials(_config.AccessKeyId, _config.SecretAccessKey, _config.SessionToken);

        var s3Config = new AmazonS3Config();

        // Set region
        if (!string.IsNullOrEmpty(_config.Region))
        {
            s3Config.RegionEndpoint = RegionEndpoint.GetBySystemName(_config.Region);
        }

        // Custom endpoint (e.g., MinIO, LocalStack)
        if (!string.IsNullOrEmpty(_config.CustomEndpoint))
        {
            s3Config.ServiceURL = _config.CustomEndpoint;
            s3Config.ForcePathStyle = true; // Required for most S3-compatible endpoints
        }

        // Path-style addressing (e.g., http://s3.amazonaws.com/bucket instead of http://bucket.s3.amazonaws.com)
        if (_config.UsePathStyle)
        {
            s3Config.ForcePathStyle = true;
        }

        s3Config.Timeout = TimeSpan.FromSeconds(_config.ConnectionTimeout);
        s3Config.MaxErrorRetry = 0; // We handle retries ourselves

        _client = new AmazonS3Client(credentials, s3Config);
        _logger.LogInformation("S3 client created for bucket {Bucket} in region {Region}",
            _config.BucketName, _config.Region);

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
            catch (AmazonS3Exception ex) when (attempt < MaxRetries - 1 && IsRetryable(ex))
            {
                _logger.LogWarning(ex, "S3 operation failed (attempt {Attempt}/{Max}), retrying...",
                    attempt + 1, MaxRetries);
                await Task.Delay(RetryDelays[attempt], ct);
            }
            catch (IOException ex) when (attempt < MaxRetries - 1)
            {
                _logger.LogWarning(ex, "S3 I/O error (attempt {Attempt}/{Max}), retrying...",
                    attempt + 1, MaxRetries);
                await Task.Delay(RetryDelays[attempt], ct);
            }
        }

        throw new InvalidOperationException("Retry logic fell through unexpectedly");
    }

    private static bool IsRetryable(AmazonS3Exception ex)
    {
        // Retry on transient errors: 500, 502, 503, 504, request timeouts, slow-down
        return ex.StatusCode == System.Net.HttpStatusCode.InternalServerError
            || ex.StatusCode == System.Net.HttpStatusCode.BadGateway
            || ex.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable
            || ex.StatusCode == System.Net.HttpStatusCode.GatewayTimeout
            || ex.StatusCode == System.Net.HttpStatusCode.RequestTimeout
            || string.Equals(ex.ErrorCode, "SlowDown", StringComparison.OrdinalIgnoreCase)
            || string.Equals(ex.ErrorCode, "RequestTimeTooSkewed", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Normalizes a path to an S3 prefix (used for listing). Ensures trailing slash, no leading slash.
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
    /// Normalizes a path to an S3 object key. No leading slash, no trailing slash.
    /// </summary>
    private static string NormalizeKey(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("S3 object key cannot be empty", nameof(path));

        return path.Replace('\\', '/').TrimStart('/').TrimEnd('/');
    }

    /// <summary>Extracts the file name from an S3 object key.</summary>
    private static string GetFileName(string key)
    {
        if (string.IsNullOrEmpty(key)) return string.Empty;
        var trimmed = key.TrimEnd('/');
        var lastSlash = trimmed.LastIndexOf('/');
        return lastSlash >= 0 ? trimmed[(lastSlash + 1)..] : trimmed;
    }

    /// <summary>Extracts the folder name from an S3 common prefix.</summary>
    private static string GetFolderName(string commonPrefix)
    {
        if (string.IsNullOrEmpty(commonPrefix)) return string.Empty;
        var trimmed = commonPrefix.TrimEnd('/');
        var lastSlash = trimmed.LastIndexOf('/');
        return lastSlash >= 0 ? trimmed[(lastSlash + 1)..] : trimmed;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _client?.Dispose();
        _client = null;
    }
}
