using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using MatchLogic.Application.Interfaces.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MatchLogic.Infrastructure.Storage;

public class S3FileStorageService : IFileStorageService
{
    private readonly IAmazonS3 _s3;
    private readonly S3Options _options;
    private readonly ILogger<S3FileStorageService> _logger;

    public S3FileStorageService(
        IAmazonS3 s3,
        IOptions<S3Options> options,
        ILogger<S3FileStorageService> logger)
    {
        _s3 = s3;
        _options = options.Value;
        _logger = logger;

        if (string.IsNullOrWhiteSpace(_options.BucketName))
        {
            throw new InvalidOperationException(
                "S3:BucketName is not configured. Set it in appsettings or via the S3__BucketName " +
                "environment variable (Fargate task pulls it from SSM /profiler-saas/dev/s3/uploads-bucket).");
        }
    }

    public Task<PresignedUploadResult> CreatePresignedUploadAsync(
        Guid fileId,
        string fileExtension,
        CancellationToken cancellationToken = default)
    {
        var s3Key = BuildUploadKey(fileId, fileExtension);
        var expiresAt = DateTime.UtcNow.AddMinutes(_options.PresignedUploadExpiryMinutes);

        var request = new GetPreSignedUrlRequest
        {
            BucketName = _options.BucketName,
            Key = s3Key,
            Verb = HttpVerb.PUT,
            Expires = expiresAt,
            Protocol = Protocol.HTTPS
        };

        var url = _s3.GetPreSignedURL(request);

        _logger.LogInformation(
            "Minted presigned PUT URL for {S3Key} (expires {ExpiresAt:o})",
            s3Key, expiresAt);

        return Task.FromResult(new PresignedUploadResult(s3Key, url, expiresAt));
    }

    public async Task<string> DownloadToTempAsync(
        string s3Key,
        CancellationToken cancellationToken = default)
    {
        var extension = Path.GetExtension(s3Key);
        var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}{extension}");

        var request = new GetObjectRequest
        {
            BucketName = _options.BucketName,
            Key = s3Key
        };

        try
        {
            using var response = await _s3.GetObjectAsync(request, cancellationToken);
            await using var fileStream = new FileStream(
                tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
            await response.ResponseStream.CopyToAsync(fileStream, cancellationToken);
        }
        catch
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
            throw;
        }

        _logger.LogInformation(
            "Downloaded s3://{Bucket}/{Key} to {TempPath}",
            _options.BucketName, s3Key, tempPath);

        return tempPath;
    }

    public async Task DeleteAsync(string s3Key, CancellationToken cancellationToken = default)
    {
        await _s3.DeleteObjectAsync(
            new DeleteObjectRequest { BucketName = _options.BucketName, Key = s3Key },
            cancellationToken);

        _logger.LogInformation("Deleted s3://{Bucket}/{Key}", _options.BucketName, s3Key);
    }

    public async Task<bool> ExistsAsync(string s3Key, CancellationToken cancellationToken = default)
    {
        try
        {
            await _s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = _options.BucketName, Key = s3Key },
                cancellationToken);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<long> GetSizeAsync(string s3Key, CancellationToken cancellationToken = default)
    {
        var response = await _s3.GetObjectMetadataAsync(
            new GetObjectMetadataRequest { BucketName = _options.BucketName, Key = s3Key },
            cancellationToken);
        return response.ContentLength;
    }

    private string BuildUploadKey(Guid fileId, string fileExtension)
    {
        var prefix = _options.UploadKeyPrefix.TrimEnd('/');
        var ext = string.IsNullOrEmpty(fileExtension)
            ? string.Empty
            : (fileExtension.StartsWith('.') ? fileExtension : "." + fileExtension);
        return $"{prefix}/{fileId:D}{ext}";
    }
}
