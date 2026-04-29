using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Storage;

public interface IFileStorageService
{
    Task<PresignedUploadResult> CreatePresignedUploadAsync(
        Guid fileId,
        string fileExtension,
        CancellationToken cancellationToken = default);

    Task<string> DownloadToTempAsync(
        string s3Key,
        CancellationToken cancellationToken = default);

    Task DeleteAsync(
        string s3Key,
        CancellationToken cancellationToken = default);

    Task<bool> ExistsAsync(
        string s3Key,
        CancellationToken cancellationToken = default);

    Task<long> GetSizeAsync(
        string s3Key,
        CancellationToken cancellationToken = default);
}
