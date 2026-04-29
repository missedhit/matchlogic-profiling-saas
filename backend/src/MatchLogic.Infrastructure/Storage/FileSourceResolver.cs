using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Storage;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Storage;

public class FileSourceResolver : IFileSourceResolver
{
    private readonly IGenericRepository<FileImport, Guid> _fileImportRepo;
    private readonly IFileStorageService _storage;
    private readonly ILogger<FileSourceResolver> _logger;

    public FileSourceResolver(
        IGenericRepository<FileImport, Guid> fileImportRepo,
        IFileStorageService storage,
        ILogger<FileSourceResolver> logger)
    {
        _fileImportRepo = fileImportRepo;
        _storage = storage;
        _logger = logger;
    }

    public async Task<IFileSourceLease> ResolveAsync(
        Guid fileImportId,
        CancellationToken cancellationToken = default)
    {
        var fileImport = await _fileImportRepo.GetByIdAsync(fileImportId, Constants.Collections.ImportFile)
            ?? throw new InvalidOperationException($"FileImport {fileImportId} not found.");

        if (!string.IsNullOrEmpty(fileImport.S3Key))
        {
            var tempPath = await _storage.DownloadToTempAsync(fileImport.S3Key, cancellationToken);
            return new TempFileLease(tempPath, _logger);
        }

        // Legacy multipart-upload path: file already on local disk under MatchLogicApi/Uploads/.
        // No download, no cleanup — owner of the local file is the multipart upload handler.
        if (!string.IsNullOrEmpty(fileImport.FilePath))
        {
            return new PassThroughLease(fileImport.FilePath);
        }

        throw new InvalidOperationException(
            $"FileImport {fileImportId} has neither an S3Key nor a FilePath; cannot resolve file source.");
    }

    private sealed class TempFileLease : IFileSourceLease
    {
        private readonly ILogger _logger;
        private bool _disposed;

        public TempFileLease(string localPath, ILogger logger)
        {
            LocalPath = localPath;
            _logger = logger;
        }

        public string LocalPath { get; }

        public ValueTask DisposeAsync()
        {
            if (_disposed) return ValueTask.CompletedTask;
            _disposed = true;

            try
            {
                if (File.Exists(LocalPath)) File.Delete(LocalPath);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete temp file {LocalPath}", LocalPath);
            }
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PassThroughLease : IFileSourceLease
    {
        public PassThroughLease(string localPath) { LocalPath = localPath; }
        public string LocalPath { get; }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
