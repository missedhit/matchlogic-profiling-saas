using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Import;

/// <summary>
/// Abstraction for connecting to remote file storage systems (FTP, S3, Azure, etc.).
/// Each implementation handles a specific protocol/provider.
/// </summary>
public interface IRemoteFileConnector : IDisposable
{
    /// <summary>Validate that the connection can be established with the given credentials.</summary>
    Task<bool> TestConnectionAsync(CancellationToken ct = default);

    /// <summary>List files at the given remote path. Use "/" or "" for root.</summary>
    Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default);

    /// <summary>List folders/directories at the given remote path.</summary>
    Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default);

    /// <summary>Get metadata for a single remote file.</summary>
    Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default);

    /// <summary>
    /// Download a file from remote storage to a local temp directory.
    /// Returns the local file path of the downloaded file.
    /// </summary>
    Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default);

    /// <summary>
    /// Upload a local file to remote storage at the given path.
    /// Returns the remote path/URL of the uploaded file.
    /// </summary>
    Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default);

    /// <summary>Create a folder/directory at the given remote path.</summary>
    Task CreateFolderAsync(string remotePath, CancellationToken ct = default);

    /// <summary>Check if a file exists at the given remote path.</summary>
    Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default);
}

/// <summary>Metadata for a remote file entry.</summary>
public record RemoteFileInfo(
    string Name,
    string Path,
    long Size,
    DateTime LastModified,
    string Extension
);

/// <summary>Metadata for a remote folder entry.</summary>
public record RemoteFolderInfo(
    string Name,
    string Path,
    DateTime LastModified
);

/// <summary>Detailed metadata for a single remote file.</summary>
public record RemoteFileMetadata(
    string Name,
    long Size,
    DateTime LastModified,
    string? ETag,
    string? ContentType
);

/// <summary>Result of a remote browse operation (files + folders at a path).</summary>
public record RemoteBrowseResult(
    string CurrentPath,
    List<RemoteFileInfo> Files,
    List<RemoteFolderInfo> Folders
);
