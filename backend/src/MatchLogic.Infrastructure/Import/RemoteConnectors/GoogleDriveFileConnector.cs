using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Download;
using Google.Apis.Services;
using Google.Apis.Upload;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import.RemoteConnectors;

[HandlesRemoteConnector(DataSourceType.GoogleDrive)]
public class GoogleDriveFileConnector : IRemoteFileConnector
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly ILogger _logger;
    private DriveService? _driveService;
    private bool _disposed;

    private const int MaxRetries = 3;
    private const int MaxPageSize = 1000;
    private static readonly TimeSpan[] RetryDelays =
    {
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(3),
        TimeSpan.FromSeconds(7)
    };

    /// <summary>Google Sheets MIME type — these files must be exported rather than downloaded directly.</summary>
    private const string GoogleSheetsMimeType = "application/vnd.google-apps.spreadsheet";
    private const string GoogleFolderMimeType = "application/vnd.google-apps.folder";

    /// <summary>Set of MIME types representing Google Workspace documents that cannot be downloaded as binary.</summary>
    private static readonly HashSet<string> GoogleWorkspaceMimeTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "application/vnd.google-apps.spreadsheet",
        "application/vnd.google-apps.document",
        "application/vnd.google-apps.presentation",
        "application/vnd.google-apps.drawing"
    };

    public GoogleDriveFileConnector(RemoteFileConnectionConfig config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    #region IRemoteFileConnector Implementation

    public async Task<bool> TestConnectionAsync(CancellationToken ct = default)
    {
        try
        {
            var service = GetDriveService();
            var aboutRequest = service.About.Get();
            aboutRequest.Fields = "user";
            var about = await aboutRequest.ExecuteAsync(ct);

            _logger.LogInformation(
                "Google Drive connection test successful. User: {UserEmail}",
                about.User?.EmailAddress ?? "unknown");

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Google Drive connection test failed");
            return false;
        }
    }

    public async Task<List<RemoteFileInfo>> ListFilesAsync(string path, CancellationToken ct = default)
    {
        var service = GetDriveService();
        var folderId = await ResolvePathToFolderIdAsync(service, path, ct);

        var query = $"'{folderId}' in parents " +
                    $"and mimeType != '{GoogleFolderMimeType}' " +
                    "and trashed = false";

        var files = await ListAllFilesAsync(service, query, ct);

        // Filter by file extension if one is configured
        var extension = _config.FileExtension;
        if (!string.IsNullOrWhiteSpace(extension))
        {
            var ext = extension.StartsWith('.') ? extension : $".{extension}";
            files = files
                .Where(f => f.Name.EndsWith(ext, StringComparison.OrdinalIgnoreCase)
                            || IsExportableAsExtension(f.MimeType, ext))
                .ToList();
        }

        return files
            .Select(f => new RemoteFileInfo(
                Name: f.Name,
                Path: f.Id,
                Size: f.Size ?? 0,
                LastModified: f.ModifiedTimeDateTimeOffset?.UtcDateTime ?? DateTime.MinValue,
                Extension: GetEffectiveExtension(f.Name, f.MimeType)))
            .OrderBy(f => f.Name)
            .ToList();
    }

    public async Task<List<RemoteFolderInfo>> ListFoldersAsync(string path, CancellationToken ct = default)
    {
        var service = GetDriveService();
        var folderId = await ResolvePathToFolderIdAsync(service, path, ct);

        var query = $"'{folderId}' in parents " +
                    $"and mimeType = '{GoogleFolderMimeType}' " +
                    "and trashed = false";

        var folders = await ListAllFilesAsync(service, query, ct);

        return folders
            .Select(f => new RemoteFolderInfo(
                Name: f.Name,
                Path: f.Id,
                LastModified: f.ModifiedTimeDateTimeOffset?.UtcDateTime ?? DateTime.MinValue))
            .OrderBy(f => f.Name)
            .ToList();
    }

    public async Task<RemoteFileMetadata> GetFileMetadataAsync(string remotePath, CancellationToken ct = default)
    {
        var service = GetDriveService();
        var fileId = await ResolvePathToFileIdAsync(service, remotePath, ct);

        var request = service.Files.Get(fileId);
        request.Fields = "id,name,size,modifiedTime,mimeType";
        var file = await ExecuteWithRetryAsync(() => request.ExecuteAsync(ct), ct);

        return new RemoteFileMetadata(
            Name: file.Name,
            Size: file.Size ?? 0,
            LastModified: file.ModifiedTimeDateTimeOffset?.UtcDateTime ?? DateTime.MinValue,
            ETag: file.ETag,
            ContentType: file.MimeType);
    }

    public async Task<string> DownloadFileAsync(string remotePath, string localTempDir,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var service = GetDriveService();
        var fileId = await ResolvePathToFileIdAsync(service, remotePath, ct);

        // Fetch metadata to determine MIME type and file name
        var metaRequest = service.Files.Get(fileId);
        metaRequest.Fields = "id,name,size,mimeType";
        var fileMeta = await ExecuteWithRetryAsync(() => metaRequest.ExecuteAsync(ct), ct);

        Directory.CreateDirectory(localTempDir);

        string localPath;

        if (IsGoogleWorkspaceFile(fileMeta.MimeType))
        {
            // Google Workspace files must be exported — Sheets export as CSV
            localPath = await ExportGoogleWorkspaceFileAsync(
                service, fileId, fileMeta, localTempDir, progress, ct);
        }
        else
        {
            // Binary files (CSV, Excel, etc.) — direct download
            localPath = await DownloadBinaryFileAsync(
                service, fileId, fileMeta, localTempDir, progress, ct);
        }

        _logger.LogInformation(
            "Google Drive download complete: {FileName} -> {LocalPath} ({Size} bytes)",
            fileMeta.Name, localPath, new FileInfo(localPath).Length);

        return localPath;
    }

    public async Task<string> UploadFileAsync(string localFilePath, string remotePath,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var service = GetDriveService();

        // Determine parent folder ID from remotePath
        var parentFolderId = "root";
        var fileName = Path.GetFileName(localFilePath);

        if (!string.IsNullOrWhiteSpace(remotePath))
        {
            var parentPath = GetParentPath(remotePath);
            if (!string.IsNullOrWhiteSpace(parentPath))
            {
                parentFolderId = await ResolveOrCreateFolderPathAsync(service, parentPath, ct);
            }

            // Use the remote path's file name if provided
            var remoteFileName = GetFileName(remotePath);
            if (!string.IsNullOrWhiteSpace(remoteFileName))
                fileName = remoteFileName;
        }

        var fileMetadata = new Google.Apis.Drive.v3.Data.File
        {
            Name = fileName,
            Parents = new List<string> { parentFolderId }
        };

        var contentType = GetContentType(fileName);

        _logger.LogInformation("Google Drive uploading: {LocalPath} -> folder {FolderId}",
            localFilePath, parentFolderId);

        await using var fileStream = new FileStream(localFilePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, 81920, useAsync: true);

        var uploadRequest = service.Files.Create(fileMetadata, fileStream, contentType);
        uploadRequest.Fields = "id,name";

        if (progress != null)
        {
            uploadRequest.ProgressChanged += p =>
            {
                if (p.Status == UploadStatus.Uploading)
                    progress.Report(p.BytesSent);
            };
        }

        var uploadProgress = await ExecuteWithRetryAsync(
            () => uploadRequest.UploadAsync(ct), ct);

        if (uploadProgress.Status == UploadStatus.Failed)
        {
            throw new IOException(
                $"Google Drive upload failed for: {localFilePath}",
                uploadProgress.Exception);
        }

        var uploadedFile = uploadRequest.ResponseBody;
        _logger.LogInformation("Google Drive upload complete: {FileId} ({FileName})",
            uploadedFile?.Id, uploadedFile?.Name);

        return uploadedFile?.Id ?? string.Empty;
    }

    public async Task CreateFolderAsync(string remotePath, CancellationToken ct = default)
    {
        var service = GetDriveService();

        // Resolve or create each segment of the path
        var segments = SplitPath(remotePath);
        var currentParentId = "root";

        foreach (var segment in segments)
        {
            var existingId = await FindItemIdByNameAsync(
                service, segment, currentParentId, GoogleFolderMimeType, ct);

            if (existingId != null)
            {
                currentParentId = existingId;
            }
            else
            {
                var folderMeta = new Google.Apis.Drive.v3.Data.File
                {
                    Name = segment,
                    MimeType = GoogleFolderMimeType,
                    Parents = new List<string> { currentParentId }
                };

                var createRequest = service.Files.Create(folderMeta);
                createRequest.Fields = "id";
                var created = await ExecuteWithRetryAsync(() => createRequest.ExecuteAsync(ct), ct);
                currentParentId = created.Id;

                _logger.LogDebug("Google Drive created folder: {Name} (id: {Id})", segment, created.Id);
            }
        }
    }

    public async Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        try
        {
            var service = GetDriveService();
            var parentPath = GetParentPath(remotePath);
            var fileName = GetFileName(remotePath);

            if (string.IsNullOrWhiteSpace(fileName))
                return false;

            var parentId = await ResolvePathToFolderIdAsync(service, parentPath, ct);
            var fileId = await FindItemIdByNameAsync(service, fileName, parentId, mimeType: null, ct);

            return fileId != null;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "FileExistsAsync check failed for: {Path}", remotePath);
            return false;
        }
    }

    #endregion

    #region Google Drive Service Initialization

    private DriveService GetDriveService()
    {
        if (_driveService != null)
            return _driveService;

        if (string.IsNullOrWhiteSpace(_config.AccessToken))
            throw new InvalidOperationException("Google Drive access token is not configured.");

        var credential = GoogleCredential.FromAccessToken(_config.AccessToken);

        _driveService = new DriveService(new BaseClientService.Initializer
        {
            HttpClientInitializer = credential,
            ApplicationName = "MatchLogic"
        });

        _logger.LogDebug("Google Drive service initialized");
        return _driveService;
    }

    #endregion

    #region Path Resolution

    /// <summary>
    /// Resolves a human-readable path (e.g., "Folder1/Folder2") to a Google Drive folder ID.
    /// Returns "root" for empty/null/root paths.
    /// If the path looks like a raw Google Drive ID (no slashes, alphanumeric+dash+underscore), returns it directly.
    /// </summary>
    private async Task<string> ResolvePathToFolderIdAsync(
        DriveService service, string path, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(path) || path == "/" || path.Equals("root", StringComparison.OrdinalIgnoreCase))
            return "root";

        // If it looks like a raw Google Drive ID, return as-is
        if (LooksLikeDriveId(path))
            return path;

        var segments = SplitPath(path);
        var currentId = "root";

        foreach (var segment in segments)
        {
            var folderId = await FindItemIdByNameAsync(
                service, segment, currentId, GoogleFolderMimeType, ct);

            if (folderId == null)
                throw new DirectoryNotFoundException(
                    $"Google Drive folder not found: '{segment}' in path '{path}'");

            currentId = folderId;
        }

        return currentId;
    }

    /// <summary>
    /// Resolves a folder path, creating any missing folders along the way.
    /// Returns the Google Drive folder ID for the final segment.
    /// </summary>
    private async Task<string> ResolveOrCreateFolderPathAsync(
        DriveService service, string path, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(path) || path == "/" || path.Equals("root", StringComparison.OrdinalIgnoreCase))
            return "root";

        if (LooksLikeDriveId(path))
            return path;

        var segments = SplitPath(path);
        var currentId = "root";

        foreach (var segment in segments)
        {
            var folderId = await FindItemIdByNameAsync(
                service, segment, currentId, GoogleFolderMimeType, ct);

            if (folderId != null)
            {
                currentId = folderId;
            }
            else
            {
                // Create the missing folder
                var folderMeta = new Google.Apis.Drive.v3.Data.File
                {
                    Name = segment,
                    MimeType = GoogleFolderMimeType,
                    Parents = new List<string> { currentId }
                };

                var createRequest = service.Files.Create(folderMeta);
                createRequest.Fields = "id";
                var created = await ExecuteWithRetryAsync(() => createRequest.ExecuteAsync(ct), ct);
                currentId = created.Id;

                _logger.LogInformation("Google Drive created folder during upload: {Name} (id: {Id})", segment, created.Id);
            }
        }

        return currentId;
    }

    /// <summary>
    /// Resolves a path to a file ID. Accepts either a raw Drive file ID or a path like "Folder/file.csv".
    /// </summary>
    private async Task<string> ResolvePathToFileIdAsync(
        DriveService service, string remotePath, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(remotePath))
            throw new ArgumentException("Remote path cannot be empty.", nameof(remotePath));

        // If it looks like a raw Drive ID, return directly
        if (LooksLikeDriveId(remotePath))
            return remotePath;

        var parentPath = GetParentPath(remotePath);
        var fileName = GetFileName(remotePath);

        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException($"Cannot extract file name from path: {remotePath}");

        var parentId = await ResolvePathToFolderIdAsync(service, parentPath, ct);
        var fileId = await FindItemIdByNameAsync(service, fileName, parentId, mimeType: null, ct);

        if (fileId == null)
            throw new FileNotFoundException($"Google Drive file not found: {remotePath}");

        return fileId;
    }

    /// <summary>
    /// Searches for a file or folder by name within a specific parent folder.
    /// Optionally filters by MIME type. Returns the ID or null if not found.
    /// </summary>
    private async Task<string?> FindItemIdByNameAsync(
        DriveService service, string name, string parentId,
        string? mimeType, CancellationToken ct)
    {
        var escapedName = name.Replace("'", "\\'");
        var query = $"'{parentId}' in parents " +
                    $"and name = '{escapedName}' " +
                    "and trashed = false";

        if (!string.IsNullOrEmpty(mimeType))
            query += $" and mimeType = '{mimeType}'";

        var listRequest = service.Files.List();
        listRequest.Q = query;
        listRequest.Fields = "files(id)";
        listRequest.PageSize = 1;

        var result = await ExecuteWithRetryAsync(() => listRequest.ExecuteAsync(ct), ct);
        return result.Files?.FirstOrDefault()?.Id;
    }

    #endregion

    #region Download Helpers

    private async Task<string> DownloadBinaryFileAsync(
        DriveService service, string fileId,
        Google.Apis.Drive.v3.Data.File fileMeta,
        string localTempDir, IProgress<long>? progress, CancellationToken ct)
    {
        var localPath = Path.Combine(localTempDir, SanitizeFileName(fileMeta.Name));

        _logger.LogInformation(
            "Google Drive downloading binary file: {FileName} ({FileId})",
            fileMeta.Name, fileId);

        var getRequest = service.Files.Get(fileId);

        await using var fileStream = new FileStream(
            localPath, FileMode.Create, FileAccess.Write,
            FileShare.None, 81920, useAsync: true);

        if (progress != null)
        {
            getRequest.MediaDownloader.ProgressChanged += p =>
            {
                if (p.Status == DownloadStatus.Downloading)
                    progress.Report(p.BytesDownloaded);
            };
        }

        var downloadProgress = await ExecuteWithRetryAsync(
            () => getRequest.DownloadAsync(fileStream, ct), ct);

        if (downloadProgress.Status == DownloadStatus.Failed)
        {
            throw new IOException(
                $"Google Drive download failed for file: {fileMeta.Name} ({fileId})",
                downloadProgress.Exception);
        }

        return localPath;
    }

    private async Task<string> ExportGoogleWorkspaceFileAsync(
        DriveService service, string fileId,
        Google.Apis.Drive.v3.Data.File fileMeta,
        string localTempDir, IProgress<long>? progress, CancellationToken ct)
    {
        // Determine export MIME type and extension based on the Google Workspace file type
        var (exportMimeType, exportExtension) = GetExportFormat(fileMeta.MimeType);

        var exportFileName = Path.GetFileNameWithoutExtension(fileMeta.Name) + exportExtension;
        var localPath = Path.Combine(localTempDir, SanitizeFileName(exportFileName));

        _logger.LogInformation(
            "Google Drive exporting Workspace file: {FileName} ({MimeType}) as {ExportFormat}",
            fileMeta.Name, fileMeta.MimeType, exportMimeType);

        var exportRequest = service.Files.Export(fileId, exportMimeType);

        await using var fileStream = new FileStream(
            localPath, FileMode.Create, FileAccess.Write,
            FileShare.None, 81920, useAsync: true);

        if (progress != null)
        {
            exportRequest.MediaDownloader.ProgressChanged += p =>
            {
                if (p.Status == DownloadStatus.Downloading)
                    progress.Report(p.BytesDownloaded);
            };
        }

        var downloadProgress = await ExecuteWithRetryAsync(
            () => exportRequest.DownloadAsync(fileStream, ct), ct);

        if (downloadProgress.Status == DownloadStatus.Failed)
        {
            throw new IOException(
                $"Google Drive export failed for file: {fileMeta.Name} ({fileId})",
                downloadProgress.Exception);
        }

        return localPath;
    }

    #endregion

    #region Retry & Rate Limiting

    /// <summary>
    /// Executes an async operation with retry logic, handling Google API rate limits (HTTP 429)
    /// and transient errors (5xx).
    /// </summary>
    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken ct)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Google.GoogleApiException gex)
                when (attempt < MaxRetries - 1 && IsRetryableGoogleError(gex))
            {
                var delay = RetryDelays[attempt];
                _logger.LogWarning(gex,
                    "Google Drive API error (HTTP {StatusCode}, attempt {Attempt}/{Max}). Retrying in {Delay}s...",
                    (int)gex.HttpStatusCode, attempt + 1, MaxRetries, delay.TotalSeconds);

                await Task.Delay(delay, ct);
            }
            catch (Exception ex) when (attempt < MaxRetries - 1 && IsTransientError(ex))
            {
                var delay = RetryDelays[attempt];
                _logger.LogWarning(ex,
                    "Transient error during Google Drive operation (attempt {Attempt}/{Max}). Retrying in {Delay}s...",
                    attempt + 1, MaxRetries, delay.TotalSeconds);

                await Task.Delay(delay, ct);
            }
        }

        throw new InvalidOperationException("Retry logic fell through unexpectedly.");
    }

    private static bool IsRetryableGoogleError(Google.GoogleApiException ex)
    {
        return ex.HttpStatusCode == HttpStatusCode.TooManyRequests      // 429 rate limit
            || ex.HttpStatusCode == HttpStatusCode.InternalServerError  // 500
            || ex.HttpStatusCode == HttpStatusCode.BadGateway           // 502
            || ex.HttpStatusCode == HttpStatusCode.ServiceUnavailable   // 503
            || ex.HttpStatusCode == HttpStatusCode.GatewayTimeout;     // 504
    }

    private static bool IsTransientError(Exception ex)
    {
        return ex is HttpRequestException
            || ex is TaskCanceledException { InnerException: TimeoutException }
            || ex is IOException;
    }

    #endregion

    #region File Listing Helper

    /// <summary>
    /// Fetches all matching files using pagination (handles nextPageToken automatically).
    /// </summary>
    private async Task<List<Google.Apis.Drive.v3.Data.File>> ListAllFilesAsync(
        DriveService service, string query, CancellationToken ct)
    {
        var allFiles = new List<Google.Apis.Drive.v3.Data.File>();
        string? pageToken = null;

        do
        {
            var listRequest = service.Files.List();
            listRequest.Q = query;
            listRequest.Fields = "nextPageToken,files(id,name,size,modifiedTime,mimeType)";
            listRequest.PageSize = MaxPageSize;
            listRequest.PageToken = pageToken;
            listRequest.OrderBy = "name";

            var result = await ExecuteWithRetryAsync(() => listRequest.ExecuteAsync(ct), ct);

            if (result.Files != null)
                allFiles.AddRange(result.Files);

            pageToken = result.NextPageToken;
        }
        while (!string.IsNullOrEmpty(pageToken));

        return allFiles;
    }

    #endregion

    #region Utility Methods

    private static string[] SplitPath(string path)
    {
        return path
            .Replace('\\', '/')
            .Split('/', StringSplitOptions.RemoveEmptyEntries);
    }

    private static string GetParentPath(string path)
    {
        var normalized = path.Replace('\\', '/').TrimEnd('/');
        var lastSlash = normalized.LastIndexOf('/');
        return lastSlash <= 0 ? string.Empty : normalized[..lastSlash];
    }

    private static string GetFileName(string path)
    {
        var normalized = path.Replace('\\', '/').TrimEnd('/');
        var lastSlash = normalized.LastIndexOf('/');
        return lastSlash < 0 ? normalized : normalized[(lastSlash + 1)..];
    }

    /// <summary>
    /// Heuristic: Google Drive IDs are typically 25-50 chars of [a-zA-Z0-9_-] with no slashes.
    /// </summary>
    private static bool LooksLikeDriveId(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Contains('/') || value.Contains('\\'))
            return false;

        return value.Length >= 10
            && value.All(c => char.IsLetterOrDigit(c) || c == '-' || c == '_');
    }

    private static bool IsGoogleWorkspaceFile(string? mimeType)
    {
        return !string.IsNullOrEmpty(mimeType) && GoogleWorkspaceMimeTypes.Contains(mimeType);
    }

    /// <summary>
    /// Determines the export MIME type and file extension for a Google Workspace document.
    /// Sheets are exported as CSV; other types as PDF (safe default).
    /// </summary>
    private static (string ExportMimeType, string Extension) GetExportFormat(string googleMimeType)
    {
        return googleMimeType switch
        {
            GoogleSheetsMimeType => ("text/csv", ".csv"),
            "application/vnd.google-apps.document" => ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"),
            "application/vnd.google-apps.presentation" => ("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"),
            _ => ("application/pdf", ".pdf")
        };
    }

    /// <summary>
    /// Returns the effective extension for a file, considering Google Workspace exports.
    /// </summary>
    private static string GetEffectiveExtension(string fileName, string? mimeType)
    {
        if (!string.IsNullOrEmpty(mimeType) && IsGoogleWorkspaceFile(mimeType))
        {
            var (_, ext) = GetExportFormat(mimeType);
            return ext;
        }

        return Path.GetExtension(fileName).ToLowerInvariant();
    }

    /// <summary>
    /// Checks whether a Google Workspace MIME type can be exported as a given file extension.
    /// </summary>
    private static bool IsExportableAsExtension(string? mimeType, string extension)
    {
        if (string.IsNullOrEmpty(mimeType) || !IsGoogleWorkspaceFile(mimeType))
            return false;

        var (_, exportExt) = GetExportFormat(mimeType);
        return exportExt.Equals(extension, StringComparison.OrdinalIgnoreCase);
    }

    private static string SanitizeFileName(string fileName)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = new string(fileName.Select(c => invalidChars.Contains(c) ? '_' : c).ToArray());
        return string.IsNullOrWhiteSpace(sanitized) ? "downloaded_file" : sanitized;
    }

    private static string GetContentType(string fileName)
    {
        var extension = Path.GetExtension(fileName).ToLowerInvariant();
        return extension switch
        {
            ".csv" => "text/csv",
            ".tsv" => "text/tab-separated-values",
            ".txt" => "text/plain",
            ".xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls" => "application/vnd.ms-excel",
            ".json" => "application/json",
            ".xml" => "application/xml",
            _ => "application/octet-stream"
        };
    }

    #endregion

    #region IDisposable

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _driveService?.Dispose();
        _driveService = null;
    }

    #endregion
}
