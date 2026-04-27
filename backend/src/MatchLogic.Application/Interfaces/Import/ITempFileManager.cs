using System;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Import;

/// <summary>
/// Manages temporary file storage for remote file downloads and export staging.
/// Ensures proper cleanup of temp files after use.
/// </summary>
public interface ITempFileManager
{
    /// <summary>
    /// Create a unique temp file path for a given datasource and filename.
    /// Creates the directory structure if it doesn't exist.
    /// </summary>
    string CreateTempPath(Guid dataSourceId, string fileName);

    /// <summary>Delete a specific temp file if it exists.</summary>
    void CleanupTempFile(string path);

    /// <summary>Delete all temp files for a given datasource.</summary>
    void CleanupDataSourceTemp(Guid dataSourceId);

    /// <summary>Get the total size of the temp directory in bytes.</summary>
    Task<long> GetTempDirSizeAsync();

    /// <summary>
    /// Purge orphaned temp files older than the specified age.
    /// Called by background cleanup service.
    /// </summary>
    Task<int> PurgeOrphanedFilesAsync(TimeSpan maxAge);
}
