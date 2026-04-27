using MatchLogic.Application.Interfaces.Import;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import;

public class TempFileManager : ITempFileManager
{
    private readonly string _baseTempDir;
    private readonly ILogger<TempFileManager> _logger;
    private const long DefaultMaxTempDirSize = 10L * 1024 * 1024 * 1024; // 10 GB

    public TempFileManager(ILogger<TempFileManager> logger)
    {
        _logger = logger;
        _baseTempDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "MatchLogic", "temp");

        if (!Directory.Exists(_baseTempDir))
            Directory.CreateDirectory(_baseTempDir);
    }

    public string CreateTempPath(Guid dataSourceId, string fileName)
    {
        var dsDir = Path.Combine(_baseTempDir, dataSourceId.ToString("N"));
        if (!Directory.Exists(dsDir))
            Directory.CreateDirectory(dsDir);

        // Use GUID prefix to avoid filename collisions
        var safeName = $"{Guid.NewGuid():N}_{SanitizeFileName(fileName)}";
        var tempPath = Path.Combine(dsDir, safeName);

        _logger.LogDebug("Created temp path: {TempPath} for DataSource: {DataSourceId}", tempPath, dataSourceId);
        return tempPath;
    }

    public void CleanupTempFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
                _logger.LogDebug("Cleaned up temp file: {Path}", path);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup temp file: {Path}", path);
        }
    }

    public void CleanupDataSourceTemp(Guid dataSourceId)
    {
        try
        {
            var dsDir = Path.Combine(_baseTempDir, dataSourceId.ToString("N"));
            if (Directory.Exists(dsDir))
            {
                Directory.Delete(dsDir, recursive: true);
                _logger.LogDebug("Cleaned up temp directory for DataSource: {DataSourceId}", dataSourceId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup temp directory for DataSource: {DataSourceId}", dataSourceId);
        }
    }

    public Task<long> GetTempDirSizeAsync()
    {
        return Task.Run(() =>
        {
            if (!Directory.Exists(_baseTempDir))
                return 0L;

            return new DirectoryInfo(_baseTempDir)
                .EnumerateFiles("*", SearchOption.AllDirectories)
                .Sum(f => f.Length);
        });
    }

    public Task<int> PurgeOrphanedFilesAsync(TimeSpan maxAge)
    {
        return Task.Run(() =>
        {
            if (!Directory.Exists(_baseTempDir))
                return 0;

            var cutoff = DateTime.UtcNow - maxAge;
            var purged = 0;

            foreach (var dir in Directory.GetDirectories(_baseTempDir))
            {
                try
                {
                    var dirInfo = new DirectoryInfo(dir);
                    if (dirInfo.LastWriteTimeUtc < cutoff)
                    {
                        dirInfo.Delete(recursive: true);
                        purged++;
                        _logger.LogInformation("Purged orphaned temp directory: {Dir}", dir);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to purge orphaned directory: {Dir}", dir);
                }
            }

            return purged;
        });
    }

    private static string SanitizeFileName(string fileName)
    {
        var invalid = Path.GetInvalidFileNameChars();
        return new string(fileName.Select(c => invalid.Contains(c) ? '_' : c).ToArray());
    }
}
