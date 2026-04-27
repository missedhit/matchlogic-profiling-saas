using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;


namespace MatchLogic.Infrastructure.FinalExport;

/// <summary>
/// Implementation of export file path helper.
/// </summary>
public class ExportFilePathHelper : IExportFilePathHelper
{
    private readonly string _basePath;
    private readonly ILogger<ExportFilePathHelper> _logger;
    private const string FilePathKey = "FilePath";

    public ExportFilePathHelper(
        IConfiguration configuration,
        ILogger<ExportFilePathHelper> logger)
    {
        _logger = logger;
        _basePath = configuration["Storage:DictionaryPath"] ??
             Path.Combine(MatchLogic.Application.Common.StoragePaths.DefaultUploadPath, "FileExport");

        Directory.CreateDirectory(_basePath);
    }

    public bool IsFileBasedExport(DataSourceType type) => type switch
    {
        DataSourceType.CSV => true,
        DataSourceType.Excel => true,
        _ => false
    };

    public string GetFileExtension(DataSourceType type) => type switch
    {
        DataSourceType.CSV => ".csv",
        DataSourceType.Excel => ".xlsx",
        _ => ".dat"
    };

    /// <summary>
    /// Generates a temporary file path for export (in temp subfolder).
    /// </summary>
    public string GetTemporaryExportPath(Guid projectId, DataSourceType type)
    {
        var tempFolder = Path.Combine(_basePath, "temp");
        Directory.CreateDirectory(tempFolder);

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var extension = GetFileExtension(type);
        var fileName = $"export_{projectId:N}_{timestamp}{extension}";

        return Path.Combine(tempFolder, fileName);
    }

    /// <summary>
    /// Moves temp export file to final project folder, cleaning up old exports.
    /// Returns the final file path.
    /// </summary>
    public string FinalizeExportFile(string tempPath, Guid projectId, DataSourceType type)
    {
        if (!File.Exists(tempPath))
            throw new FileNotFoundException("Temporary export file not found", tempPath);

        // Ensure destination folder exists
        var exportFolder = GetProjectExportFolder(projectId);
        EnsureExportFolderExists(projectId);

        // Delete old export files for this project (keep only latest)
        var extension = GetFileExtension(type);
        var pattern = $"export_{projectId:N}_*{extension}";

        foreach (var oldFile in Directory.GetFiles(exportFolder, pattern))
        {
            try
            {
                File.Delete(oldFile);
                _logger.LogInformation("Deleted old export file: {Path}", oldFile);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete old export file: {Path}", oldFile);
            }
        }

        // Move temp file to final location
        var fileName = Path.GetFileName(tempPath);
        var finalPath = Path.Combine(exportFolder, fileName);

        File.Move(tempPath, finalPath, overwrite: true);
        _logger.LogInformation("Finalized export file: {TempPath} -> {FinalPath}", tempPath, finalPath);

        return finalPath;
    }

    /// <summary>
    /// Cleans up a temporary export file (on failure).
    /// </summary>
    public void CleanupTemporaryFile(string tempPath)
    {
        try
        {
            if (!string.IsNullOrEmpty(tempPath) && File.Exists(tempPath))
            {
                File.Delete(tempPath);
                _logger.LogInformation("Cleaned up temp file: {Path}", tempPath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup temp file: {Path}", tempPath);
        }
    }

    public BaseConnectionInfo EnsureExportFilePath(BaseConnectionInfo connectionInfo, Guid projectId)
    {
        // If path already set (user override), return as-is
        if (connectionInfo.Parameters.TryGetValue(FilePathKey, out var existingPath)
            && !string.IsNullOrWhiteSpace(existingPath))
        {
            return connectionInfo;
        }

        // Only generate path for file-based exports
        if (!IsFileBasedExport(connectionInfo.Type))
            return connectionInfo;

        // Generate final path (used for user overrides or non-temp scenarios)
        EnsureExportFolderExists(projectId);
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var extension = GetFileExtension(connectionInfo.Type);
        var fileName = $"export_{projectId:N}_{timestamp}{extension}";
        var exportFolder = GetProjectExportFolder(projectId);
        var filePath = Path.Combine(exportFolder, fileName);

        var parameters = new Dictionary<string, string>(connectionInfo.Parameters)
        {
            [FilePathKey] = filePath
        };

        return new BaseConnectionInfo
        {
            Type = connectionInfo.Type,
            Parameters = parameters
        };
    }

    public void EnsureExportFolderExists(Guid projectId)
    {
        var folder = GetProjectExportFolder(projectId);
        if (!Directory.Exists(folder))
        {
            Directory.CreateDirectory(folder);
        }
    }

    private string GetProjectExportFolder(Guid projectId)
    {
        return Path.Combine(_basePath, projectId.ToString("N"));
    }
}
