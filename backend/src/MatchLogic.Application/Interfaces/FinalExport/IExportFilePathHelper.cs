using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.FinalExport;

/// <summary>
/// Helper class for managing export file paths.
/// Handles file-based export detection, path generation, and folder management.
/// </summary>
public interface IExportFilePathHelper
{
    /// <summary>
    /// Checks if the connection type is file-based (CSV, Excel, JSON, etc.)
    /// </summary>
    bool IsFileBasedExport(DataSourceType type);

    /// <summary>
    /// Ensures the export file path is set for file-based exports.
    /// Creates a unique file path in the configured export folder.
    /// </summary>
    BaseConnectionInfo EnsureExportFilePath(BaseConnectionInfo connectionInfo, Guid projectId);

    /// <summary>
    /// Gets the file extension for a given data source type.
    /// </summary>
    string GetFileExtension(DataSourceType type);

    /// <summary>
    /// Ensures the export folder exists for a given project.
    /// </summary>
    /// <summary>
    /// Ensures the export folder exists for a given project.
    /// </summary>
    void EnsureExportFolderExists(Guid projectId);

    string GetTemporaryExportPath(Guid projectId, DataSourceType type);
    string FinalizeExportFile(string tempPath, Guid projectId, DataSourceType type);
    void CleanupTemporaryFile(string tempPath);
}
