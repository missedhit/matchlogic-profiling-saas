using System;
using System.IO;

namespace MatchLogic.Application.Common;

/// <summary>
/// Cross-platform storage path resolution.
/// Detects container environment and provides appropriate paths.
/// </summary>
public static class StoragePaths
{
    private static bool _initialized;

    public static bool IsRunningInContainer =>
        Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true";

    public static string DefaultUploadPath { get; private set; } = string.Empty;
    public static string DefaultLogsPath { get; private set; } = string.Empty;
    public static string DefaultTempPath { get; private set; } = string.Empty;
    public static string DefaultDatabasePath { get; private set; } = string.Empty;

    public static void Initialize()
    {
        if (_initialized) return;

        if (IsRunningInContainer)
        {
            // Linux container paths
            DefaultUploadPath = "/app/Uploads";
            DefaultLogsPath = "/app/logs";
            DefaultTempPath = "/app/temp";
            DefaultDatabasePath = "/app/data";
        }
        else if (OperatingSystem.IsWindows())
        {
            // Windows desktop paths. Preserve the legacy "MatchLogicApi" folder name so existing
            // installations continue to find their uploaded files, LiteDB databases, cleansing
            // dictionaries, and exports at the paths the API has always used
            // (e.g. C:\ProgramData\MatchLogicApi\Uploads). Renaming this folder would silently
            // orphan every existing user's data.
            var basePath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "MatchLogicApi");
            DefaultUploadPath = Path.Combine(basePath, "Uploads");
            DefaultLogsPath = Path.Combine(basePath, "Logs");
            DefaultTempPath = Path.Combine(basePath, "temp");
            DefaultDatabasePath = Path.Combine(basePath, "Database");
        }
        else
        {
            // Linux non-container
            var basePath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "MatchLogic");
            DefaultUploadPath = Path.Combine(basePath, "uploads");
            DefaultLogsPath = Path.Combine(basePath, "logs");
            DefaultTempPath = Path.Combine(basePath, "temp");
            DefaultDatabasePath = Path.Combine(basePath, "data");
        }

        // Ensure directories exist
        EnsureDirectoryExists(DefaultUploadPath);
        EnsureDirectoryExists(DefaultLogsPath);
        EnsureDirectoryExists(DefaultTempPath);

        _initialized = true;
    }

    private static void EnsureDirectoryExists(string path)
    {
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
            Console.WriteLine($"Created directory: {path}");
        }
    }
}
