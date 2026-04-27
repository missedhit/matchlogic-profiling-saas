using System.Globalization;
using System.Text;

namespace MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;

/// <summary>
/// Helper utilities for creating test CSV files, comparing file contents,
/// and managing temporary directories during test runs.
/// </summary>
public static class TestFileHelper
{
    /// <summary>
    /// Creates a small test CSV file at the given path with the specified number of rows.
    /// The file contains columns: Id, Name, Value.
    /// </summary>
    /// <param name="directory">Directory where the file will be created.</param>
    /// <param name="fileName">Name of the CSV file (default: "test.csv").</param>
    /// <param name="rows">Number of data rows to generate (default: 10).</param>
    /// <returns>The full path to the created file.</returns>
    public static string CreateTestCsvFile(string directory, string fileName = "test.csv", int rows = 10)
    {
        Directory.CreateDirectory(directory);

        var filePath = Path.Combine(directory, fileName);
        var sb = new StringBuilder();
        sb.AppendLine("Id,Name,Value");

        for (var i = 1; i <= rows; i++)
        {
            var value = (i * 100.1).ToString("F1", CultureInfo.InvariantCulture);
            sb.AppendLine($"{i},Name_{i},{value}");
        }

        File.WriteAllText(filePath, sb.ToString(), Encoding.UTF8);
        return filePath;
    }

    /// <summary>
    /// Asserts that two files have identical content by performing a binary comparison.
    /// Throws <see cref="Xunit.Sdk.EqualException"/> if the files differ.
    /// </summary>
    /// <param name="path1">Path to the first file.</param>
    /// <param name="path2">Path to the second file.</param>
    public static void AssertFileContentsMatch(string path1, string path2)
    {
        var bytes1 = File.ReadAllBytes(path1);
        var bytes2 = File.ReadAllBytes(path2);

        Assert.Equal(bytes1.Length, bytes2.Length);
        Assert.Equal(bytes1, bytes2);
    }

    /// <summary>
    /// Creates a unique temporary directory for tests and returns its path.
    /// The directory is placed under the system temp folder in a MatchLogic_Tests subfolder.
    /// </summary>
    /// <returns>The full path to the newly created temporary directory.</returns>
    public static string CreateTestTempDir()
    {
        var path = Path.Combine(Path.GetTempPath(), "MatchLogic_Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    /// <summary>
    /// Cleans up a test temporary directory by deleting it recursively.
    /// Silently ignores errors (e.g., if the directory no longer exists or files are locked).
    /// </summary>
    /// <param name="path">Path to the directory to delete.</param>
    public static void CleanupTestDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
            // Swallow errors during cleanup to avoid masking test failures.
        }
    }
}
