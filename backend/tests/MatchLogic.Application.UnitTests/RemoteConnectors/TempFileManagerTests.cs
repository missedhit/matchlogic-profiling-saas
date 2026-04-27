using FluentAssertions;
using MatchLogic.Infrastructure.Import;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

public class TempFileManagerTests : IDisposable
{
    private readonly TempFileManager _manager;
    private readonly List<string> _createdPaths = new();

    public TempFileManagerTests()
    {
        var logger = new Mock<ILogger<TempFileManager>>().Object;
        _manager = new TempFileManager(logger);
    }

    public void Dispose()
    {
        // Cleanup any created temp paths
        foreach (var path in _createdPaths)
        {
            try
            {
                var dir = Path.GetDirectoryName(path);
                if (dir != null && Directory.Exists(dir))
                    Directory.Delete(dir, true);
            }
            catch
            {
                // Best-effort cleanup
            }
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateTempPath_ReturnsValidPath()
    {
        // Arrange
        var dataSourceId = Guid.NewGuid();
        var fileName = "test-data.csv";

        // Act
        var result = _manager.CreateTempPath(dataSourceId, fileName);
        _createdPaths.Add(result);

        // Assert
        result.Should().NotBeNullOrWhiteSpace();
        Path.GetDirectoryName(result).Should().NotBeNullOrWhiteSpace();
        Directory.Exists(Path.GetDirectoryName(result)).Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateTempPath_CreatesDirectory()
    {
        // Arrange
        var dataSourceId = Guid.NewGuid();
        var fileName = "sample.csv";

        // Act
        var result = _manager.CreateTempPath(dataSourceId, fileName);
        _createdPaths.Add(result);

        // Assert
        var parentDir = Path.GetDirectoryName(result);
        parentDir.Should().NotBeNull();
        Directory.Exists(parentDir).Should().BeTrue();
        parentDir.Should().Contain(dataSourceId.ToString("N"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateTempPath_SanitizesFileName()
    {
        // Arrange
        var dataSourceId = Guid.NewGuid();
        var unsafeFileName = "file:name?.csv";

        // Act
        var result = _manager.CreateTempPath(dataSourceId, unsafeFileName);
        _createdPaths.Add(result);

        // Assert
        var actualFileName = Path.GetFileName(result);
        actualFileName.Should().NotContain(":");
        actualFileName.Should().NotContain("?");
        actualFileName.Should().EndWith(".csv");
        // The invalid characters should be replaced with underscores
        actualFileName.Should().Contain("file_name_");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CleanupTempFile_ExistingFile_DeletesFile()
    {
        // Arrange
        var dataSourceId = Guid.NewGuid();
        var tempPath = _manager.CreateTempPath(dataSourceId, "to-delete.csv");
        _createdPaths.Add(tempPath);

        // Write a real file so there is something to delete
        File.WriteAllText(tempPath, "id,name\n1,Alice\n2,Bob");
        File.Exists(tempPath).Should().BeTrue("file should exist before cleanup");

        // Act
        _manager.CleanupTempFile(tempPath);

        // Assert
        File.Exists(tempPath).Should().BeFalse("file should be deleted after cleanup");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CleanupTempFile_NonExistent_DoesNotThrow()
    {
        // Arrange
        var fakePath = Path.Combine(Path.GetTempPath(), "MatchLogic", "nonexistent", "ghost.csv");

        // Act
        var act = () => _manager.CleanupTempFile(fakePath);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CleanupDataSourceTemp_DeletesAllFiles()
    {
        // Arrange
        var dataSourceId = Guid.NewGuid();
        var tempPath = _manager.CreateTempPath(dataSourceId, "file1.csv");
        // Don't add to _createdPaths since CleanupDataSourceTemp should remove it
        File.WriteAllText(tempPath, "col1,col2\nval1,val2");

        var dsDir = Path.GetDirectoryName(tempPath)!;
        Directory.Exists(dsDir).Should().BeTrue("data source directory should exist before cleanup");

        // Act
        _manager.CleanupDataSourceTemp(dataSourceId);

        // Assert
        Directory.Exists(dsDir).Should().BeFalse("data source directory should be removed after cleanup");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTempDirSizeAsync_ReturnsSize()
    {
        // Arrange
        var dataSourceId = Guid.NewGuid();
        var tempPath = _manager.CreateTempPath(dataSourceId, "sized-file.csv");
        _createdPaths.Add(tempPath);

        var content = new string('X', 1024); // 1 KB of content
        File.WriteAllText(tempPath, content);

        // Act
        var size = await _manager.GetTempDirSizeAsync();

        // Assert
        size.Should().BeGreaterThan(0, "temp directory should have non-zero size after writing a file");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task PurgeOrphanedFilesAsync_RemovesOldDirectories()
    {
        // Arrange
        var dataSourceId = Guid.NewGuid();
        var tempPath = _manager.CreateTempPath(dataSourceId, "orphaned.csv");
        File.WriteAllText(tempPath, "stale,data\n1,old");

        var dsDir = Path.GetDirectoryName(tempPath)!;
        Directory.Exists(dsDir).Should().BeTrue();

        // Backdate the directory's LastWriteTimeUtc to 2 days ago
        var twoDaysAgo = DateTime.UtcNow.AddDays(-2);
        Directory.SetLastWriteTimeUtc(dsDir, twoDaysAgo);

        // Act — purge anything older than 1 hour
        var purgedCount = await _manager.PurgeOrphanedFilesAsync(TimeSpan.FromHours(1));

        // Assert
        purgedCount.Should().BeGreaterThanOrEqualTo(1, "at least the backdated directory should be purged");
        Directory.Exists(dsDir).Should().BeFalse("the old directory should have been deleted");
    }
}
