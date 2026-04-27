using FluentAssertions;
using MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

[Collection("FTP")]
public class FtpFileConnectorTests : IDisposable
{
    private readonly FtpContainerFixture _fixture;
    private readonly FtpFileConnector _connector;
    private readonly string _tempDir;

    public FtpFileConnectorTests(FtpContainerFixture fixture)
    {
        _fixture = fixture;
        var config = RemoteFileConnectionConfigBuilder
            .ForFtp(fixture.Host, fixture.Port, fixture.Username, fixture.Password)
            .Build();
        _connector = new FtpFileConnector(config, new Mock<ILogger>().Object);
        _tempDir = TestFileHelper.CreateTestTempDir();
    }

    public void Dispose()
    {
        _connector.Dispose();
        TestFileHelper.CleanupTestDir(_tempDir);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TestConnectionAsync_ValidCredentials_ReturnsTrue()
    {
        // Act
        var result = await _connector.TestConnectionAsync();

        // Assert
        result.Should().BeTrue("valid credentials should establish a successful FTP connection");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TestConnectionAsync_InvalidCredentials_ReturnsFalse()
    {
        // Arrange
        var badConfig = RemoteFileConnectionConfigBuilder
            .ForFtp(_fixture.Host, _fixture.Port, _fixture.Username, "wrong_password")
            .Build();
        using var badConnector = new FtpFileConnector(badConfig, new Mock<ILogger>().Object);

        // Act
        var result = await badConnector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse("invalid credentials should fail the FTP connection test");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_EmptyDirectory_ReturnsEmptyList()
    {
        // Arrange — create a fresh empty subdirectory to avoid interference from other tests
        var emptyDir = $"/upload/empty_{Guid.NewGuid():N}";
        await _connector.CreateFolderAsync(emptyDir);

        // Act
        var files = await _connector.ListFilesAsync(emptyDir);

        // Assert
        files.Should().NotBeNull();
        files.Should().BeEmpty("a newly created directory should contain no files");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_WithFiles_ReturnsFileInfos()
    {
        // Arrange — upload a file first so the directory is not empty
        var testFile = TestFileHelper.CreateTestCsvFile(_tempDir, "list_test.csv", 5);
        var remotePath = $"/upload/list_test_{Guid.NewGuid():N}.csv";
        await _connector.UploadFileAsync(testFile, remotePath);

        // Act
        var files = await _connector.ListFilesAsync("/upload");

        // Assert
        files.Should().NotBeNull();
        files.Should().Contain(f => f.Name == Path.GetFileName(remotePath),
            "the uploaded file should appear in the directory listing");

        var match = files.First(f => f.Name == Path.GetFileName(remotePath));
        match.Size.Should().BeGreaterThan(0, "the listed file should report a positive size");
        match.Extension.Should().Be(".csv");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFoldersAsync_WithSubfolders_ReturnsFolderInfos()
    {
        // Arrange — create a subfolder inside /upload
        var folderName = $"subfolder_{Guid.NewGuid():N}";
        var remoteFolderPath = $"/upload/{folderName}";
        await _connector.CreateFolderAsync(remoteFolderPath);

        // Act
        var folders = await _connector.ListFoldersAsync("/upload");

        // Assert
        folders.Should().NotBeNull();
        folders.Should().Contain(f => f.Name == folderName,
            "the created subfolder should appear in the folder listing");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task UploadFileAsync_SmallFile_UploadsSuccessfully()
    {
        // Arrange
        var testFile = TestFileHelper.CreateTestCsvFile(_tempDir, "upload_test.csv", 10);
        var remotePath = $"/upload/upload_test_{Guid.NewGuid():N}.csv";

        // Act
        var resultPath = await _connector.UploadFileAsync(testFile, remotePath);

        // Assert
        resultPath.Should().NotBeNullOrEmpty("upload should return the remote path");
        var exists = await _connector.FileExistsAsync(remotePath);
        exists.Should().BeTrue("the file should exist on the FTP server after upload");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task DownloadFileAsync_ExistingFile_DownloadsCorrectly()
    {
        // Arrange — upload a file first
        var testFile = TestFileHelper.CreateTestCsvFile(_tempDir, "download_source.csv", 8);
        var remotePath = $"/upload/download_test_{Guid.NewGuid():N}.csv";
        await _connector.UploadFileAsync(testFile, remotePath);

        var downloadDir = Path.Combine(_tempDir, "downloads");
        Directory.CreateDirectory(downloadDir);

        // Act
        var localPath = await _connector.DownloadFileAsync(remotePath, downloadDir);

        // Assert
        localPath.Should().NotBeNullOrEmpty("download should return a local file path");
        File.Exists(localPath).Should().BeTrue("the downloaded file should exist on disk");
        new FileInfo(localPath).Length.Should().BeGreaterThan(0, "the downloaded file should not be empty");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task UploadThenDownload_RoundTrip_ContentMatches()
    {
        // Arrange — create a CSV file with known content
        var testFile = TestFileHelper.CreateTestCsvFile(_tempDir, "roundtrip.csv", 15);
        var remotePath = $"/upload/roundtrip_{Guid.NewGuid():N}.csv";

        // Act — upload then download
        await _connector.UploadFileAsync(testFile, remotePath);

        var downloadDir = Path.Combine(_tempDir, "roundtrip_download");
        Directory.CreateDirectory(downloadDir);
        var downloadedPath = await _connector.DownloadFileAsync(remotePath, downloadDir);

        // Assert — binary-compare the original and downloaded files
        TestFileHelper.AssertFileContentsMatch(testFile, downloadedPath);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task CreateFolderAsync_NewFolder_CreatesSuccessfully()
    {
        // Arrange
        var folderName = $"new_folder_{Guid.NewGuid():N}";
        var remoteFolderPath = $"/upload/{folderName}";

        // Act
        await _connector.CreateFolderAsync(remoteFolderPath);

        // Assert — verify the folder appears in the listing
        var folders = await _connector.ListFoldersAsync("/upload");
        folders.Should().Contain(f => f.Name == folderName,
            "the newly created folder should appear in the parent directory listing");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FileExistsAsync_ExistingFile_ReturnsTrue()
    {
        // Arrange — upload a file so we know it exists
        var testFile = TestFileHelper.CreateTestCsvFile(_tempDir, "exists_test.csv", 3);
        var remotePath = $"/upload/exists_test_{Guid.NewGuid():N}.csv";
        await _connector.UploadFileAsync(testFile, remotePath);

        // Act
        var exists = await _connector.FileExistsAsync(remotePath);

        // Assert
        exists.Should().BeTrue("a file that was just uploaded should be reported as existing");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FileExistsAsync_NonExistent_ReturnsFalse()
    {
        // Arrange — use a path that definitely does not exist
        var remotePath = $"/upload/nonexistent_{Guid.NewGuid():N}.csv";

        // Act
        var exists = await _connector.FileExistsAsync(remotePath);

        // Assert
        exists.Should().BeFalse("a file that was never uploaded should not be reported as existing");
    }
}
