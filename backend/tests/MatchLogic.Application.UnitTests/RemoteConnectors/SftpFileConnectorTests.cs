using FluentAssertions;
using MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

[Collection("SFTP")]
public class SftpFileConnectorTests : IDisposable
{
    private readonly SftpContainerFixture _fixture;
    private readonly SftpFileConnector _connector;
    private readonly string _tempDir;

    public SftpFileConnectorTests(SftpContainerFixture fixture)
    {
        _fixture = fixture;
        _tempDir = TestFileHelper.CreateTestTempDir();

        var config = RemoteFileConnectionConfigBuilder
            .ForSftp(_fixture.Host, _fixture.Port, _fixture.Username, _fixture.Password)
            .Build();

        var logger = new Mock<ILogger>().Object;
        _connector = new SftpFileConnector(config, logger);
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
        result.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TestConnectionAsync_InvalidCredentials_ReturnsFalse()
    {
        // Arrange
        var badConfig = RemoteFileConnectionConfigBuilder
            .ForSftp(_fixture.Host, _fixture.Port, "wrong_user", "wrong_pass")
            .Build();

        using var badConnector = new SftpFileConnector(badConfig, new Mock<ILogger>().Object);

        // Act
        var result = await badConnector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_EmptyDirectory_ReturnsEmptyList()
    {
        // Arrange — create an empty subfolder to list
        var emptyFolder = $"/upload/empty_{Guid.NewGuid():N}";
        await _connector.CreateFolderAsync(emptyFolder);

        // Act
        var files = await _connector.ListFilesAsync(emptyFolder);

        // Assert
        files.Should().BeEmpty();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_WithFiles_ReturnsFileInfos()
    {
        // Arrange — upload two files into a known subfolder
        var folder = $"/upload/list_{Guid.NewGuid():N}";
        await _connector.CreateFolderAsync(folder);

        var localFile1 = TestFileHelper.CreateTestCsvFile(_tempDir, "file_a.csv", rows: 3);
        var localFile2 = TestFileHelper.CreateTestCsvFile(_tempDir, "file_b.csv", rows: 5);

        await _connector.UploadFileAsync(localFile1, $"{folder}/file_a.csv");
        await _connector.UploadFileAsync(localFile2, $"{folder}/file_b.csv");

        // Act
        var files = await _connector.ListFilesAsync(folder);

        // Assert
        files.Should().HaveCount(2);
        files.Select(f => f.Name).Should().BeEquivalentTo(new[] { "file_a.csv", "file_b.csv" });
        files.Should().AllSatisfy(f =>
        {
            f.Size.Should().BeGreaterThan(0);
            f.Extension.Should().Be(".csv");
        });
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFoldersAsync_WithSubfolders_ReturnsFolderInfos()
    {
        // Arrange — create a parent folder with two subfolders
        var parent = $"/upload/parent_{Guid.NewGuid():N}";
        await _connector.CreateFolderAsync(parent);
        await _connector.CreateFolderAsync($"{parent}/sub_alpha");
        await _connector.CreateFolderAsync($"{parent}/sub_beta");

        // Act
        var folders = await _connector.ListFoldersAsync(parent);

        // Assert
        folders.Should().HaveCount(2);
        folders.Select(f => f.Name).Should().BeEquivalentTo(new[] { "sub_alpha", "sub_beta" });
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task UploadFileAsync_SmallFile_UploadsSuccessfully()
    {
        // Arrange
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "upload_test.csv", rows: 5);
        var remotePath = $"/upload/upload_{Guid.NewGuid():N}.csv";

        // Act
        var returnedPath = await _connector.UploadFileAsync(localFile, remotePath);

        // Assert
        returnedPath.Should().Be(remotePath);
        var exists = await _connector.FileExistsAsync(remotePath);
        exists.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task DownloadFileAsync_ExistingFile_DownloadsCorrectly()
    {
        // Arrange — upload a file first so we have something to download
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "to_download.csv", rows: 8);
        var remotePath = $"/upload/dl_{Guid.NewGuid():N}.csv";
        await _connector.UploadFileAsync(localFile, remotePath);

        var downloadDir = Path.Combine(_tempDir, "downloads");
        Directory.CreateDirectory(downloadDir);

        // Act
        var downloadedPath = await _connector.DownloadFileAsync(remotePath, downloadDir);

        // Assert
        File.Exists(downloadedPath).Should().BeTrue();
        new FileInfo(downloadedPath).Length.Should().BeGreaterThan(0);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task UploadThenDownload_RoundTrip_ContentMatches()
    {
        // Arrange
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "roundtrip.csv", rows: 15);
        var remotePath = $"/upload/rt_{Guid.NewGuid():N}.csv";

        // Act — upload then download
        await _connector.UploadFileAsync(localFile, remotePath);

        var downloadDir = Path.Combine(_tempDir, "roundtrip_dl");
        Directory.CreateDirectory(downloadDir);
        var downloadedPath = await _connector.DownloadFileAsync(remotePath, downloadDir);

        // Assert — binary content should be identical
        TestFileHelper.AssertFileContentsMatch(localFile, downloadedPath);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task CreateFolderAsync_NewFolder_CreatesSuccessfully()
    {
        // Arrange
        var folderPath = $"/upload/new_{Guid.NewGuid():N}";

        // Act
        await _connector.CreateFolderAsync(folderPath);

        // Assert — listing the parent should reveal the new folder
        var folders = await _connector.ListFoldersAsync("/upload");
        folders.Select(f => f.Path).Should().Contain(folderPath);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FileExistsAsync_ExistingFile_ReturnsTrue()
    {
        // Arrange — upload a file
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "exists_check.csv", rows: 3);
        var remotePath = $"/upload/exists_{Guid.NewGuid():N}.csv";
        await _connector.UploadFileAsync(localFile, remotePath);

        // Act
        var exists = await _connector.FileExistsAsync(remotePath);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FileExistsAsync_NonExistent_ReturnsFalse()
    {
        // Arrange
        var remotePath = $"/upload/no_such_file_{Guid.NewGuid():N}.csv";

        // Act
        var exists = await _connector.FileExistsAsync(remotePath);

        // Assert
        exists.Should().BeFalse();
    }
}
