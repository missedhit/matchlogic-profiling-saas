using FluentAssertions;
using MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

[Collection("Azurite")]
public class AzureBlobFileConnectorTests : IDisposable
{
    private readonly AzuriteContainerFixture _fixture;
    private readonly AzureBlobFileConnector _connector;
    private readonly string _tempDir;
    private readonly string _testPrefix;

    public AzureBlobFileConnectorTests(AzuriteContainerFixture fixture)
    {
        _fixture = fixture;
        _testPrefix = $"test-{Guid.NewGuid():N}/"; // unique prefix per test run

        var config = RemoteFileConnectionConfigBuilder
            .ForAzureBlob(fixture.ConnectionString, fixture.ContainerName)
            .Build();

        _connector = new AzureBlobFileConnector(config, new Mock<ILogger>().Object);
        _tempDir = TestFileHelper.CreateTestTempDir();
    }

    public void Dispose()
    {
        _connector.Dispose();
        TestFileHelper.CleanupTestDir(_tempDir);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TestConnectionAsync_ValidConnection_ReturnsTrue()
    {
        // Act
        var result = await _connector.TestConnectionAsync();

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TestConnectionAsync_InvalidContainer_ReturnsFalse()
    {
        // Arrange
        var badConfig = RemoteFileConnectionConfigBuilder
            .ForAzureBlob(_fixture.ConnectionString, "nonexistent-container")
            .Build();

        using var badConnector = new AzureBlobFileConnector(badConfig, new Mock<ILogger>().Object);

        // Act
        var result = await badConnector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_EmptyContainer_ReturnsEmptyList()
    {
        // Arrange — use the unique test prefix which has no blobs under it yet
        var emptyPrefix = $"empty-{Guid.NewGuid():N}";

        // Act
        var files = await _connector.ListFilesAsync(emptyPrefix);

        // Assert
        files.Should().BeEmpty();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_WithBlobs_ReturnsFileInfos()
    {
        // Arrange — upload two blobs into the test prefix
        var localFile1 = TestFileHelper.CreateTestCsvFile(_tempDir, "file_a.csv", rows: 3);
        var localFile2 = TestFileHelper.CreateTestCsvFile(_tempDir, "file_b.csv", rows: 5);

        await _connector.UploadFileAsync(localFile1, _testPrefix + "file_a.csv");
        await _connector.UploadFileAsync(localFile2, _testPrefix + "file_b.csv");

        // Act
        var files = await _connector.ListFilesAsync(_testPrefix);

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
    public async Task ListFoldersAsync_WithVirtualDirs_ReturnsFolderInfos()
    {
        // Arrange — upload blobs into two virtual sub-folders under the test prefix
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "nested.csv", rows: 2);

        await _connector.UploadFileAsync(localFile, _testPrefix + "sub_alpha/nested.csv");
        await _connector.UploadFileAsync(localFile, _testPrefix + "sub_beta/nested.csv");

        // Act
        var folders = await _connector.ListFoldersAsync(_testPrefix);

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
        var remotePath = _testPrefix + "upload_test.csv";

        // Act
        var returnedUrl = await _connector.UploadFileAsync(localFile, remotePath);

        // Assert
        returnedUrl.Should().NotBeNullOrWhiteSpace();
        var exists = await _connector.FileExistsAsync(remotePath);
        exists.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task DownloadFileAsync_ExistingBlob_DownloadsCorrectly()
    {
        // Arrange — upload a blob first so we have something to download
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "to_download.csv", rows: 8);
        var remotePath = _testPrefix + "to_download.csv";
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
        var remotePath = _testPrefix + "roundtrip.csv";

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
    public async Task GetFileMetadataAsync_ExistingBlob_ReturnsMetadata()
    {
        // Arrange — upload a blob so we can query its metadata
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "metadata_test.csv", rows: 5);
        var remotePath = _testPrefix + "metadata_test.csv";
        await _connector.UploadFileAsync(localFile, remotePath);

        var expectedSize = new FileInfo(localFile).Length;

        // Act
        var metadata = await _connector.GetFileMetadataAsync(remotePath);

        // Assert
        metadata.Should().NotBeNull();
        metadata.Name.Should().Be("metadata_test.csv");
        metadata.Size.Should().Be(expectedSize);
        metadata.LastModified.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(5));
        metadata.ETag.Should().NotBeNullOrWhiteSpace();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FileExistsAsync_ExistingBlob_ReturnsTrue()
    {
        // Arrange — upload a blob
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "exists_check.csv", rows: 3);
        var remotePath = _testPrefix + "exists_check.csv";
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
        var remotePath = _testPrefix + $"no_such_file_{Guid.NewGuid():N}.csv";

        // Act
        var exists = await _connector.FileExistsAsync(remotePath);

        // Assert
        exists.Should().BeFalse();
    }
}
