using FluentAssertions;
using MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

[Collection("MinIO")]
public class S3FileConnectorTests : IDisposable
{
    private readonly MinioContainerFixture _fixture;
    private readonly S3FileConnector _connector;
    private readonly string _tempDir;
    private readonly string _testPrefix;

    public S3FileConnectorTests(MinioContainerFixture fixture)
    {
        _fixture = fixture;
        _testPrefix = $"test-{Guid.NewGuid():N}/";
        var config = RemoteFileConnectionConfigBuilder
            .ForS3(fixture.BucketName, fixture.AccessKey, fixture.SecretKey, "us-east-1", fixture.Endpoint)
            .Build();
        _connector = new S3FileConnector(config, new Mock<ILogger>().Object);
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
        result.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TestConnectionAsync_InvalidBucket_ReturnsFalse()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForS3("non-existent-bucket", _fixture.AccessKey, _fixture.SecretKey, "us-east-1", _fixture.Endpoint)
            .Build();
        using var connector = new S3FileConnector(config, new Mock<ILogger>().Object);

        // Act
        var result = await connector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_EmptyPrefix_ReturnsAllFiles()
    {
        // Arrange — upload two files under the test prefix
        var file1 = TestFileHelper.CreateTestCsvFile(_tempDir, "file1.csv", 5);
        var file2 = TestFileHelper.CreateTestCsvFile(_tempDir, "file2.csv", 3);
        await _connector.UploadFileAsync(file1, _testPrefix + "file1.csv");
        await _connector.UploadFileAsync(file2, _testPrefix + "file2.csv");

        // Act — list at the test prefix (acts as "root" for this test run)
        var files = await _connector.ListFilesAsync(_testPrefix);

        // Assert
        files.Should().HaveCount(2);
        files.Select(f => f.Name).Should().BeEquivalentTo(["file1.csv", "file2.csv"]);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFilesAsync_WithPrefix_ReturnsFilteredFiles()
    {
        // Arrange — upload files into two different sub-prefixes
        var fileA = TestFileHelper.CreateTestCsvFile(_tempDir, "a.csv", 2);
        var fileB = TestFileHelper.CreateTestCsvFile(_tempDir, "b.csv", 2);
        await _connector.UploadFileAsync(fileA, _testPrefix + "subA/a.csv");
        await _connector.UploadFileAsync(fileB, _testPrefix + "subB/b.csv");

        // Act — list only subA
        var files = await _connector.ListFilesAsync(_testPrefix + "subA/");

        // Assert
        files.Should().ContainSingle();
        files[0].Name.Should().Be("a.csv");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ListFoldersAsync_WithCommonPrefixes_ReturnsFolderInfos()
    {
        // Arrange — upload files into two sub-folders to create common prefixes
        var fileA = TestFileHelper.CreateTestCsvFile(_tempDir, "a.csv", 2);
        var fileB = TestFileHelper.CreateTestCsvFile(_tempDir, "b.csv", 2);
        await _connector.UploadFileAsync(fileA, _testPrefix + "folderX/a.csv");
        await _connector.UploadFileAsync(fileB, _testPrefix + "folderY/b.csv");

        // Act — list folders at the test prefix
        var folders = await _connector.ListFoldersAsync(_testPrefix);

        // Assert
        folders.Should().HaveCount(2);
        folders.Select(f => f.Name).Should().BeEquivalentTo(["folderX", "folderY"]);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task UploadFileAsync_SmallFile_UploadsSuccessfully()
    {
        // Arrange
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "upload-test.csv", 10);
        var remoteKey = _testPrefix + "upload-test.csv";

        // Act
        var result = await _connector.UploadFileAsync(localFile, remoteKey);

        // Assert
        result.Should().Contain(_fixture.BucketName);
        result.Should().Contain("upload-test.csv");

        var exists = await _connector.FileExistsAsync(remoteKey);
        exists.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task DownloadFileAsync_ExistingKey_DownloadsCorrectly()
    {
        // Arrange — upload a file first
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "download-src.csv", 8);
        var remoteKey = _testPrefix + "download-src.csv";
        await _connector.UploadFileAsync(localFile, remoteKey);

        var downloadDir = Path.Combine(_tempDir, "downloads");
        Directory.CreateDirectory(downloadDir);

        // Act
        var downloadedPath = await _connector.DownloadFileAsync(remoteKey, downloadDir);

        // Assert
        File.Exists(downloadedPath).Should().BeTrue();
        new FileInfo(downloadedPath).Length.Should().BeGreaterThan(0);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task UploadThenDownload_RoundTrip_ContentMatches()
    {
        // Arrange
        var originalFile = TestFileHelper.CreateTestCsvFile(_tempDir, "roundtrip.csv", 15);
        var remoteKey = _testPrefix + "roundtrip.csv";

        // Act — upload then download
        await _connector.UploadFileAsync(originalFile, remoteKey);

        var downloadDir = Path.Combine(_tempDir, "roundtrip-download");
        Directory.CreateDirectory(downloadDir);
        var downloadedPath = await _connector.DownloadFileAsync(remoteKey, downloadDir);

        // Assert — binary contents must match
        TestFileHelper.AssertFileContentsMatch(originalFile, downloadedPath);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GetFileMetadataAsync_ExistingKey_ReturnsMetadata()
    {
        // Arrange — upload a file
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "metadata-test.csv", 5);
        var remoteKey = _testPrefix + "metadata-test.csv";
        await _connector.UploadFileAsync(localFile, remoteKey);
        var expectedSize = new FileInfo(localFile).Length;

        // Act
        var metadata = await _connector.GetFileMetadataAsync(remoteKey);

        // Assert
        metadata.Name.Should().Be("metadata-test.csv");
        metadata.Size.Should().Be(expectedSize);
        metadata.LastModified.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(5));
        metadata.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FileExistsAsync_ExistingKey_ReturnsTrue()
    {
        // Arrange — upload a file
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "exists-test.csv", 3);
        var remoteKey = _testPrefix + "exists-test.csv";
        await _connector.UploadFileAsync(localFile, remoteKey);

        // Act
        var exists = await _connector.FileExistsAsync(remoteKey);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FileExistsAsync_NonExistentKey_ReturnsFalse()
    {
        // Arrange
        var remoteKey = _testPrefix + "does-not-exist.csv";

        // Act
        var exists = await _connector.FileExistsAsync(remoteKey);

        // Assert
        exists.Should().BeFalse();
    }
}
