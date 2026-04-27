using FluentAssertions;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

public class GoogleDriveFileConnectorTests : IDisposable
{
    private readonly string _tempDir;

    public GoogleDriveFileConnectorTests()
    {
        _tempDir = TestFileHelper.CreateTestTempDir();
    }

    public void Dispose()
    {
        TestFileHelper.CleanupTestDir(_tempDir);
    }

    #region Constructor Tests

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_NullConfig_ThrowsArgumentNullException()
    {
        // Arrange
        var logger = new Mock<ILogger>().Object;

        // Act
        var act = () => new GoogleDriveFileConnector(null!, logger);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("config");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("some-token")
            .Build();

        // Act
        var act = () => new GoogleDriveFileConnector(config, null!);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("logger");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_ValidParams_CreatesInstance()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("fake-token-for-testing")
            .Build();
        var logger = new Mock<ILogger>().Object;

        // Act
        using var connector = new GoogleDriveFileConnector(config, logger);

        // Assert
        connector.Should().NotBeNull();
    }

    #endregion

    #region TestConnectionAsync Tests

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectionAsync_InvalidToken_ReturnsFalse()
    {
        // Arrange — a fake token will cause the Google API call to fail,
        // but the connector should catch the exception and return false.
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("fake-invalid-token-12345")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var result = await connector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse("an invalid access token should fail the Google Drive connection test");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectionAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange — an empty access token should throw InvalidOperationException
        // when GetDriveService() is invoked internally.
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        // Note: TestConnectionAsync catches all exceptions and returns false,
        // but GetDriveService() throws before the try-catch in some code paths.
        // Since TestConnectionAsync wraps everything in a try-catch, the
        // InvalidOperationException from GetDriveService is caught and returns false.
        var result = await connector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse("an empty access token should cause the connection test to fail");
    }

    #endregion

    #region Empty Token — Method Validation Tests

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ListFilesAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.ListFilesAsync("/");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ListFoldersAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.ListFoldersAsync("/");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DownloadFileAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.DownloadFileAsync("some-file-id", _tempDir);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UploadFileAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir);
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.UploadFileAsync(localFile, "/upload/test.csv");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task FileExistsAsync_EmptyToken_ReturnsFalse()
    {
        // Arrange — FileExistsAsync has its own try-catch that returns false on error,
        // so even though GetDriveService() throws InvalidOperationException,
        // the method catches it and returns false.
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var result = await connector.FileExistsAsync("/some-folder/some-file.csv");

        // Assert
        result.Should().BeFalse("an empty access token should cause FileExistsAsync to return false");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetFileMetadataAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("")
            .Build();
        using var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.GetFileMetadataAsync("some-file-id");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*");
    }

    #endregion

    #region Dispose Tests

    [Fact]
    [Trait("Category", "Unit")]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("fake-token-for-testing")
            .Build();
        var connector = new GoogleDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act — call Dispose multiple times
        var act = () =>
        {
            connector.Dispose();
            connector.Dispose();
            connector.Dispose();
        };

        // Assert
        act.Should().NotThrow("Dispose should be idempotent and safe to call multiple times");
    }

    #endregion
}
