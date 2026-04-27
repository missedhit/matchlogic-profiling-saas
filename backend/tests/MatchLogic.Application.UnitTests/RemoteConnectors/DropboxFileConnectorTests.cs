using FluentAssertions;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

public class DropboxFileConnectorTests : IDisposable
{
    private readonly string _tempDir;

    public DropboxFileConnectorTests()
    {
        _tempDir = TestFileHelper.CreateTestTempDir();
    }

    public void Dispose()
    {
        TestFileHelper.CleanupTestDir(_tempDir);
    }

    #region Constructor Validation

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_NullConfig_ThrowsArgumentNullException()
    {
        // Arrange
        var logger = new Mock<ILogger>().Object;

        // Act
        var act = () => new DropboxFileConnector(null!, logger);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("config");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("some-token").Build();

        // Act
        var act = () => new DropboxFileConnector(config, null!);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("logger");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_ValidParams_CreatesInstance()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("valid-token").Build();
        var logger = new Mock<ILogger>().Object;

        // Act
        using var connector = new DropboxFileConnector(config, logger);

        // Assert
        connector.Should().NotBeNull();
    }

    #endregion

    #region Empty Token — GetClient() Throws InvalidOperationException

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectionAsync_EmptyToken_ReturnsFalse()
    {
        // Arrange — TestConnectionAsync catches all exceptions (including the
        // InvalidOperationException from GetClient()) and returns false.
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var result = await connector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse("an empty access token should cause GetClient() to throw internally, which TestConnectionAsync catches and returns false");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ListFilesAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.ListFilesAsync("/some-path");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Dropbox access token is not configured.");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ListFoldersAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.ListFoldersAsync("/some-path");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Dropbox access token is not configured.");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DownloadFileAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.DownloadFileAsync("/remote/file.csv", _tempDir);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Dropbox access token is not configured.");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UploadFileAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);
        var testFile = TestFileHelper.CreateTestCsvFile(_tempDir, "upload_test.csv", 5);

        // Act
        var act = () => connector.UploadFileAsync(testFile, "/remote/upload_test.csv");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Dropbox access token is not configured.");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetFileMetadataAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.GetFileMetadataAsync("/remote/file.csv");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Dropbox access token is not configured.");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task FileExistsAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.FileExistsAsync("/remote/file.csv");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Dropbox access token is not configured.");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CreateFolderAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () => connector.CreateFolderAsync("/remote/new-folder");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Dropbox access token is not configured.");
    }

    #endregion

    #region Invalid Token — TestConnectionAsync Returns False

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectionAsync_InvalidToken_ReturnsFalse()
    {
        // Arrange — a fake token will cause the Dropbox API call to fail,
        // and TestConnectionAsync catches all exceptions and returns false.
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("fake-dropbox-token-12345").Build();
        using var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var result = await connector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse("an invalid/fake access token should fail the Dropbox connection test");
    }

    #endregion

    #region Dispose Safety

    [Fact]
    [Trait("Category", "Unit")]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForDropbox("some-token").Build();
        var connector = new DropboxFileConnector(config, new Mock<ILogger>().Object);

        // Act — dispose multiple times
        var act = () =>
        {
            connector.Dispose();
            connector.Dispose();
            connector.Dispose();
        };

        // Assert
        act.Should().NotThrow("calling Dispose multiple times should be safe and idempotent");
    }

    #endregion
}
