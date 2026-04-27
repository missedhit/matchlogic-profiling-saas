using FluentAssertions;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

public class OneDriveFileConnectorTests : IDisposable
{
    private readonly string _tempDir;

    public OneDriveFileConnectorTests()
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
        var act = () => new OneDriveFileConnector(null!, logger);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .And.ParamName.Should().Be("config");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("some-token").Build();

        // Act
        var act = () => new OneDriveFileConnector(config, null!);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .And.ParamName.Should().Be("logger");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_ValidParams_CreatesInstance()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("test-token").Build();
        var logger = new Mock<ILogger>().Object;

        // Act
        var connector = new OneDriveFileConnector(config, logger);

        // Assert
        connector.Should().NotBeNull();
        connector.Dispose();
    }

    #endregion

    #region Empty Token — InvalidOperationException

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectionAsync_EmptyToken_ReturnsFalse()
    {
        // Arrange — TestConnectionAsync catches ALL exceptions (including the
        // InvalidOperationException from GetGraphClient) and returns false
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var result = await connector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse(
            "an empty token triggers InvalidOperationException inside GetGraphClient, " +
            "which TestConnectionAsync catches and converts to false");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ListFilesAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = async () => await connector.ListFilesAsync("/");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*not configured*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ListFoldersAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = async () => await connector.ListFoldersAsync("/");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*not configured*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DownloadFileAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = async () => await connector.DownloadFileAsync("/test.csv", _tempDir);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*not configured*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UploadFileAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);
        var localFile = TestFileHelper.CreateTestCsvFile(_tempDir, "upload_test.csv", rows: 3);

        // Act
        var act = async () => await connector.UploadFileAsync(localFile, "/upload/test.csv");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*not configured*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetFileMetadataAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = async () => await connector.GetFileMetadataAsync("/test.csv");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*not configured*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task FileExistsAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = async () => await connector.FileExistsAsync("/test.csv");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*not configured*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CreateFolderAsync_EmptyToken_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = async () => await connector.CreateFolderAsync("/new-folder");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*access token*not configured*");
    }

    #endregion

    #region Invalid Token — Graceful Failure

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectionAsync_InvalidToken_ReturnsFalse()
    {
        // Arrange — a fake token will cause Graph API to reject the request,
        // and TestConnectionAsync catches all exceptions and returns false
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("fake-onedrive-token-12345").Build();
        using var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var result = await connector.TestConnectionAsync();

        // Assert
        result.Should().BeFalse("an invalid token should cause the Graph API call to fail, returning false");
    }

    #endregion

    #region Dispose Safety

    [Fact]
    [Trait("Category", "Unit")]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder.ForOneDrive("test-token").Build();
        var connector = new OneDriveFileConnector(config, new Mock<ILogger>().Object);

        // Act
        var act = () =>
        {
            connector.Dispose();
            connector.Dispose();
            connector.Dispose();
        };

        // Assert
        act.Should().NotThrow("disposing multiple times should be safe and idempotent");
    }

    #endregion
}
