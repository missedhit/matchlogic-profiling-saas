using FluentAssertions;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Import.RemoteConnectors;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

public class RemoteFileConnectorFactoryTests
{
    private readonly RemoteFileConnectorFactory _factory;
    private readonly ILogger _logger;

    public RemoteFileConnectorFactoryTests()
    {
        _factory = new RemoteFileConnectorFactory();
        _logger = new Mock<ILogger>().Object;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_FtpType_ReturnsFtpConnector()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForFtp("localhost", 21, "user", "pass")
            .Build();

        // Act
        var result = _factory.Create(DataSourceType.FTP, config, _logger);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<FtpFileConnector>();
        result.Dispose();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_SftpType_ReturnsSftpConnector()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForSftp("localhost", 22, "user", "pass")
            .Build();

        // Act
        var result = _factory.Create(DataSourceType.SFTP, config, _logger);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<SftpFileConnector>();
        result.Dispose();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_S3Type_ReturnsS3Connector()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForS3("test-bucket", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1")
            .Build();

        // Act
        var result = _factory.Create(DataSourceType.S3, config, _logger);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<S3FileConnector>();
        result.Dispose();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_AzureBlobType_ReturnsAzureBlobConnector()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForAzureBlob("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net", "test-container")
            .Build();

        // Act
        var result = _factory.Create(DataSourceType.AzureBlob, config, _logger);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<AzureBlobFileConnector>();
        result.Dispose();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_GoogleDriveType_ReturnsGoogleDriveConnector()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForGoogleDrive("ya29.test-access-token")
            .Build();

        // Act
        var result = _factory.Create(DataSourceType.GoogleDrive, config, _logger);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<GoogleDriveFileConnector>();
        result.Dispose();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_DropboxType_ReturnsDropboxConnector()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForDropbox("sl.test-access-token")
            .Build();

        // Act
        var result = _factory.Create(DataSourceType.Dropbox, config, _logger);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<DropboxFileConnector>();
        result.Dispose();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_OneDriveType_ReturnsOneDriveConnector()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForOneDrive("EwB0A8l6BAAL-test-token")
            .Build();

        // Act
        var result = _factory.Create(DataSourceType.OneDrive, config, _logger);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<OneDriveFileConnector>();
        result.Dispose();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Create_UnsupportedType_ThrowsArgumentException()
    {
        // Arrange
        var config = RemoteFileConnectionConfigBuilder
            .ForFtp("localhost", 21, "user", "pass")
            .Build();

        // Act
        var act = () => _factory.Create(DataSourceType.CSV, config, _logger);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("No remote file connector found for DataSourceType: CSV");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsSupported_FtpType_ReturnsTrue()
    {
        // Act
        var result = _factory.IsSupported(DataSourceType.FTP);

        // Assert
        result.Should().BeTrue("FTP is a supported remote connector type");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsSupported_CsvType_ReturnsFalse()
    {
        // Act
        var result = _factory.IsSupported(DataSourceType.CSV);

        // Assert
        result.Should().BeFalse("CSV is not a remote connector type");
    }
}
