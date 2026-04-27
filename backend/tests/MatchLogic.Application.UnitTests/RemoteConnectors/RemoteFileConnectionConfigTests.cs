using FluentAssertions;
using MatchLogic.Domain.Import;

namespace MatchLogic.Application.UnitTests.RemoteConnectors;

public class RemoteFileConnectionConfigTests
{
    #region CanCreateFromArgs — Supported types

    [Fact]
    [Trait("Category", "Unit")]
    public void CanCreateFromArgs_FtpType_ReturnsTrue()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();

        // Act
        var result = config.CanCreateFromArgs(DataSourceType.FTP);

        // Assert
        result.Should().BeTrue("FTP is a supported remote file type");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CanCreateFromArgs_SftpType_ReturnsTrue()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();

        // Act
        var result = config.CanCreateFromArgs(DataSourceType.SFTP);

        // Assert
        result.Should().BeTrue("SFTP is a supported remote file type");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CanCreateFromArgs_S3Type_ReturnsTrue()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();

        // Act
        var result = config.CanCreateFromArgs(DataSourceType.S3);

        // Assert
        result.Should().BeTrue("S3 is a supported remote file type");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CanCreateFromArgs_AzureBlobType_ReturnsTrue()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();

        // Act
        var result = config.CanCreateFromArgs(DataSourceType.AzureBlob);

        // Assert
        result.Should().BeTrue("AzureBlob is a supported remote file type");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CanCreateFromArgs_GoogleDriveType_ReturnsTrue()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();

        // Act
        var result = config.CanCreateFromArgs(DataSourceType.GoogleDrive);

        // Assert
        result.Should().BeTrue("GoogleDrive is a supported remote file type");
    }

    #endregion

    #region CanCreateFromArgs — Unsupported types

    [Fact]
    [Trait("Category", "Unit")]
    public void CanCreateFromArgs_CsvType_ReturnsFalse()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();

        // Act
        var result = config.CanCreateFromArgs(DataSourceType.CSV);

        // Assert
        result.Should().BeFalse("CSV is not a remote file type");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CanCreateFromArgs_ExcelType_ReturnsFalse()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();

        // Act
        var result = config.CanCreateFromArgs(DataSourceType.Excel);

        // Assert
        result.Should().BeFalse("Excel is not a remote file type");
    }

    #endregion

    #region CreateFromArgs — FTP / SFTP

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_FtpWithHost_Succeeds()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.HostKey, "ftp.example.com" }
        };

        // Act
        var result = config.CreateFromArgs(DataSourceType.FTP, args);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeSameAs(config, "CreateFromArgs returns this");
        config.Parameters.Should().ContainKey(RemoteFileConnectionConfig.RemoteTypeKey)
            .WhoseValue.Should().Be(DataSourceType.FTP.ToString());
        config.Host.Should().Be("ftp.example.com");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_FtpMissingHost_ThrowsArgumentException()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>();

        // Act
        var act = () => config.CreateFromArgs(DataSourceType.FTP, args);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Host*required*");
    }

    #endregion

    #region CreateFromArgs — S3

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_S3WithAllParams_Succeeds()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.BucketNameKey, "my-bucket" },
            { RemoteFileConnectionConfig.AccessKeyIdKey, "AKIAIOSFODNN7EXAMPLE" },
            { RemoteFileConnectionConfig.SecretAccessKeyKey, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" }
        };

        // Act
        var result = config.CreateFromArgs(DataSourceType.S3, args);

        // Assert
        result.Should().NotBeNull();
        config.BucketName.Should().Be("my-bucket");
        config.AccessKeyId.Should().Be("AKIAIOSFODNN7EXAMPLE");
        config.SecretAccessKey.Should().Be("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        config.Parameters.Should().ContainKey(RemoteFileConnectionConfig.RemoteTypeKey)
            .WhoseValue.Should().Be(DataSourceType.S3.ToString());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_S3MissingBucket_ThrowsArgumentException()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.AccessKeyIdKey, "AKIAIOSFODNN7EXAMPLE" },
            { RemoteFileConnectionConfig.SecretAccessKeyKey, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" }
        };

        // Act
        var act = () => config.CreateFromArgs(DataSourceType.S3, args);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Bucket*required*");
    }

    #endregion

    #region CreateFromArgs — Azure Blob

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_AzureBlobConnectionString_Succeeds()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.ContainerNameKey, "my-container" },
            { RemoteFileConnectionConfig.AzureConnectionStringKey, connectionString },
            { RemoteFileConnectionConfig.AzureAuthModeKey, "connectionstring" }
        };

        // Act
        var result = config.CreateFromArgs(DataSourceType.AzureBlob, args);

        // Assert
        result.Should().NotBeNull();
        config.ContainerName.Should().Be("my-container");
        config.AzureConnectionString.Should().Be(connectionString);
        config.AzureAuthMode.Should().Be("connectionstring");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_AzureBlobMissingContainer_ThrowsArgumentException()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.AzureConnectionStringKey, "DefaultEndpointsProtocol=https;AccountName=test" },
            { RemoteFileConnectionConfig.AzureAuthModeKey, "connectionstring" }
        };

        // Act
        var act = () => config.CreateFromArgs(DataSourceType.AzureBlob, args);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Container*required*");
    }

    #endregion

    #region CreateFromArgs — OAuth types

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_GoogleDrive_SucceedsWithNoExtraParams()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>();

        // Act
        var result = config.CreateFromArgs(DataSourceType.GoogleDrive, args);

        // Assert
        result.Should().NotBeNull();
        config.Parameters.Should().ContainKey(RemoteFileConnectionConfig.RemoteTypeKey)
            .WhoseValue.Should().Be(DataSourceType.GoogleDrive.ToString());
    }

    #endregion

    #region CreateFromArgs — Unsupported type

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_UnsupportedType_ThrowsArgumentException()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>();

        // Act
        var act = () => config.CreateFromArgs(DataSourceType.CSV, args);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Invalid*data source type*CSV*");
    }

    #endregion

    #region Property — RemoteType

    [Fact]
    [Trait("Category", "Unit")]
    public void RemoteType_ParsesFromParameters()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.RemoteTypeKey, "S3" }
            }
        };

        // Act
        var result = config.RemoteType;

        // Assert
        result.Should().Be(DataSourceType.S3);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RemoteType_DefaultsToFtp_WhenKeyMissing()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>()
        };

        // Act
        var result = config.RemoteType;

        // Assert
        result.Should().Be(DataSourceType.FTP, "RemoteType defaults to FTP when the key is absent");
    }

    #endregion

    #region Property — Port defaults

    [Fact]
    [Trait("Category", "Unit")]
    public void Port_DefaultsBasedOnType()
    {
        // Arrange — FTP type should default to port 21
        var ftpConfig = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.RemoteTypeKey, DataSourceType.FTP.ToString() }
            }
        };

        // Arrange — SFTP type should default to port 22
        var sftpConfig = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.RemoteTypeKey, DataSourceType.SFTP.ToString() }
            }
        };

        // Act & Assert
        ftpConfig.Port.Should().Be(RemoteFileConnectionConfig.DefaultFtpPort,
            "FTP default port is 21");
        sftpConfig.Port.Should().Be(RemoteFileConnectionConfig.DefaultSftpPort,
            "SFTP default port is 22");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Port_UsesExplicitValue_WhenSet()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.RemoteTypeKey, DataSourceType.FTP.ToString() },
                { RemoteFileConnectionConfig.PortKey, "2121" }
            }
        };

        // Act & Assert
        config.Port.Should().Be(2121, "explicit port should override the default");
    }

    #endregion

    #region Property — Region default

    [Fact]
    [Trait("Category", "Unit")]
    public void Region_DefaultsToUsEast1()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>()
        };

        // Act
        var result = config.Region;

        // Assert
        result.Should().Be("us-east-1", "Region defaults to us-east-1 when not set");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Region_UsesExplicitValue_WhenSet()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.RegionKey, "eu-west-1" }
            }
        };

        // Act
        var result = config.Region;

        // Assert
        result.Should().Be("eu-west-1");
    }

    #endregion

    #region Helper — GetBoolParam

    [Fact]
    [Trait("Category", "Unit")]
    public void GetBoolParam_ParsesCorrectly()
    {
        // Arrange — PassiveMode defaults to true; setting it to explicit "true" should parse
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.PassiveModeKey, "true" }
            }
        };

        // Act & Assert
        config.PassiveMode.Should().BeTrue("PassiveMode should parse 'true' string correctly");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetBoolParam_ReturnsFalse_WhenSetToFalse()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.PassiveModeKey, "false" }
            }
        };

        // Act & Assert
        config.PassiveMode.Should().BeFalse("PassiveMode should parse 'false' string correctly");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetBoolParam_ReturnsDefault_WhenKeyMissing()
    {
        // Arrange — PassiveMode defaults to true when not set
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>()
        };

        // Act & Assert
        config.PassiveMode.Should().BeTrue("PassiveMode defaults to true when the key is absent");
    }

    #endregion

    #region ValidateConnection

    [Fact]
    [Trait("Category", "Unit")]
    public void ValidateConnection_ValidFtpConfig_ReturnsTrue()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        config.CreateFromArgs(DataSourceType.FTP, new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.HostKey, "ftp.example.com" }
        });

        // Act
        var result = config.ValidateConnection();

        // Assert
        result.Should().BeTrue("a valid FTP config with Host should pass validation");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ValidateConnection_EmptyParameters_ReturnsFalse()
    {
        // Arrange — base.ValidateConnection() returns false when Parameters is empty
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>()
        };

        // Act
        var result = config.ValidateConnection();

        // Assert
        result.Should().BeFalse("empty Parameters should fail base validation");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ValidateConnection_FtpMissingHost_ReturnsFalse()
    {
        // Arrange — has parameters but Host is missing, so RequireParam will throw
        // and ValidateConnection catches the exception and returns false
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>
            {
                { RemoteFileConnectionConfig.RemoteTypeKey, DataSourceType.FTP.ToString() }
            }
        };

        // Act
        var result = config.ValidateConnection();

        // Assert
        result.Should().BeFalse("FTP config without Host should fail validation");
    }

    #endregion

    #region CreateFromArgs — Azure Blob auth modes

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_AzureBlobAccountKey_Succeeds()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.ContainerNameKey, "my-container" },
            { RemoteFileConnectionConfig.AzureAuthModeKey, "accountkey" },
            { RemoteFileConnectionConfig.AccountNameKey, "mystorageaccount" },
            { RemoteFileConnectionConfig.AccountKeyKey, "dGVzdGtleQ==" }
        };

        // Act
        var result = config.CreateFromArgs(DataSourceType.AzureBlob, args);

        // Assert
        result.Should().NotBeNull();
        config.AzureAuthMode.Should().Be("accountkey");
        config.AccountName.Should().Be("mystorageaccount");
        config.AccountKey.Should().Be("dGVzdGtleQ==");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_AzureBlobSasToken_Succeeds()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.ContainerNameKey, "my-container" },
            { RemoteFileConnectionConfig.AzureAuthModeKey, "sastoken" },
            { RemoteFileConnectionConfig.SasTokenKey, "sv=2021-06-08&ss=b&srt=sco&sp=rwdlacitfx" }
        };

        // Act
        var result = config.CreateFromArgs(DataSourceType.AzureBlob, args);

        // Assert
        result.Should().NotBeNull();
        config.AzureAuthMode.Should().Be("sastoken");
        config.SasToken.Should().Be("sv=2021-06-08&ss=b&srt=sco&sp=rwdlacitfx");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_AzureBlobAccountKeyMissingAccountName_ThrowsArgumentException()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.ContainerNameKey, "my-container" },
            { RemoteFileConnectionConfig.AzureAuthModeKey, "accountkey" },
            { RemoteFileConnectionConfig.AccountKeyKey, "dGVzdGtleQ==" }
            // AccountName intentionally missing
        };

        // Act
        var act = () => config.CreateFromArgs(DataSourceType.AzureBlob, args);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Account name*required*");
    }

    #endregion

    #region CreateFromArgs — Dropbox and OneDrive (OAuth)

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_Dropbox_SucceedsWithNoExtraParams()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>();

        // Act
        var result = config.CreateFromArgs(DataSourceType.Dropbox, args);

        // Assert
        result.Should().NotBeNull();
        config.RemoteType.Should().Be(DataSourceType.Dropbox);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_OneDrive_SucceedsWithNoExtraParams()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>();

        // Act
        var result = config.CreateFromArgs(DataSourceType.OneDrive, args);

        // Assert
        result.Should().NotBeNull();
        config.RemoteType.Should().Be(DataSourceType.OneDrive);
    }

    #endregion

    #region CreateFromArgs — SFTP

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_SftpWithHost_Succeeds()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>
        {
            { RemoteFileConnectionConfig.HostKey, "sftp.example.com" }
        };

        // Act
        var result = config.CreateFromArgs(DataSourceType.SFTP, args);

        // Assert
        result.Should().NotBeNull();
        config.Host.Should().Be("sftp.example.com");
        config.RemoteType.Should().Be(DataSourceType.SFTP);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateFromArgs_SftpMissingHost_ThrowsArgumentException()
    {
        // Arrange
        var config = new RemoteFileConnectionConfig();
        var args = new Dictionary<string, string>();

        // Act
        var act = () => config.CreateFromArgs(DataSourceType.SFTP, args);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Host*required*");
    }

    #endregion
}
