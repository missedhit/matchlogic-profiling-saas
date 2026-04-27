using MatchLogic.Domain.Import;

namespace MatchLogic.Application.UnitTests.RemoteConnectors.Helpers;

/// <summary>
/// Fluent builder for constructing <see cref="RemoteFileConnectionConfig"/> instances in tests.
/// Use the static factory methods to start with a preset for each remote type,
/// then chain additional configuration via <c>With*</c> methods before calling <see cref="Build"/>.
/// </summary>
public class RemoteFileConnectionConfigBuilder
{
    private readonly Dictionary<string, string> _params = new();

    #region Static Factory Methods

    /// <summary>
    /// Creates a builder preconfigured for an FTP connection.
    /// </summary>
    public static RemoteFileConnectionConfigBuilder ForFtp(string host, int port, string username, string password)
    {
        var builder = new RemoteFileConnectionConfigBuilder();
        builder._params[RemoteFileConnectionConfig.RemoteTypeKey] = DataSourceType.FTP.ToString();
        builder._params[RemoteFileConnectionConfig.HostKey] = host;
        builder._params[RemoteFileConnectionConfig.PortKey] = port.ToString();
        builder._params[RemoteFileConnectionConfig.UsernameKey] = username;
        builder._params[RemoteFileConnectionConfig.PasswordKey] = password;
        builder._params[RemoteFileConnectionConfig.UseSftpKey] = false.ToString();
        return builder;
    }

    /// <summary>
    /// Creates a builder preconfigured for an SFTP connection.
    /// </summary>
    public static RemoteFileConnectionConfigBuilder ForSftp(string host, int port, string username, string password)
    {
        var builder = new RemoteFileConnectionConfigBuilder();
        builder._params[RemoteFileConnectionConfig.RemoteTypeKey] = DataSourceType.SFTP.ToString();
        builder._params[RemoteFileConnectionConfig.HostKey] = host;
        builder._params[RemoteFileConnectionConfig.PortKey] = port.ToString();
        builder._params[RemoteFileConnectionConfig.UsernameKey] = username;
        builder._params[RemoteFileConnectionConfig.PasswordKey] = password;
        builder._params[RemoteFileConnectionConfig.UseSftpKey] = true.ToString();
        return builder;
    }

    /// <summary>
    /// Creates a builder preconfigured for an AWS S3 connection.
    /// </summary>
    public static RemoteFileConnectionConfigBuilder ForS3(
        string bucket, string accessKey, string secretKey, string region, string? customEndpoint = null)
    {
        var builder = new RemoteFileConnectionConfigBuilder();
        builder._params[RemoteFileConnectionConfig.RemoteTypeKey] = DataSourceType.S3.ToString();
        builder._params[RemoteFileConnectionConfig.BucketNameKey] = bucket;
        builder._params[RemoteFileConnectionConfig.AccessKeyIdKey] = accessKey;
        builder._params[RemoteFileConnectionConfig.SecretAccessKeyKey] = secretKey;
        builder._params[RemoteFileConnectionConfig.RegionKey] = region;

        if (!string.IsNullOrEmpty(customEndpoint))
        {
            builder._params[RemoteFileConnectionConfig.CustomEndpointKey] = customEndpoint;
        }

        return builder;
    }

    /// <summary>
    /// Creates a builder preconfigured for an Azure Blob Storage connection.
    /// </summary>
    public static RemoteFileConnectionConfigBuilder ForAzureBlob(string connectionString, string container)
    {
        var builder = new RemoteFileConnectionConfigBuilder();
        builder._params[RemoteFileConnectionConfig.RemoteTypeKey] = DataSourceType.AzureBlob.ToString();
        builder._params[RemoteFileConnectionConfig.AzureConnectionStringKey] = connectionString;
        builder._params[RemoteFileConnectionConfig.ContainerNameKey] = container;
        builder._params[RemoteFileConnectionConfig.AzureAuthModeKey] = "connectionstring";
        return builder;
    }

    /// <summary>
    /// Creates a builder preconfigured for a Google Drive connection.
    /// </summary>
    public static RemoteFileConnectionConfigBuilder ForGoogleDrive(string accessToken)
    {
        var builder = new RemoteFileConnectionConfigBuilder();
        builder._params[RemoteFileConnectionConfig.RemoteTypeKey] = DataSourceType.GoogleDrive.ToString();
        builder._params[RemoteFileConnectionConfig.AccessTokenKey] = accessToken;
        return builder;
    }

    /// <summary>
    /// Creates a builder preconfigured for a Dropbox connection.
    /// </summary>
    public static RemoteFileConnectionConfigBuilder ForDropbox(string accessToken)
    {
        var builder = new RemoteFileConnectionConfigBuilder();
        builder._params[RemoteFileConnectionConfig.RemoteTypeKey] = DataSourceType.Dropbox.ToString();
        builder._params[RemoteFileConnectionConfig.AccessTokenKey] = accessToken;
        return builder;
    }

    /// <summary>
    /// Creates a builder preconfigured for a OneDrive connection.
    /// </summary>
    public static RemoteFileConnectionConfigBuilder ForOneDrive(string accessToken)
    {
        var builder = new RemoteFileConnectionConfigBuilder();
        builder._params[RemoteFileConnectionConfig.RemoteTypeKey] = DataSourceType.OneDrive.ToString();
        builder._params[RemoteFileConnectionConfig.AccessTokenKey] = accessToken;
        return builder;
    }

    #endregion

    #region Fluent Setters

    /// <summary>
    /// Sets an arbitrary parameter on the connection config.
    /// </summary>
    public RemoteFileConnectionConfigBuilder WithParam(string key, string value)
    {
        _params[key] = value;
        return this;
    }

    /// <summary>
    /// Sets the remote path (folder/key prefix) for the connection.
    /// </summary>
    public RemoteFileConnectionConfigBuilder WithRemotePath(string path)
    {
        _params[RemoteFileConnectionConfig.RemotePathKey] = path;
        return this;
    }

    /// <summary>
    /// Sets the connection timeout in seconds.
    /// </summary>
    public RemoteFileConnectionConfigBuilder WithTimeout(int seconds)
    {
        _params[RemoteFileConnectionConfig.ConnectionTimeoutKey] = seconds.ToString();
        return this;
    }

    #endregion

    /// <summary>
    /// Builds and returns a fully configured <see cref="RemoteFileConnectionConfig"/>.
    /// </summary>
    public RemoteFileConnectionConfig Build()
    {
        var config = new RemoteFileConnectionConfig
        {
            Parameters = new Dictionary<string, string>(_params)
        };
        return config;
    }
}
