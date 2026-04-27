using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.Import;

public class RemoteFileConnectionConfig : ConnectionConfig
{
    #region Parameter Keys — Connection (shared)
    public const string RemoteTypeKey = "RemoteType";
    public const string RemotePathKey = "RemotePath";
    public const string FileNameKey = "FileName";
    public const string FileExtensionKey = "FileExtension";
    public const string LocalTempPathKey = "LocalTempPath";
    #endregion

    #region Parameter Keys — FTP / SFTP
    public const string HostKey = "Host";
    public const string PortKey = "Port";
    public const string UsernameKey = "Username";
    public const string PasswordKey = "Password";
    public const string UseSftpKey = "UseSftp";
    public const string PassiveModeKey = "PassiveMode";
    public const string UseTlsKey = "UseTls";
    public const string PrivateKeyKey = "PrivateKey";
    public const string PrivateKeyPassphraseKey = "PrivateKeyPassphrase";
    public const string HostFingerprintKey = "HostFingerprint";
    public const string ConnectionTimeoutKey = "ConnectionTimeout";
    #endregion

    #region Parameter Keys — AWS S3
    public const string BucketNameKey = "BucketName";
    public const string AccessKeyIdKey = "AccessKeyId";
    public const string SecretAccessKeyKey = "SecretAccessKey";
    public const string RegionKey = "Region";
    public const string SessionTokenKey = "SessionToken";
    public const string CustomEndpointKey = "CustomEndpoint";
    public const string UsePathStyleKey = "UsePathStyle";
    #endregion

    #region Parameter Keys — Azure Blob
    public const string AzureConnectionStringKey = "AzureConnectionString";
    public const string ContainerNameKey = "ContainerName";
    public const string AccountNameKey = "AccountName";
    public const string AccountKeyKey = "AccountKey";
    public const string SasTokenKey = "SasToken";
    public const string AzureAuthModeKey = "AzureAuthMode";
    #endregion

    #region Parameter Keys — OAuth (Google Drive, Dropbox, OneDrive)
    public const string AccessTokenKey = "AccessToken";
    public const string RefreshTokenKey = "RefreshToken";
    public const string TokenExpiryKey = "TokenExpiry";
    public const string OAuthDataSourceIdKey = "OAuthDataSourceId";
    #endregion

    #region Parameter Keys — Export
    public const string ExportFormatKey = "ExportFormat";
    public const string OverwriteExistingKey = "OverwriteExisting";
    #endregion

    #region Default Values
    public const int DefaultFtpPort = 21;
    public const int DefaultSftpPort = 22;
    public const int DefaultConnectionTimeout = 30;
    public const string DefaultS3Region = "us-east-1";
    #endregion

    #region Properties — Connection
    public string Host => GetParam(HostKey);
    public int Port => GetIntParam(PortKey, RemoteType == DataSourceType.SFTP ? DefaultSftpPort : DefaultFtpPort);
    public string Username => GetParam(UsernameKey);
    public string Password => GetParam(PasswordKey);
    public string RemotePath => GetParam(RemotePathKey);
    public string FileName => GetParam(FileNameKey);
    public string FileExtension => GetParam(FileExtensionKey);
    public string LocalTempPath => GetParam(LocalTempPathKey);
    public int ConnectionTimeout => GetIntParam(ConnectionTimeoutKey, DefaultConnectionTimeout);
    #endregion

    #region Properties — FTP/SFTP
    public bool UseSftp => GetBoolParam(UseSftpKey, false);
    public bool PassiveMode => GetBoolParam(PassiveModeKey, true);
    public bool UseTls => GetBoolParam(UseTlsKey, false);
    public string PrivateKey => GetParam(PrivateKeyKey);
    public string PrivateKeyPassphrase => GetParam(PrivateKeyPassphraseKey);
    public string HostFingerprint => GetParam(HostFingerprintKey);
    #endregion

    #region Properties — AWS S3
    public string BucketName => GetParam(BucketNameKey);
    public string AccessKeyId => GetParam(AccessKeyIdKey);
    public string SecretAccessKey => GetParam(SecretAccessKeyKey);
    public string Region => GetParamOrDefault(RegionKey, DefaultS3Region);
    public string SessionToken => GetParam(SessionTokenKey);
    public string CustomEndpoint => GetParam(CustomEndpointKey);
    public bool UsePathStyle => GetBoolParam(UsePathStyleKey, false);
    #endregion

    #region Properties — Azure Blob
    public string AzureConnectionString => GetParam(AzureConnectionStringKey);
    public string ContainerName => GetParam(ContainerNameKey);
    public string AccountName => GetParam(AccountNameKey);
    public string AccountKey => GetParam(AccountKeyKey);
    public string SasToken => GetParam(SasTokenKey);
    public string AzureAuthMode => GetParamOrDefault(AzureAuthModeKey, "connectionstring");
    #endregion

    #region Properties — OAuth
    public string AccessToken => GetParam(AccessTokenKey);
    public string RefreshToken => GetParam(RefreshTokenKey);
    public string TokenExpiry => GetParam(TokenExpiryKey);
    public string OAuthDataSourceId => GetParam(OAuthDataSourceIdKey);
    #endregion

    #region Properties — Export
    public string ExportFormat => GetParamOrDefault(ExportFormatKey, "CSV");
    public bool OverwriteExisting => GetBoolParam(OverwriteExistingKey, true);
    #endregion

    public DataSourceType RemoteType
    {
        get
        {
            if (Parameters.TryGetValue(RemoteTypeKey, out var val) && Enum.TryParse<DataSourceType>(val, true, out var type))
                return type;
            return DataSourceType.FTP;
        }
    }

    private static readonly HashSet<DataSourceType> SupportedRemoteTypes = new()
    {
        DataSourceType.FTP, DataSourceType.SFTP, DataSourceType.S3,
        DataSourceType.AzureBlob, DataSourceType.GoogleDrive,
        DataSourceType.Dropbox, DataSourceType.OneDrive
    };

    public override bool CanCreateFromArgs(DataSourceType type)
        => SupportedRemoteTypes.Contains(type);

    public override ConnectionConfig CreateFromArgs(DataSourceType type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        if (!CanCreateFromArgs(type))
            throw new ArgumentException($"Invalid data source type for RemoteFileConnectionConfig: {type}");

        Parameters = args;
        SourceConfig = sourceConfiguration;
        Parameters[RemoteTypeKey] = type.ToString();

        ValidateRequiredParams(type);
        return this;
    }

    public override bool ValidateConnection()
    {
        if (!base.ValidateConnection())
            return false;

        try
        {
            ValidateRequiredParams(RemoteType);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private void ValidateRequiredParams(DataSourceType type)
    {
        switch (type)
        {
            case DataSourceType.FTP:
            case DataSourceType.SFTP:
                RequireParam(HostKey, "Host is required for FTP/SFTP connection");
                break;

            case DataSourceType.S3:
                RequireParam(BucketNameKey, "Bucket name is required for S3 connection");
                RequireParam(AccessKeyIdKey, "Access Key ID is required for S3 connection");
                RequireParam(SecretAccessKeyKey, "Secret Access Key is required for S3 connection");
                break;

            case DataSourceType.AzureBlob:
                RequireParam(ContainerNameKey, "Container name is required for Azure Blob connection");
                var mode = AzureAuthMode;
                if (mode == "connectionstring")
                    RequireParam(AzureConnectionStringKey, "Connection string is required for Azure Blob connection");
                else if (mode == "accountkey")
                {
                    RequireParam(AccountNameKey, "Account name is required for Azure Blob connection");
                    RequireParam(AccountKeyKey, "Account key is required for Azure Blob connection");
                }
                else if (mode == "sastoken")
                    RequireParam(SasTokenKey, "SAS token is required for Azure Blob connection");
                break;

            case DataSourceType.GoogleDrive:
            case DataSourceType.Dropbox:
            case DataSourceType.OneDrive:
                // OAuth tokens are resolved server-side via OAuthTokenService
                break;
        }
    }

    #region Helpers
    private string GetParam(string key)
        => Parameters.TryGetValue(key, out var val) ? val ?? string.Empty : string.Empty;

    private string GetParamOrDefault(string key, string defaultValue)
    {
        if (Parameters.TryGetValue(key, out var val) && !string.IsNullOrEmpty(val))
            return val;
        return defaultValue;
    }

    private int GetIntParam(string key, int defaultValue)
    {
        if (Parameters.TryGetValue(key, out var val) && int.TryParse(val, out var result))
            return result;
        return defaultValue;
    }

    private bool GetBoolParam(string key, bool defaultValue)
    {
        if (Parameters.TryGetValue(key, out var val) && bool.TryParse(val, out var result))
            return result;
        return defaultValue;
    }

    private void RequireParam(string key, string errorMessage)
    {
        if (!Parameters.ContainsKey(key) || string.IsNullOrEmpty(Parameters[key]))
            throw new ArgumentException(errorMessage);
    }
    #endregion
}
