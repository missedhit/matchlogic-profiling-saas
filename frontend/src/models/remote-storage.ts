/** Shared types for remote storage connections (import + export). */

export interface RemoteStorageConnection {
  type: RemoteStorageType;

  // FTP/SFTP
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  useSftp?: boolean;
  passiveMode?: boolean;
  useTls?: boolean;
  privateKey?: string;
  privateKeyPassphrase?: string;
  hostFingerprint?: string;
  connectionTimeout?: number;

  // AWS S3
  accessKeyId?: string;
  secretAccessKey?: string;
  region?: string;
  bucketName?: string;
  sessionToken?: string;
  customEndpoint?: string;
  usePathStyle?: boolean;

  // Azure Blob
  azureAuthMode?: "connectionstring" | "accountkey" | "sastoken";
  azureConnectionString?: string;
  accountName?: string;
  accountKey?: string;
  sasToken?: string;
  containerName?: string;

  // OAuth (Google Drive, Dropbox, OneDrive)
  oauthConnected?: boolean;
  oauthProvider?: string;
  oauthAccountEmail?: string;
  oauthAccountName?: string;
  oauthDataSourceId?: string;
}

export type RemoteStorageType =
  | "ftp"
  | "s3"
  | "azure-blob"
  | "google-drive"
  | "dropbox"
  | "onedrive";

export interface RemoteFileInfo {
  name: string;
  path: string;
  size: number;
  lastModified: string;
  extension: string;
}

export interface RemoteFolderInfo {
  name: string;
  path: string;
  lastModified: string;
}

export interface RemoteBrowseResult {
  currentPath: string;
  files: RemoteFileInfo[];
  folders: RemoteFolderInfo[];
}

export interface RemoteFileMetadata {
  name: string;
  size: number;
  lastModified: string;
  eTag?: string;
  contentType?: string;
}

/** Result from the backend CheckRemoteUpdates endpoint for a single data source. */
export interface RemoteUpdateCheckResult {
  dataSourceId: string;
  dataSourceName: string;
  dataSourceType: number;
  hasUpdates: boolean;
  currentMetadata?: RemoteFileMetadata;
  storedMetadata?: RemoteFileMetadata;
  error?: string;
}

/** Maps frontend remote storage type to backend DataSourceType enum value. */
export const REMOTE_TYPE_TO_DATASOURCE_TYPE: Record<RemoteStorageType, number> = {
  ftp: 8,       // Uses SFTP=9 when useSftp=true
  s3: 10,
  "azure-blob": 11,
  "google-drive": 12,
  dropbox: 13,
  onedrive: 14,
};

/** Build BaseConnectionInfo.Parameters from a RemoteStorageConnection. */
export function buildRemoteConnectionParameters(
  connection: RemoteStorageConnection
): Record<string, string> {
  const params: Record<string, string> = {};

  switch (connection.type) {
    case "ftp":
      params["RemoteType"] = connection.useSftp ? "SFTP" : "FTP";
      if (connection.host) params["Host"] = connection.host;
      if (connection.port) params["Port"] = String(connection.port);
      if (connection.username) params["Username"] = connection.username;
      if (connection.password) params["Password"] = connection.password;
      if (connection.useSftp !== undefined) params["UseSftp"] = String(connection.useSftp);
      if (connection.passiveMode !== undefined) params["PassiveMode"] = String(connection.passiveMode);
      if (connection.useTls !== undefined) params["UseTls"] = String(connection.useTls);
      if (connection.privateKey) params["PrivateKey"] = connection.privateKey;
      if (connection.privateKeyPassphrase) params["PrivateKeyPassphrase"] = connection.privateKeyPassphrase;
      if (connection.connectionTimeout !== undefined) params["ConnectionTimeout"] = String(connection.connectionTimeout);
      if (connection.useSftp && connection.hostFingerprint) params["HostFingerprint"] = connection.hostFingerprint;
      break;

    case "s3":
      if (connection.accessKeyId) params["AccessKeyId"] = connection.accessKeyId;
      if (connection.secretAccessKey) params["SecretAccessKey"] = connection.secretAccessKey;
      if (connection.region) params["Region"] = connection.region;
      if (connection.bucketName) params["BucketName"] = connection.bucketName;
      if (connection.sessionToken) params["SessionToken"] = connection.sessionToken;
      if (connection.customEndpoint) params["CustomEndpoint"] = connection.customEndpoint;
      if (connection.usePathStyle !== undefined) params["UsePathStyle"] = String(connection.usePathStyle);
      break;

    case "azure-blob":
      if (connection.containerName) params["ContainerName"] = connection.containerName;
      if (connection.azureAuthMode) params["AzureAuthMode"] = connection.azureAuthMode;
      if (connection.azureConnectionString) params["AzureConnectionString"] = connection.azureConnectionString;
      if (connection.accountName) params["AccountName"] = connection.accountName;
      if (connection.accountKey) params["AccountKey"] = connection.accountKey;
      if (connection.sasToken) params["SasToken"] = connection.sasToken;
      break;

    case "google-drive":
    case "dropbox":
    case "onedrive":
      if (connection.oauthDataSourceId) params["OAuthDataSourceId"] = connection.oauthDataSourceId;
      break;
  }

  return params;
}

/** Get the actual backend DataSourceType number (handles FTP/SFTP toggle). */
export function getRemoteDataSourceType(connection: RemoteStorageConnection): number {
  if (connection.type === "ftp" && connection.useSftp) return 9; // SFTP
  return REMOTE_TYPE_TO_DATASOURCE_TYPE[connection.type];
}
