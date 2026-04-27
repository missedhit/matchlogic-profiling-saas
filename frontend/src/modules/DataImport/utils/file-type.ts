import { SupportedData } from "@/models/api-responses";

export const getFileType = (fileName: string): keyof typeof SupportedData => {
	const extension = fileName.split(".").pop()?.toLowerCase();
	switch (extension) {
		case "csv":
			return "CSV";
		case "xlsx":
		case "xls":
			return "Excel";
		case "postgresql":
			return "PostgreSQL";
		case "sqlserver":
			return "SQLServer";
		case "mysql":
			return "MySQL";
		case "ftp":
			return "FTP";
		case "sftp":
			return "SFTP";
		case "s3":
			return "S3";
		case "azure-blob":
			return "AzureBlob";
		case "google-drive":
			return "GoogleDrive";
		case "dropbox":
			return "Dropbox";
		case "onedrive":
			return "OneDrive";
		default:
			return "Unknown";
	}
};

export const getDefaultPortNumber = (sqlType: string): number => {
	switch (sqlType) {
		case "postgresql":
			return 5432;
		case "sqlserver":
			return 1433;
		case "mysql":
			return 3306;
		default:
			return 0;
	}
};

/** Check if a file type is a remote storage type. */
export const isRemoteStorageType = (type: string): boolean => {
	return ["ftp", "s3", "azure-blob", "google-drive", "dropbox", "onedrive"].includes(type);
};

/** Check if a file type is an OAuth-based remote storage type. */
export const isOAuthStorageType = (type: string): boolean => {
	return ["google-drive", "dropbox", "onedrive"].includes(type);
};


