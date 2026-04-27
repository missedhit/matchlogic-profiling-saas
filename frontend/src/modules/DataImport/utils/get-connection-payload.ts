import { DatabaseConnection, SupportedData } from "@/models/api-responses";
import { UploadedFile } from "@/models/api-responses";
import {
	RemoteStorageConnection,
	buildRemoteConnectionParameters,
	getRemoteDataSourceType,
} from "@/models/remote-storage";

export interface RemoteFileRef {
	connection: RemoteStorageConnection;
	filePath: string;
}

export const getRemoteConnectionPayload = (
	remoteInfo: RemoteFileRef,
	isPreviewData = false,
	tableName = ""
) => {
	const params = buildRemoteConnectionParameters(remoteInfo.connection);
	params["RemotePath"] = remoteInfo.filePath;

	// Add file name and extension — required by backend for Excel sheet discovery
	const fileName = remoteInfo.filePath.split("/").pop() || "";
	if (fileName) {
		params["FileName"] = fileName;
		const extMatch = fileName.match(/\.[^.]+$/);
		if (extMatch) params["FileExtension"] = extMatch[0];
	}

	// SheetIndex = -1 tells the backend to return ALL sheets for Excel files
	params["SheetIndex"] = "-1";

	// When fetching data for a specific sheet, pass the sheet/table name
	if (tableName) {
		params["TableName"] = tableName;
	}

	const type = getRemoteDataSourceType(remoteInfo.connection);

	if (isPreviewData) {
		return {
			tableName,
			Connection: { type, parameters: params },
		};
	}
	return { type, parameters: params };
};

export const getDatabaseConnectionPayload = (uploadedFile: UploadedFile | null, databaseConnection: DatabaseConnection, isPreviewData = false, tableName = '') => {
	let payload = null
	if (uploadedFile) {
		if (isPreviewData) {
			payload = {
				tableName,
				Connection: {
					type: SupportedData[uploadedFile?.dataSourceType!],
					parameters: {
						FilePath: uploadedFile?.filePath,
						SheetIndex: "-1",
					}
				}
			};
		} else {
			payload = {
				type: SupportedData[uploadedFile?.dataSourceType!],
				parameters: {
					FilePath: uploadedFile?.filePath,
					SheetIndex: "-1",
				}
			}

		}
	}
	else if (databaseConnection.database) {
		if (isPreviewData) {
			payload = {
				tableName,
				Connection: {
					type: SupportedData[databaseConnection.type!],
					parameters: {
						Server: databaseConnection.hostname,
						...(databaseConnection.port && { Port: `${databaseConnection.port}` }),
						...(databaseConnection.username && { Username: databaseConnection.username }),
						...(databaseConnection.password && { Password: databaseConnection.password }),
						Database: databaseConnection.database,
						TrustServerCertificate: databaseConnection.trust_server_certificate ? "true" : "false",
						AuthType: databaseConnection.auth_type
					}
				}

			}
		} else {

			payload = {
				type: SupportedData[databaseConnection.type!],
				parameters: {
					Server: databaseConnection.hostname,
					...(databaseConnection.port && { Port: `${databaseConnection.port}` }),
					...(databaseConnection.username && { Username: databaseConnection.username }),
					...(databaseConnection.password && { Password: databaseConnection.password }),
					Database: databaseConnection.database,
					TrustServerCertificate: databaseConnection.trust_server_certificate ? "true" : "false",
					AuthType: databaseConnection.auth_type
				}

			}
		}
	}
	return payload
}