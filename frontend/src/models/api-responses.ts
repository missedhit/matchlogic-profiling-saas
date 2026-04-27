import { BasicResponse } from "./basic-response";

export enum SupportedData {
	Unknown = 0,
	CSV = 1,
	Excel = 2,
	SQLServer = 3,
	MySQL = 4,
	PostgreSQL = 5,
	Snowflake = 6,
	FTP = 8,
	SFTP = 9,
	S3 = 10,
	AzureBlob = 11,
	GoogleDrive = 12,
	Dropbox = 13,
	OneDrive = 14,
}

export interface Project {
	id: string;
	name: string;
	description: string;
	createdAt: Date;
	modifiedAt: Date;
}

export interface UploadedFile {
	id: string;
	projectId: string;
	dataSourceType: keyof typeof SupportedData;
	fileName: string;
	originalName: string;
	filePath: string;
	fileSize: number;
	fileExtension: string;
	createdAt: Date;
}

export interface DatabaseConnection {
	type: keyof typeof SupportedData;
	hostname: string;
	port: number;
	timeout: number;
	username: string;
	password: string;
	trust_server_certificate: boolean;
	database: string;
	auth_type: string;
}

export interface TableMetadata {
	schema: string;
	name: string;
	type: string;
	columns: {
		name: string;
		dataType: string | null;
		isNullable: boolean;
	}[] | null;
}

export interface Datasource {
	id: string;
	name: string;
	sourceType: string;
	size: number;
	recordCount: number;
	columnsCount: number;
	validCount: number;
	invalidCount: number;
	errorMessages: null;
	createdAt: Date;
	modifiedAt: Date;
	/** Connection details stored on the backend (type + encrypted parameters). */
	connectionDetails?: {
		type: number;
		parameters: Record<string, string>;
	};
	/** SHA256 hash of column headers, used for schema change detection. */
	schemaSignature?: string;
	/** File import ID for the last imported file — used by refresh endpoint. */
	latestFileImportId?: string;
	/** Metadata from the last imported remote file (for change detection). */
	lastImportedFileMetadata?: {
		lastModified: string;
		eTag?: string;
		size: number;
		contentType?: string;
	};
}

export interface ListProjectResponse extends BasicResponse {
	value: Project[];
}

export interface CreateProjectResponse extends BasicResponse {
	value: Project;
}

export interface UpdateProjectResponse extends BasicResponse {
	value: Project;
}

export interface DeleteProjectResponse extends BasicResponse {
	value: null;
}

export interface UploadFileResponse extends BasicResponse {
	value: UploadedFile;
}

export interface PreviewDatabaseReponse extends BasicResponse {
	value: string[];
}

export interface PreviewTableReponse extends BasicResponse {
	value: {
		tables: TableMetadata[];
	};
}

export interface PreviewColumnsResponse extends BasicResponse {
	value: {
		metadata: {
			tables: TableMetadata[];
			columnMappings: {
			};
		};
	};
}

export interface PreviewDataResponse extends BasicResponse {
	value: {
		data: unknown[];
		totalRecords: number;
		sameNameColumnsCount: number;
		errorMessages: string[];
	};
}

export interface ImportDataResponse extends BasicResponse {
	value: {
		projectRun: {
			id: string;
			previousRunId: string;
			projectId: string;
			startTime: Date;
			endTime: Date;
			status: number;
			runNumber: number;
			dataImportResult: {

			};
		}
	};
}


export interface DatasourceListResponse extends BasicResponse {
	value: Datasource[];
}

export interface UpdateDatasourceResponse extends BasicResponse {
	value: {
		id: string;
		name: string;
		modifiedAt: Date;
	};
}

export interface DeleteDatasourceResponse extends BasicResponse {
	value: null;
}

export interface RawDataResponse extends BasicResponse {
	value: {
		data: Record<string, unknown>[];
		totalCount: number;
	};
}
export interface CleanseDataResponse extends BasicResponse {
	value: {
		data: Record<string, unknown>[];
	};
}
export interface AdvanceDataResponse {
	value: {
		rowReferences: {
			rowData: Record<string, unknown>;
			value: string;
			rowNumber: number;
		}[];
	};
}



