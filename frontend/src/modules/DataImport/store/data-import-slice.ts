import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { ColumnMapping } from "@/modules/DataImport/hooks/datasource/import-datasource";
import { DatabaseConnection, TableMetadata, UploadedFile } from "@/models/api-responses";
import { RemoteStorageConnection, RemoteFileInfo } from "@/models/remote-storage";

type FILE_ID = string;
type TABLE_NAME = string;
type COLUMN_NAME = string;

export interface DataImportState {
	selectedFileType: string;
	uploadedFile: UploadedFile | null;
	databaseConnection: DatabaseConnection;
	selectedSheets: TableMetadata[];
	columnMappings: Record<FILE_ID, Record<TABLE_NAME, Record<COLUMN_NAME, ColumnMapping>>>;
	tableNameObj: Record<string, string>;

	// Remote storage
	remoteConnection: RemoteStorageConnection | null;
	remoteBrowsePath: string;
	selectedRemoteFiles: RemoteFileInfo[];
	remoteConnectionTested: boolean;
	/** Maps table name → remote file path so the import payload can set the
	 *  correct RemotePath per datasource (needed for multi-file Excel imports). */
	remoteTableFileMap: Record<TABLE_NAME, string>;
}

const initialState: DataImportState = {
	selectedFileType: "",
	uploadedFile: null,
	databaseConnection: {
		type: "PostgreSQL",
		hostname: "",
		port: 0,
		timeout: 30,
		username: "",
		password: "",
		database: "",
		trust_server_certificate: false,
		auth_type: "SQL"
	},
	selectedSheets: [],
	columnMappings: {},
	tableNameObj: {},

	// Remote storage
	remoteConnection: null,
	remoteBrowsePath: "/",
	selectedRemoteFiles: [],
	remoteConnectionTested: false,
	remoteTableFileMap: {},
};


const dataImportSlice = createSlice({
	name: "dataImport",
	initialState,
	reducers: {
		setSelectedFileType(state, action: PayloadAction<string>) {
			state.selectedFileType = action.payload;
		},
		setUploadedFile(state, action: PayloadAction<UploadedFile | null>) {
			state.uploadedFile = action.payload;
		},
		setDatabaseConnection(state, action: PayloadAction<DatabaseConnection>) {
			state.databaseConnection = action.payload;
		},
		clearDatabaseConnection(state) {
			state.databaseConnection = {
				type: "PostgreSQL",
				hostname: "",
				port: 0,
				timeout: 30,
				username: "",
				password: "",
				database: "",
				trust_server_certificate: false,
				auth_type: "SQL"
			};
		},
		setSelectedSheets(state, action: PayloadAction<TableMetadata[]>) {
			state.selectedSheets = action.payload;
		},
		setColumnMappings(state,
			action: PayloadAction<{ fileId: string, tableName: string, columnName: string, columnMapping: ColumnMapping }>) {
			if (!state.columnMappings[action.payload.fileId]) {
				state.columnMappings[action.payload.fileId] = {};
			}
			if (!state.columnMappings[action.payload.fileId][action.payload.tableName]) {
				state.columnMappings[action.payload.fileId][action.payload.tableName] = {};
			}
			state.columnMappings[action.payload.fileId][action.payload.tableName][action.payload.columnName] = action.payload.columnMapping;

		},
		setTableNameObj(state, action: PayloadAction<{ currentName: string, newName: string }>) {
			state.tableNameObj[action.payload.currentName] = action.payload.newName;
		},
		clearTableNameObj(state) {
			state.tableNameObj = {};
		},

		// Remote storage actions
		setRemoteConnection(state, action: PayloadAction<RemoteStorageConnection | null>) {
			state.remoteConnection = action.payload;
			if (!action.payload) {
				state.remoteConnectionTested = false;
				state.remoteBrowsePath = "/";
				state.selectedRemoteFiles = [];
			}
		},
		updateRemoteConnection(state, action: PayloadAction<Partial<RemoteStorageConnection>>) {
			if (state.remoteConnection) {
				Object.assign(state.remoteConnection, action.payload);
				state.remoteConnectionTested = false;
			}
		},
		setRemoteBrowsePath(state, action: PayloadAction<string>) {
			state.remoteBrowsePath = action.payload;
		},
		setSelectedRemoteFiles(state, action: PayloadAction<RemoteFileInfo[]>) {
			state.selectedRemoteFiles = action.payload;
		},
		setRemoteConnectionTested(state, action: PayloadAction<boolean>) {
			state.remoteConnectionTested = action.payload;
		},
		setRemoteTableFileMap(state, action: PayloadAction<Record<string, string>>) {
			state.remoteTableFileMap = action.payload;
		},
		clearRemoteState(state) {
			state.remoteConnection = null;
			state.remoteBrowsePath = "/";
			state.selectedRemoteFiles = [];
			state.remoteConnectionTested = false;
			state.remoteTableFileMap = {};
		},
	},
})

export const dataImportActions = dataImportSlice.actions;
export default dataImportSlice.reducer;
