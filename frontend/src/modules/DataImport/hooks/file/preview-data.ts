import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { PreviewDataResponse, SupportedData } from "@/models/api-responses";
import { useAppSelector } from "@/hooks/use-store";
import {
	getDatabaseConnectionPayload,
	getRemoteConnectionPayload,
} from "@/modules/DataImport/utils/get-connection-payload";

export const useTablesDataQuery = ({
	tableName,
}: {
	tableName?: string;
}) => {
	const { uploadedFile, databaseConnection, remoteConnection, selectedRemoteFiles } = useAppSelector((s) => s.dataImport);
	const BASE_PATH = `/dataimport/Preview/Data`;

	// Determine import mode
	const isRemote = !uploadedFile && !databaseConnection.database && !!remoteConnection && selectedRemoteFiles.length > 0;
	const firstRemoteFile = isRemote ? selectedRemoteFiles[0] : null;

	const payload = isRemote && firstRemoteFile
		? getRemoteConnectionPayload({ connection: remoteConnection!, filePath: firstRemoteFile.path }, true, tableName || "")
		: getDatabaseConnectionPayload(uploadedFile, databaseConnection, true, tableName);

	const queryKey = isRemote && firstRemoteFile
		? ["table-data", `remote:${firstRemoteFile.path}`, tableName]
		: ["table-data", uploadedFile?.id || `${databaseConnection.hostname}:${databaseConnection.port}/${databaseConnection.database}`, tableName];

	const columnsQuery = useQuery({
		queryKey,
		queryFn: async () => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<PreviewDataResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "POST",
					body: JSON.stringify(payload),
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Success-Toast": "true",
					},
				}
			);
			if (!response?.value) throw new Error("Failed to load preview data");
			return response;
		},
		enabled: (!!uploadedFile || !!databaseConnection.database || isRemote) && !!payload,
	});
	return columnsQuery;
};