import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { PreviewColumnsResponse, SupportedData } from "@/models/api-responses";
import { useAppSelector } from "@/hooks/use-store";
import {
	getDatabaseConnectionPayload,
	getRemoteConnectionPayload,
} from "@/modules/DataImport/utils/get-connection-payload";

export const useTablesColumnsQuery = () => {
	const { uploadedFile, databaseConnection, remoteConnection, selectedRemoteFiles } = useAppSelector((s) => s.dataImport);
	const BASE_PATH = `/dataimport/Preview/Columns`;

	// Determine which payload and cache key to use based on import mode.
	const isRemote = !uploadedFile && !databaseConnection.database && !!remoteConnection && selectedRemoteFiles.length > 0;
	const firstRemoteFile = isRemote ? selectedRemoteFiles[0] : null;

	const payload = isRemote && firstRemoteFile
		? getRemoteConnectionPayload({ connection: remoteConnection!, filePath: firstRemoteFile.path })
		: getDatabaseConnectionPayload(uploadedFile, databaseConnection);

	const queryKey = isRemote && firstRemoteFile
		? ["table-columns", `remote:${remoteConnection!.type}:${firstRemoteFile.path}`]
		: ["table-columns", uploadedFile?.id || `${databaseConnection.hostname}:${databaseConnection.port}/${databaseConnection.database}`];

	const columnsQuery = useQuery({
		queryKey,
		queryFn: async () => {
			const response = await apiFetch<PreviewColumnsResponse>(
				store.dispatch,
				store.getState,
				BASE_PATH,
				{
					method: "POST",
					body: JSON.stringify(payload),
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Success-Toast": "true",
					},
				}
			);
			if (!response?.value) throw new Error("Failed to load table columns");
			return response;
		},
		enabled: (!!uploadedFile || !!databaseConnection.database || isRemote) && !!payload,
	});
	return columnsQuery;
};