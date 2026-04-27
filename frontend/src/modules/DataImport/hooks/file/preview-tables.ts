import { PreviewTableReponse, SupportedData } from "@/models/api-responses";
import { BasicResponse } from "@/models/basic-response";
import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { useAppSelector } from "@/hooks/use-store";
import {
	getDatabaseConnectionPayload,
	getRemoteConnectionPayload,
} from "@/modules/DataImport/utils/get-connection-payload";

export const useFileTablesQuery = () => {
	const { uploadedFile, databaseConnection, remoteConnection, selectedRemoteFiles } = useAppSelector((s) => s.dataImport);
	const BASE_PATH = `/dataimport/Preview/Tables`;

	// Determine import mode.
	const isRemote = !uploadedFile && !databaseConnection.database && !!remoteConnection && selectedRemoteFiles.length > 0;
	const firstRemoteFile = isRemote ? selectedRemoteFiles[0] : null;

	const payload = isRemote && firstRemoteFile
		? getRemoteConnectionPayload({ connection: remoteConnection!, filePath: firstRemoteFile.path })
		: getDatabaseConnectionPayload(uploadedFile, databaseConnection);

	// Keep remote cache key aligned with use-remote-preview-tables so the
	// pre-seeded cache entry is hit when select-table mounts after navigation.
	const queryKey = isRemote && firstRemoteFile
		? ["file-tables", `remote:${firstRemoteFile.path}`]
		: ["file-tables", uploadedFile?.id || `${databaseConnection.hostname}:${databaseConnection.port}/${databaseConnection.database}`];

	const tablesQuery = useQuery({
		queryKey,
		queryFn: async () => {
			const response = await apiFetch<PreviewTableReponse>(
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
			if (!response?.value) throw new Error("Failed to load file tables");
			return response;
		},
		enabled: (!!uploadedFile || !!databaseConnection.database || isRemote) && !!payload,
	});
	return tablesQuery;
};
