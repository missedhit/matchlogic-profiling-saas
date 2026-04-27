import { RunResponse } from "@/models/run-response";
import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation } from "@tanstack/react-query";
import { useJobState } from "@/providers/job-state-provider";
import { useAppSelector } from "@/hooks/use-store";
import { useRouteGuard } from "@/providers/route-guard-provider";
import { useRouter } from "next/navigation";
import { SupportedData } from "@/models/api-responses";
import { bindActionCreators } from "@reduxjs/toolkit";
import { dataImportActions } from "@/modules/DataImport/store/data-import-slice";
import {
	buildRemoteConnectionParameters,
	getRemoteDataSourceType,
} from "@/models/remote-storage";

export interface ColumnMapping {
	sourceColumn: string;
	targetColumn: string;
	include: boolean;
}

interface DataSourceItem {
	name: string;
	tableName: string;
	columnMappings: Record<string, ColumnMapping>;
}
export interface RunJobParams {
	projectId: string;
	dataSources: DataSourceItem[]
}

export const useImportDatasourceMutation = () => {
	const { startProcessing } = useJobState();
	const { setImporting, setFile } = useRouteGuard()
	const { uploadedFile, databaseConnection, remoteConnection, selectedRemoteFiles, remoteTableFileMap } = useAppSelector((s) => s.dataImport);
	const { clearDatabaseConnection, clearRemoteState, clearTableNameObj } = bindActionCreators(dataImportActions, store.dispatch)
	const router = useRouter()
	const selectedProject = useAppSelector((s) => s.projects.selectedProject);
	const dataImportMutation = useMutation({
		mutationFn: async ({
			projectId,
			dataSources,
		}: RunJobParams) => {
			const url = `/dataimport/DataSource`;
			let payload = null

			if (uploadedFile) {
				payload = {
					projectId,
					Connection: {
						type: SupportedData[uploadedFile?.dataSourceType!],
						parameters: {
							FilePath: uploadedFile?.filePath
						}
					},
					dataSources
				}
			} else if (databaseConnection.database) {
				payload = {
					projectId,
					Connection: {
						type: SupportedData[databaseConnection.type!],
						parameters: {
							Server: databaseConnection.hostname,
							...(databaseConnection.port && { Port: `${databaseConnection.port}` }),
							...(databaseConnection.auth_type !== "Windows" && {
								Username: databaseConnection.username,
								Password: databaseConnection.password,
							}),
							Database: databaseConnection.database,
							TrustServerCertificate: databaseConnection.trust_server_certificate ? "true" : "false",
							ConnectionTimeout: `${databaseConnection.timeout || 30}`,
							AuthType: databaseConnection.auth_type
						}
					},
					dataSources
				}
			} else if (remoteConnection && selectedRemoteFiles.length > 0) {
				// Remote import: put RemotePath in the connection Parameters (Angular
				// alignment) using the primary file path. Per-datasource remotePath is
				// kept as a fallback for multi-file Excel imports.
				const remoteParams = buildRemoteConnectionParameters(remoteConnection);
				const remoteType = getRemoteDataSourceType(remoteConnection);
				const primaryPath = selectedRemoteFiles[0].path;
				remoteParams["RemotePath"] = primaryPath;
				const dataSourcesWithPath = dataSources.map((ds) => {
					// Use the table→file map populated by useRemotePreviewTables for
					// accurate per-sheet file paths (needed for multi-file Excel imports).
					const filePath = remoteTableFileMap[ds.tableName]
						?? selectedRemoteFiles.find((f) => f.name === ds.tableName)?.path
						?? primaryPath;
					return {
						...ds,
						remotePath: filePath,
					};
				});
				payload = {
					projectId,
					connection: {
						type: remoteType,
						parameters: remoteParams,
					},
					dataSources: dataSourcesWithPath,
				};
			}

			const response = await apiFetch<RunResponse>(
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

			return response;
		},
		onSuccess: (data) => {
			if (data.isSuccess && data.value) {
				startProcessing(
					data.value.projectRun.id,
					"/data-import/data-sources",
					{ queryKey: ["data-source"], projectId: selectedProject?.id },
					selectedProject?.name || "Data Import",
					"Data Imported"
				);
				setImporting(true);
				setFile(null);
				clearDatabaseConnection();
				clearRemoteState();
				clearTableNameObj();
				router.push("/data-import/data-sources");
			}
		}
	});
	return dataImportMutation;
};
