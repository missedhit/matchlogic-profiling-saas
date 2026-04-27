import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
	GeneralWarningIcon,
	DataImportColumnsIcon,
	DataImportPreviewIcon,
} from "@/assets/icons";
import { Loader2 } from "lucide-react";
import { NoTableSelected } from "./ui/column-mapping/no-table-selected";
import { useImportDatasourceMutation } from "@/modules/DataImport/hooks/datasource/import-datasource";
import { useCallback, useEffect, useState } from "react";
import { useAppSelector } from "@/hooks/use-store";
import { useTablesColumnsQuery } from "@/modules/DataImport/hooks/file/preview-columns";
import { bindActionCreators } from "@reduxjs/toolkit";
import { dataImportActions } from "@/modules/DataImport/store/data-import-slice";
import { store } from "@/store";
import { Exception, ExceptionTab } from "./ui/column-mapping/exceptions.tab";
import { ColumnsTab } from "./ui/column-mapping/columns-tab";
import { PreviewTab } from "./ui/column-mapping/preview-tab";
import { useRouteGuard } from "@/providers/route-guard-provider";
import type { RootState } from "@/store";
import { useRouter } from "next/navigation";
import { toast } from "sonner";
import { NavigationConfirmationDialog } from "./ui/column-mapping/navigation-guard-dialog";
import { useNavigationGuard } from "@/modules/DataImport/hooks/use-navigation-guard";

export default function ColumnMapping() {
	const { mutateAsync: importDataSource } = useImportDatasourceMutation();
	const { selectedProject } = useAppSelector((s) => s.projects);
	// License gating removed during saas-extract — always allow import in SaaS variant.
	// Quota enforcement (1000-record cap) replaces this in M4 via IQuotaService.
	const canImport = true;
	const {
		selectedSheets,
		uploadedFile,
		columnMappings,
		tableNameObj,
		databaseConnection,
		selectedFileType,
		remoteConnection,
		selectedRemoteFiles,
	} = useAppSelector((s) => s.dataImport);
	const { setColumnMappings } = bindActionCreators(
		dataImportActions,
		store.dispatch
	);

	// Determine import mode.
	const isRemote =
		!uploadedFile &&
		!databaseConnection.database &&
		!!remoteConnection &&
		selectedRemoteFiles.length > 0;

	// For remote files use the first remote file's path as a stable fileId so
	// columnMappings are keyed consistently across select-table and column-mapping.
	const fileId =
		uploadedFile?.id ||
		databaseConnection.database ||
		(isRemote ? selectedRemoteFiles[0].path : "");

	// Display name override: for CSV files the server-assigned table name is a GUID.
	// For remote CSV files, the table name is the file name so no override is needed.
	const fileDisplayName =
		selectedFileType === "csv" && uploadedFile?.originalName
			? uploadedFile.originalName.split(".")[0]
			: null;

	const getDisplayName = (tableName: string) => fileDisplayName ?? tableName;

	/**
	 * Build schema-qualified table name for API calls.
	 * Matches Angular: connectionInfo.type === DataSourceType.Excel ? tableName : `${schema}.${tableName}`
	 * For file-based imports (CSV, Excel, remote) → raw table name
	 * For database imports (SQL Server, MySQL, PostgreSQL) → schema.tableName
	 */
	const getQualifiedTableName = (table: { schema: string; name: string }) => {
		const isFileType =
			["csv", "xlsx"].includes(selectedFileType) || !!uploadedFile || isRemote;
		if (isFileType) return table.name;
		return table.schema ? `${table.schema}.${table.name}` : table.name;
	};

	const [activeTable, setActiveTable] = useState<string | null>(null);
	const [activeTab, setActiveTab] = useState("columns");
	const [exceptions, setExceptions] = useState<Exception[]>([]);
	const { data, isLoading: isLoadingColumns } = useTablesColumnsQuery();

	// Auto-select table when there's only one available
	useEffect(() => {
		if (selectedSheets.length === 1 && activeTable === null) {
			setActiveTable(selectedSheets[0].name);
		}
	}, [selectedSheets, activeTable]);
	const { isOpen, onConfirm, onCancel } = useNavigationGuard();
	useEffect(() => {
		if (!fileId) return;
		const newExceptions: Exception[] = [];
		data?.value?.metadata?.tables?.forEach((table) => {
			if (table.name) {
				if (
					!columnMappings[fileId] ||
					!(table.name in columnMappings[fileId])
				) {
					table.columns?.map((column) => {
						setColumnMappings({
							fileId,
							tableName: table.name,
							columnName: column.name,
							columnMapping: {
								sourceColumn: column.name,
								targetColumn: column.name,
								include: true,
							},
						});
					});
				}
				if (table.columns?.length === 0) {
					newExceptions.push({
						tableName: table.name,
						message: `No columns found in file: ${getDisplayName(table.name)}`,
					});
				}
			}
		});
		setExceptions(newExceptions);
	}, [data]);

	const createDatasource = useCallback(() => {
		if (!fileId) return;

		let duplicates: string[] = [];
		const dataSources = selectedSheets.map((table) => {
			const columns = Object.values(columnMappings[fileId][table.name])
				.filter((col) => col.include)
				.map((col) => col.targetColumn || col.sourceColumn);
			duplicates = columns.filter(
				(name, index) => columns.indexOf(name) !== index
			);
			return {
				name: tableNameObj[table.name] || getDisplayName(table.name),
				tableName: getQualifiedTableName(table),
				columnMappings: columnMappings[fileId][table.name],
			};
		});
		if (duplicates.length > 0) {
			toast.error(
				`Duplicate column names found: ${[...new Set(duplicates)].join(", ")}.`,
				{
					description: "Please ensure all column names are unique.",
				}
			);
			return;
		}
		if (fileId && selectedProject?.id) {
			importDataSource({
				projectId: selectedProject?.id ?? "",
				dataSources,
			});
		}
	}, [
		columnMappings,
		tableNameObj,
		selectedSheets,
		selectedProject,
		importDataSource,
		fileId,
	]);

	return (
		<>
			<NavigationConfirmationDialog
				isOpen={isOpen}
				onConfirm={onConfirm}
				onCancel={onCancel}
			/>
			<h1 className="text-xl font-bold mb-5">
				Import selected Sheets or Files
			</h1>
			<p className="w-[700px] text-md">
				Select files or selectedSheets to import. A Custom SQL query for
				databases or Excel (xlsx) files may be used if desired. The Data Source
				name may also be changed to help minimize ambiguity.
			</p>

			<div className="mt-6">
				<div className="mt-6">
					<div className="mb-4 bg-iris-mist p-2 rounded-md">
						<div className="flex flex-wrap gap-2">
							{selectedSheets.map((table) => (
								<Button
									key={table.name}
									variant={activeTable === table.name ? "default" : "outline"}
									onClick={() => setActiveTable(table.name)}
									className="text-sm"
								>
									{getDisplayName(table.name)}
								</Button>
							))}
						</div>
					</div>

					{isLoadingColumns ? (
						<div className="flex items-center justify-center h-48 border rounded-md">
							<Loader2 className="h-6 w-6 animate-spin text-primary" />
						</div>
					) : activeTable ? (
						<>
							<Tabs value={activeTab} onValueChange={setActiveTab}>
								<TabsList className="mb-4">
									<TabsTrigger
										value="columns"
										className="flex items-center gap-2"
									>
										<DataImportColumnsIcon className="h-4 w-4" />
										<span>Columns</span>
									</TabsTrigger>
									<TabsTrigger
										value="preview"
										className="flex items-center gap-2"
									>
										<DataImportPreviewIcon className="h-4 w-4" />
										<span>Preview</span>
									</TabsTrigger>
									<TabsTrigger
										value="exception"
										className="flex items-center gap-2"
									>
										<GeneralWarningIcon className="h-4 w-4" />
										<span>Exception</span>
									</TabsTrigger>
								</TabsList>
								<ColumnsTab
									tableName={activeTable}
									displayName={getDisplayName(activeTable)}
								/>
								<PreviewTab
									tableName={activeTable}
									apiTableName={(() => {
										const tableInfo = selectedSheets.find(
											(t) => t.name === activeTable
										);
										if (!tableInfo) return undefined;
										const qualified = getQualifiedTableName(tableInfo);
										return qualified !== activeTable ? qualified : undefined;
									})()}
								/>
								<ExceptionTab exceptions={exceptions} />
							</Tabs>
							<div className="flex justify-end gap-4 mt-6">
								<Button
									onClick={() => createDatasource()}
									disabled={!canImport}
								>
									Import
								</Button>
							</div>
						</>
					) : (
						<NoTableSelected />
					)}
				</div>
			</div>
		</>
	);
}
