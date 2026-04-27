import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { TabsContent } from "@/components/ui/tabs";
import { useAppSelector } from "@/hooks/use-store";
import { store } from "@/store";
import { bindActionCreators } from "@reduxjs/toolkit";
import { dataImportActions } from "@/modules/DataImport/store/data-import-slice";

export function ColumnsTab({ tableName, displayName }: { tableName: string; displayName?: string }) {
	const { columnMappings, uploadedFile, tableNameObj, databaseConnection, selectedFileType, remoteConnection, selectedRemoteFiles } =
		useAppSelector((state) => state.dataImport);
	const { setColumnMappings, setTableNameObj } = bindActionCreators(
		dataImportActions,
		store.dispatch
	);
	const isRemote = !uploadedFile && !databaseConnection.database && !!remoteConnection && selectedRemoteFiles.length > 0;
	const fileId = uploadedFile?.id || databaseConnection.database || (isRemote ? selectedRemoteFiles[0].path : "");
	const fileColumns = Object.values(columnMappings[fileId]?.[tableName] || {});

	return (
		<TabsContent value="columns">
			<div className="flex items-center gap-2 mb-2 text-sm">
				<div className="flex-1">Table Name</div>
				<div className="flex-1">{displayName ?? tableName}</div>
				<Input
					key={tableName}
					placeholder="New Name"
					className="flex-1"
					defaultValue={tableNameObj?.[tableName] || ""}
					onChange={(e) =>
						setTableNameObj({ currentName: tableName, newName: e.target.value })
					}
				/>
			</div>
			<Table parentClassName="border h-[400px]">
				<TableHeader className="sticky top-0 bg-iris-mist z-10">
					<TableRow>
						<TableHead className="w-1/3">Current Name</TableHead>
						<TableHead className="w-1/3">New Name</TableHead>
						<TableHead className="w-1/3 text-center">
							Include in Import
						</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{fileId &&
						fileColumns.map((column, index) => (
							<TableRow
								key={index}
								className={index % 2 === 0 ? "bg-white" : "bg-iris-mist"}
							>
								<TableCell>{column.sourceColumn}</TableCell>
								<TableCell>
									<Input
										value={column.targetColumn}
										onChange={(e) =>
											setColumnMappings({
												fileId,
												tableName,
												columnName: column.sourceColumn,
												columnMapping: {
													...column,
													targetColumn: e.target.value,
												},
											})
										}
										placeholder="Enter new name (optional)"
									/>
								</TableCell>
								<TableCell className="text-center">
									<Checkbox
										checked={column.include}
										onCheckedChange={() =>
											setColumnMappings({
												fileId,
												tableName,
												columnName: column.sourceColumn,
												columnMapping: {
													...column,
													include: !column.include,
												},
											})
										}
									/>
								</TableCell>
							</TableRow>
						))}
					{fileColumns.length === 0 && (
						<TableRow>
							<TableCell colSpan={3} className="text-center py-4">
								No columns found in this file
							</TableCell>
						</TableRow>
					)}
				</TableBody>
			</Table>
		</TabsContent>
	);
}
