import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import {
	Table,
	TableHeader,
	TableRow,
	TableHead,
	TableBody,
	TableCell,
} from "@/components/ui/table";
import { TabsContent } from "@/components/ui/tabs";
import { useTablesDataQuery } from "@/modules/DataImport/hooks/file/preview-data";
import { useAppSelector } from "@/hooks/use-store";
import { Loader2 } from "lucide-react";

export function PreviewTab({ tableName, apiTableName }: { tableName: string; apiTableName?: string }) {
	const { uploadedFile, columnMappings, databaseConnection, remoteConnection, selectedRemoteFiles } = useAppSelector(
		(s) => s.dataImport
	);
	const { data, isLoading } = useTablesDataQuery({
		tableName: apiTableName || tableName,
	});
	const isRemote = !uploadedFile && !databaseConnection.database && !!remoteConnection && selectedRemoteFiles.length > 0;
	const fileId = uploadedFile?.id || databaseConnection.database || (isRemote ? selectedRemoteFiles[0].path : "");
	const fileColumns = Object.values(columnMappings[fileId]?.[tableName] || {});
	if (isLoading) {
		return (
			<TabsContent value="preview">
				<div className="flex items-center justify-center h-48 border rounded-md">
					<Loader2 className="h-6 w-6 animate-spin text-primary" />
				</div>
			</TabsContent>
		);
	}

	return (
		<TabsContent value="preview">
			<Table parentClassName="border max-h-[400px] ">
				<TableHeader className="sticky top-0 bg-iris-mist z-10">
					<TableRow>
						{fileColumns
							.filter((col) => col.include)
							.map((col, index) => (
								<TableHead key={index} className="whitespace-nowrap">
									{col.targetColumn || col.sourceColumn}
								</TableHead>
							))}
					</TableRow>
				</TableHeader>
				<TableBody>
					{data?.value?.data?.map((row, rowIndex) => (
						<TableRow
							key={rowIndex}
							className={rowIndex % 2 === 0 ? "bg-white" : "bg-iris-mist"}
						>
							{fileColumns
								.filter((col) => col.include)
								.map((col, colIndex) => (
									<TableCell key={colIndex} className="whitespace-nowrap">
										{row?.[col.sourceColumn as string as keyof typeof row] !==
										undefined
											? String(
													row?.[col.sourceColumn as string as keyof typeof row]
												)
											: ""}
									</TableCell>
								))}
						</TableRow>
					))}
				</TableBody>
			</Table>
		</TabsContent>
	);
}
