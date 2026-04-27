import {
	Table,
	TableHead,
	TableRow,
	TableHeader,
	TableBody,
	TableCell,
} from "@/components/ui/table";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
	DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Pencil, Trash2, Eye } from "lucide-react";
import { GeneralRetryIcon, DataImportDatabaseIcon } from "@/assets/icons";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { useState } from "react";
import { useUpdateDataSourceMutation } from "../../../hooks/datasource/update-datasource";
import {
	AlertDialog,
	AlertDialogAction,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
	AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { useDeleteDataSourceMutation } from "../../../hooks/datasource/delete-datasource";
import { Datasource } from "@/models/api-responses";
import { formatBytes } from "@/utils/format-bytes";
import { useRouteGuard } from "@/providers/route-guard-provider";
import { Skeleton } from "@/components/ui/skeleton";
import {
	Tooltip,
	TooltipContent,
	TooltipTrigger,
} from "@/components/ui/tooltip";

interface DataSourceTableProps {
	dataSources: Datasource[] | null | undefined;
	selectedDataSource: Datasource | null;
	setSelectedDataSource: (dataSource: Datasource | null) => void;
	projectId: string | undefined;
}

export function DataSourceTable({
	dataSources,
	selectedDataSource,
	setSelectedDataSource,
	projectId,
}: DataSourceTableProps) {
	const { importing } = useRouteGuard();
	return (
		<div className="rounded-md border">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead>Data Source</TableHead>
						<TableHead>Source Type</TableHead>
						{/* <TableHead>Data Size</TableHead> */}
						<TableHead>Record Count</TableHead>
						<TableHead>Date added</TableHead>
						<TableHead>Last updated</TableHead>
						<TableHead className="w-[100px] text-right">Actions</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{dataSources?.map((dataSource) => (
						<TableRow
							key={dataSource.id}
							className={`group/row cursor-pointer transition-colors hover:bg-iris-mist/60 ${
								selectedDataSource?.id === dataSource.id ? "bg-iris-mist" : ""
							}`}
							onClick={() => setSelectedDataSource(dataSource)}
						>
							<TableCell>
								<div className="flex items-center">
									<div className="bg-primary p-1 rounded mr-2">
										<DataImportDatabaseIcon className="h-4 w-4 text-white" />
									</div>
									{dataSource.name}
									<span className="ml-2 inline-flex items-center gap-1 text-[11px] text-primary opacity-0 group-hover/row:opacity-100 transition-opacity">
										<Eye className="h-3 w-3" />
										Click to preview
									</span>
								</div>
							</TableCell>
							<TableCell>{dataSource.sourceType}</TableCell>
							{/* <TableCell>{formatBytes(dataSource.size)}</TableCell> */}
							<TableCell>{dataSource.recordCount.toLocaleString()}</TableCell>
							<TableCell>
								{dataSource.createdAt
									? new Date(dataSource.createdAt).toLocaleDateString(
											"en-GB",
											{
												day: "2-digit",
												month: "long",
												year: "numeric",
											}
										)
									: ""}
							</TableCell>
							<TableCell>
								{dataSource.modifiedAt
									? new Date(dataSource.modifiedAt).toLocaleDateString(
											"en-GB",
											{
												day: "2-digit",
												month: "long",
												year: "numeric",
											}
										)
									: ""}
							</TableCell>
							<TableCell
								className="text-right"
								onClick={(e) => e.stopPropagation()}
							>
								<div className="flex justify-end space-x-2">
									<UpdateDataSourceModal
										dataSource={dataSource}
										selectedDataSource={selectedDataSource}
										setSelectedDataSource={setSelectedDataSource}
									/>
									<DeleteDataSourceModal
										dataSource={dataSource}
										selectedDataSource={selectedDataSource}
										setSelectedDataSource={setSelectedDataSource}
									/>
								</div>
							</TableCell>
						</TableRow>
					))}
					{importing && (
						<TableRow>
							<TableCell>
								<Skeleton className="h-4 w-32" />
							</TableCell>
							<TableCell>
								<Skeleton className="h-4 w-32" />
							</TableCell>
							<TableCell>
								<Skeleton className="h-4 w-32" />
							</TableCell>
							<TableCell>
								<Skeleton className="h-4 w-32" />
							</TableCell>
							<TableCell>
								<Skeleton className="h-4 w-32" />
							</TableCell>
							<TableCell>
								<Skeleton className="h-4 w-32" />
							</TableCell>
						</TableRow>
					)}
				</TableBody>
			</Table>
		</div>
	);
}

function UpdateDataSourceModal({
	dataSource,
	selectedDataSource,
	setSelectedDataSource,
}: {
	dataSource: Datasource;
	selectedDataSource: Datasource | null;
	setSelectedDataSource: (dataSource: Datasource | null) => void;
}) {
	const [isOpen, setIsOpen] = useState(false);
	const [name, setName] = useState(dataSource.name);
	const [error, setError] = useState<string>("");
	const { mutate } = useUpdateDataSourceMutation();
	const validateForm = () => {
		if (!name.trim()) {
			setError("Data source name is required");
			return false;
		}
		setError("");
		return true;
	};
	const handleSubmit = async (e: React.FormEvent) => {
		e.preventDefault();
		if (validateForm()) {
			try {
				await mutate({ id: dataSource.id, name });
				setIsOpen(false);
			} catch (err) {
				setError("Failed to update data source name");
			}
		}
	};
	return (
		<Dialog open={isOpen} onOpenChange={setIsOpen}>
			<Button variant="ghost" size="icon" requiredPermission="dataimport.execute" onClick={() => setIsOpen(true)}>
				<span className="sr-only">Rename data source</span>
				<Pencil className="h-4 w-4" aria-hidden="true" />
			</Button>
			<DialogContent className="sm:max-w-[500px]">
				<form onSubmit={handleSubmit}>
					<DialogHeader>
						<DialogTitle>Update Data Source Name</DialogTitle>
						<DialogDescription>
							Change the name of your data source.
						</DialogDescription>
					</DialogHeader>
					<div className="grid gap-4 py-4">
						<div className="grid grid-cols-4 items-center gap-4">
							<Label htmlFor="name" className="text-right">
								Name
							</Label>
							<div className="col-span-3 space-y-1">
								<Input
									id="name"
									value={name}
									onChange={(e) => setName(e.target.value)}
									className={error ? "border-red-500" : ""}
								/>
								{error && <p className="text-xs text-red-500">{error}</p>}
							</div>
						</div>
					</div>
					<DialogFooter>
						<Button
							type="button"
							variant="outline"
							onClick={() => setIsOpen(false)}
						>
							Cancel
						</Button>
						<Button type="submit">Save Changes</Button>
					</DialogFooter>
				</form>
			</DialogContent>
		</Dialog>
	);
}

const REMOTE_SOURCE_TYPES = new Set([
	"FTP",
	"SFTP",
	"S3",
	"AzureBlob",
	"GoogleDrive",
	"Dropbox",
	"OneDrive",
	"PostgreSQL",
	"SQLServer",
	"MySQL",
]);

function isRemoteSource(sourceType: string): boolean {
	return REMOTE_SOURCE_TYPES.has(sourceType);
}

function DeleteDataSourceModal({
	dataSource,
	selectedDataSource,
	setSelectedDataSource,
}: {
	dataSource: Datasource;
	selectedDataSource: Datasource | null;
	setSelectedDataSource: (dataSource: Datasource | null) => void;
}) {
	const [isOpen, setIsOpen] = useState(false);
	const { mutate } = useDeleteDataSourceMutation();
	const onConfirm = () => {
		mutate({ id: dataSource.id });
		setIsOpen(false);
		if (selectedDataSource?.id === dataSource.id) {
			setSelectedDataSource(null);
		}
	};
	return (
		<AlertDialog open={isOpen} onOpenChange={setIsOpen}>
			<Button variant="ghost" size="icon" requiredPermission="dataimport.execute" onClick={(e) => { e.stopPropagation(); setIsOpen(true); }}>
				<span className="sr-only">Delete data source</span>
				<Trash2 className="text-red-500 h-4 w-4" aria-hidden="true" />
			</Button>
			<AlertDialogContent>
				<AlertDialogHeader>
					<AlertDialogTitle>Are you sure?</AlertDialogTitle>
					<AlertDialogDescription>
						This action cannot be undone. This will permanently delete the data
						source.
						<strong> {dataSource.name}</strong>.
					</AlertDialogDescription>
				</AlertDialogHeader>
				<AlertDialogFooter>
					<AlertDialogCancel>Cancel</AlertDialogCancel>
					<AlertDialogAction
						onClick={onConfirm}
						className="bg-red-600 text-white hover:bg-red-700"
					>
						Delete
					</AlertDialogAction>
				</AlertDialogFooter>
			</AlertDialogContent>
		</AlertDialog>
	);
}
