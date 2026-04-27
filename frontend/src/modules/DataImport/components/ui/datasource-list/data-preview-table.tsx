import { CustomPagination } from "@/components/ui/custom-pagination";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { useDataSourceDataQuery } from "@/modules/DataImport/hooks/datasource/fetch-data";
import { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
import { Datasource } from "@/models/api-responses";

interface DataPreviewTableProps {
	selectedDataSource: Datasource;
}

export function DataPreviewTable({
	selectedDataSource,
}: DataPreviewTableProps) {
	const [currentPage, setCurrentPage] = useState(1);
	const [totalPages, setTotalPages] = useState(1);
	const [pageSize, setPageSize] = useState(50);
	const { data, isLoading } = useDataSourceDataQuery({
		dataSourceId: selectedDataSource.id,
		page: currentPage,
		pageSize: pageSize,
	});
	const [headers, setHeaders] = useState([] as string[]);

	// Reset to page 1 when switching data sources
	useEffect(() => {
		setCurrentPage(1);
	}, [selectedDataSource.id]);

	useEffect(() => {
		if (data?.value?.totalCount) {
			setTotalPages(Math.ceil(data.value.totalCount / pageSize));
		}
		if (data && data.value && data.value.data && data.value.data.length > 0) {
			const columns = Object.keys(data.value.data[0]).filter(
				(column) => !["_id", "_metadata"].includes(column)
			);
			setHeaders(columns);
		} else {
			setHeaders([]);
		}
	}, [data]);

	if (isLoading) {
		return (
			<div className="flex items-center justify-center h-[400px]">
				<Loader2 className="h-8 w-8 animate-spin text-primary" />
			</div>
		);
	}

	return (
		<>
			<Table parentClassName="h-[400px] border border-gray-200">
				<TableHeader>
					<TableRow>
						{headers.map((header) => (
							<TableHead key={header}>{header}</TableHead>
						))}
					</TableRow>
				</TableHeader>
				<TableBody>
					{data?.value?.data && data?.value?.data.length > 0 ? (
						data?.value?.data.map((row, rowIndex) => (
							<TableRow key={rowIndex}>
								{headers.map((header) => (
									<TableCell key={header}>
										{row[header as keyof typeof row] as string}
									</TableCell>
								))}
							</TableRow>
						))
					) : (
						<TableRow>
							<TableCell colSpan={headers.length} className="text-center">
								No records found.
							</TableCell>
						</TableRow>
					)}
				</TableBody>
			</Table>
			<div>
				<CustomPagination
					currentPage={currentPage}
					totalPages={totalPages}
					onPageChange={setCurrentPage}
				/>
			</div>
		</>
	);
}
