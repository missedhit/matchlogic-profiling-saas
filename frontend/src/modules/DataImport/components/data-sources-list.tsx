"use client";
import { useDataSourceQuery } from "@/hooks/use-data-source-query";
import { DataPreviewTable } from "./ui/datasource-list/data-preview-table";
import { useAppSelector } from "@/hooks/use-store";
import { useState } from "react";
import { Datasource } from "@/models/api-responses";
import { Button } from "@/components/ui/button";
import { DataSourceTable } from "./ui/datasource-list/data-source-table";
import { useRouter } from "next/navigation";
import { QueryErrorBoundary } from "@/components/common/QueryErrorBoundary";

// Profiling SaaS DataSourcesList — slimmed during saas-extract.
// Removed: useCheckRemoteUpdates (cloud-storage refresh), OAuth disconnection alerts.
export default function DataSourcesList() {
	const router = useRouter();
	const projectId = useAppSelector((s) => s.projects.selectedProject)?.id;
	const { data, isError, error, refetch } = useDataSourceQuery({ projectId });
	const [selectedDataSource, setSelectedDataSource] =
		useState<Datasource | null>(null);

	return (
		<div className="space-y-8">
			<div className="flex justify-between items-center">
				<h1 className="text-3xl font-bold">Data Import</h1>
				<Button onClick={() => router.push("/data-import")}>
					Add New Data
				</Button>
			</div>
			<QueryErrorBoundary isError={isError} error={error} refetch={refetch}>
				<DataSourceTable
					dataSources={data}
					selectedDataSource={selectedDataSource}
					setSelectedDataSource={setSelectedDataSource}
					projectId={projectId}
				/>
				{selectedDataSource && (
					<DataPreviewTable selectedDataSource={selectedDataSource} />
				)}
			</QueryErrorBoundary>
		</div>
	);
}
