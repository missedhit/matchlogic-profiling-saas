"use client";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useAppSelector } from "@/hooks/use-store";
import { GeneralInfoIcon } from "@/assets/icons";
import { FileUploadArea } from "./ui/data-import/file-upload-area";
import { useRouteGuard } from "@/providers/route-guard-provider";
import { useDataSourceQuery } from "@/hooks/use-data-source-query";
import { useRouter } from "next/navigation";
import { useEffect, useRef } from "react";
import { Button } from "@/components/ui/button";
import { ArrowLeft } from "lucide-react";
import { dataImportActions } from "@/modules/DataImport/store/data-import-slice";
import { store } from "@/store";
import { bindActionCreators } from "@reduxjs/toolkit";

// Profiling SaaS DataImport — slimmed from main-product version during saas-extract.
// Removed: database/FTP/S3/Azure/OAuth connection areas, license trial banner,
// import-options-container picker. Single path: CSV/Excel upload only.
export default function DataImportContainer() {
	const selectedProject = useAppSelector((s) => s.projects.selectedProject);
	const selectedFileType = useAppSelector((s) => s.dataImport.selectedFileType);

	const { setUploadedFile } = bindActionCreators(
		dataImportActions,
		store.dispatch
	);
	const { setFile } = useRouteGuard();

	const prevFileTypeRef = useRef<string | null>(null);

	useEffect(() => {
		const prev = prevFileTypeRef.current;
		prevFileTypeRef.current = selectedFileType;
		if (prev === null) return;
		if (prev === selectedFileType) return;
		setFile(null);
		setUploadedFile(null);
	}, [selectedFileType, setFile, setUploadedFile]);

	const router = useRouter();
	const { data: dataSources } = useDataSourceQuery({
		projectId: selectedProject?.id,
	});
	const hasDataSources = Array.isArray(dataSources) && dataSources.length > 0;

	return (
		<div className="space-y-8">
			<div className="flex items-center justify-between">
				<h1 className="text-xl font-semibold">Data Import</h1>
				{hasDataSources && (
					<Button
						variant="outline"
						size="sm"
						onClick={() => router.push("/data-import/data-sources")}
					>
						<ArrowLeft className="h-4 w-4 mr-1.5" />
						Back to Data Sources
					</Button>
				)}
			</div>

			{selectedProject && (
				<Alert className="bg-iris-mist border-primary text-primary">
					<GeneralInfoIcon className="h-4 w-4" />
					<AlertTitle>Working on project: {selectedProject.name}</AlertTitle>
					<AlertDescription>
						All imported data will be associated with this project. CSV and
						Excel only, up to 1000 records lifetime per account.
					</AlertDescription>
				</Alert>
			)}

			<div className="grid grid-cols-1 gap-2 items-start">
				<FileUploadArea />
			</div>
		</div>
	);
}
