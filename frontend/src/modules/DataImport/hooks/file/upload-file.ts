import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { bindActionCreators } from "@reduxjs/toolkit";
import { dataImportActions } from "../../store/data-import-slice";
import { PreviewTableReponse, SupportedData, UploadFileResponse } from "@/models/api-responses";
import { useState } from "react";

interface ImportFileParams {
	projectId: string;
	dataSourceType: string;
	file: File;
}

/** Visible sub-step while the button is in its pending state. */
export type UploadStep = "uploading" | "loading-tables";

export const useUploadFileMutation = () => {
	const router = useRouter();
	const queryClient = useQueryClient();
	const { setUploadedFile, setSelectedSheets } = bindActionCreators(dataImportActions, store.dispatch);

	// Tracks which sub-step is currently running so the button label can update.
	const [uploadStep, setUploadStep] = useState<UploadStep>("uploading");

	const uploadFileMutation = useMutation({
		mutationFn: async ({
			projectId,
			dataSourceType,
			file,
		}: ImportFileParams) => {
			// ── Step 1: Upload the file ────────────────────────────────────────────
			setUploadStep("uploading");
			const uploadUrl = `/dataimport/File?projectId=${encodeURIComponent(
				projectId
			)}&sourceType=${encodeURIComponent(dataSourceType)}`;
			const formData = new FormData();
			formData.append("file", file);
			const uploadResponse = await apiFetch<UploadFileResponse>(
				store.dispatch,
				store.getState,
				uploadUrl,
				{
					method: "POST",
					body: formData,
					headers: { "X-Skip-Success-Toast": "true" },
				}
			);
			// apiFetch returns {} on non-2xx instead of throwing. Treat a missing
			// value as a failed upload so onError is taken and we do NOT navigate.
			if (!uploadResponse?.value) {
				throw new Error("File upload failed");
			}

			// ── Step 2: Fetch tables using the just-uploaded file ─────────────────
			setUploadStep("loading-tables");
			const uploadedFile = uploadResponse.value;
			const tablesPayload = {
				type: SupportedData[uploadedFile.dataSourceType],
				parameters: {
					FilePath: uploadedFile.filePath,
				},
			};
			const tablesResponse = await apiFetch<PreviewTableReponse>(
				store.dispatch,
				store.getState,
				`/dataimport/Preview/Tables`,
				{
					method: "POST",
					body: JSON.stringify(tablesPayload),
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Toast": "true",
					},
				}
			);
			if (!tablesResponse?.value) {
				throw new Error("Failed to load file tables");
			}

			return { uploadedFile, tablesResponse };
		},
		onSuccess: ({ uploadedFile, tablesResponse }) => {
			// Seed the React Query cache so select-table renders immediately
			// without issuing a second network request for the same data.
			queryClient.setQueryData(["file-tables", uploadedFile.id], tablesResponse);

			// Hydrate Redux with both the uploaded file and the pre-fetched tables.
			setUploadedFile(uploadedFile);
			setSelectedSheets(tablesResponse.value.tables);
			// TODO (M2): re-add Excel sheet picker — for now jump straight to column-mapping
			//            (CSV always has one "sheet"; Excel uses the first sheet).
			router.push(`/data-import/column-mapping`);
		},
		onSettled: () => {
			// Reset sub-step label for subsequent uploads.
			setUploadStep("uploading");
		},
	});

	return { ...uploadFileMutation, uploadStep };
};
