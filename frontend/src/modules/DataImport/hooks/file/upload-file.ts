import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { bindActionCreators } from "@reduxjs/toolkit";
import { dataImportActions } from "../../store/data-import-slice";
import {
	PresignedUploadResponse,
	PreviewTableReponse,
	SupportedData,
	UploadFileResponse,
} from "@/models/api-responses";
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

	const [uploadStep, setUploadStep] = useState<UploadStep>("uploading");

	const uploadFileMutation = useMutation({
		mutationFn: async ({
			projectId,
			dataSourceType,
			file,
		}: ImportFileParams) => {
			setUploadStep("uploading");

			// ── Step 1: Mint a presigned PUT URL from the API ─────────────────────
			const presignResponse = await apiFetch<PresignedUploadResponse>(
				store.dispatch,
				store.getState,
				`/dataimport/File/PresignedUpload`,
				{
					method: "POST",
					body: JSON.stringify({
						projectId,
						sourceType: dataSourceType,
						fileName: file.name,
					}),
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Success-Toast": "true",
					},
				}
			);
			if (!presignResponse?.value) {
				throw new Error("Failed to mint presigned upload URL");
			}
			const presigned = presignResponse.value;

			// ── Step 2: PUT the file directly to S3 (bypasses the API) ────────────
			const putResponse = await fetch(presigned.presignedUrl, {
				method: "PUT",
				body: file,
			});
			if (!putResponse.ok) {
				throw new Error(
					`S3 upload failed: ${putResponse.status} ${putResponse.statusText}`
				);
			}

			// ── Step 3: Confirm the upload — API verifies S3 + persists FileImport ─
			const confirmResponse = await apiFetch<UploadFileResponse>(
				store.dispatch,
				store.getState,
				`/dataimport/File/Confirm`,
				{
					method: "POST",
					body: JSON.stringify({
						fileId: presigned.fileId,
						projectId,
						sourceType: dataSourceType,
						originalName: presigned.originalName,
						fileExtension: presigned.fileExtension,
						s3Key: presigned.s3Key,
					}),
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Success-Toast": "true",
					},
				}
			);
			if (!confirmResponse?.value) {
				throw new Error("File upload confirm failed");
			}

			// ── Step 4: Fetch tables, addressed by FileId (BE resolves to S3) ─────
			setUploadStep("loading-tables");
			const uploadedFile = confirmResponse.value;
			const tablesPayload = {
				type: SupportedData[uploadedFile.dataSourceType],
				parameters: {
					FileId: uploadedFile.id,
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
			queryClient.setQueryData(["file-tables", uploadedFile.id], tablesResponse);

			setUploadedFile(uploadedFile);
			setSelectedSheets(tablesResponse.value.tables);
			// TODO (M2): re-add Excel sheet picker — for now jump straight to column-mapping
			//            (CSV always has one "sheet"; Excel uses the first sheet).
			router.push(`/data-import/column-mapping`);
		},
		onSettled: () => {
			setUploadStep("uploading");
		},
	});

	return { ...uploadFileMutation, uploadStep };
};
