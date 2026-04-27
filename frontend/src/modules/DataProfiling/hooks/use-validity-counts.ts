import { useState, useEffect } from "react";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";
import { apiFetch } from "@/utils/apiFetch";
import { store } from "@/store";
import { AdvanceDataResponse } from "@/models/api-responses";

/**
 * @deprecated This hook is deprecated and should not be used.
 * It makes excessive API calls (2 calls per field) on initial load.
 * Use calculateValidityCounts from @/modules/DataProfiling/utils/calculate-validity instead.
 *
 * The new approach calculates validity directly from the column profile data
 * without making any API calls, matching the Angular app implementation.
 */
export const useValidityCounts = (
	data: ColumnProfile,
	dataSourceId: string
) => {
	const [validityCounts, setValidityCounts] = useState<
		Record<string, { valid: number; invalid: number }>
	>({});
	const [loading, setLoading] = useState(false);

	useEffect(() => {
		const fetchValidityCounts = async () => {
			if (!dataSourceId || Object.keys(data).length === 0) return;

			setLoading(true);

			try {
				const results: Record<string, { valid: number; invalid: number }> = {};

				// Initialize all fields
				Object.keys(data).forEach((fieldKey) => {
					results[fieldKey] = { valid: 0, invalid: 0 };
				});

				// Create fetch promises for all document IDs
				const fetchPromises: Promise<{
					fieldKey: string;
					type: "valid" | "invalid";
					count: number;
				}>[] = [];

				Object.entries(data).forEach(([fieldKey, fieldData]) => {
					// For validity, use the same approach as getFilterOptions
					const identifiedPattern = fieldData.pattern;
					if (!identifiedPattern) {
						return;
					}

					const validDocumentId =
						fieldData.patternMatchRowDocumentIds?.[
						`${identifiedPattern}_Valid`
						];
					const invalidDocumentId =
						fieldData.patternMatchRowDocumentIds?.[
						`${identifiedPattern}_Invalid`
						];

					if (validDocumentId) {
						fetchPromises.push(
							(async () => {
								try {
									const url = `/DataProfile/AdvanceData?dataSourceId=${dataSourceId}&documentId={${validDocumentId}}`;
									const result = await apiFetch<AdvanceDataResponse>(
										store.dispatch,
										store.getState,
										url,
										{ method: "GET" }
									);

									const count = result?.value?.rowReferences?.length || 0;

									return { fieldKey, type: "valid" as const, count };
								} catch (error) {
									console.error(
										`Failed to fetch valid count for ${fieldKey}:`,
										error
									);
									return { fieldKey, type: "valid" as const, count: 0 };
								}
							})()
						);
					}

					if (invalidDocumentId) {
						fetchPromises.push(
							(async () => {
								try {
									const url = `/DataProfile/AdvanceData?dataSourceId=${dataSourceId}&documentId={${invalidDocumentId}}`;
									const result = await apiFetch<AdvanceDataResponse>(
										store.dispatch,
										store.getState,
										url,
										{ method: "GET" }
									);

									const count = result?.value?.rowReferences?.length || 0;
									return { fieldKey, type: "invalid" as const, count };
								} catch (error) {
									console.error(
										`Failed to fetch invalid count for ${fieldKey}:`,
										error
									);
									return { fieldKey, type: "invalid" as const, count: 0 };
								}
							})()
						);
					}
				});

				const fetchResults = await Promise.all(fetchPromises);

				// Update results with fetched counts
				fetchResults.forEach(({ fieldKey, type, count }) => {
					if (results[fieldKey]) {
						results[fieldKey][type] = count;
					}
				});

				setValidityCounts(results);
			} catch (error) {
				console.error("Failed to fetch validity counts:", error);
				// Initialize with zeros on error
				const errorResults: Record<string, { valid: number; invalid: number }> =
					{};
				Object.keys(data).forEach((fieldKey) => {
					errorResults[fieldKey] = { valid: 0, invalid: 0 };
				});
				setValidityCounts(errorResults);
			} finally {
				setLoading(false);
			}
		};

		fetchValidityCounts();
	}, [dataSourceId]); // Only depend on dataSourceId to prevent infinite loops

	return { validityCounts, loading };
};
