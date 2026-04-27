import { AdvanceDataResponse } from "@/models/api-responses";
import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";

export const useDataSourceDataQuery = ({
	dataSourceId,
	documentId,
	...otherParams
}: {
	dataSourceId: string;
	documentId?: string;
	[key: string]: any; // Allow other parameters for compatibility
}) => {
	const BASE_PATH = `/DataProfile/AdvanceData?dataSourceId=${dataSourceId}&documentId={${documentId || ""}}`;
	const dataSourceQuery = useQuery({
		queryKey: ["advance-data", dataSourceId, documentId, otherParams],
		queryFn: async () => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<AdvanceDataResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "GET",
					// you can add X-Skip-Loader: "true" here if you don't want loader
				}
			);
			if (!response?.value) throw new Error("Failed to load profiling data");
			return response;
		},
		enabled: !!documentId && !!dataSourceId, // Only run when we have both dataSourceId and documentId
	});
	return dataSourceQuery;
};
