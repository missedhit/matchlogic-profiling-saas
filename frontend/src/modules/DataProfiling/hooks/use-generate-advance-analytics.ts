import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { RunResponse } from "@/models/run-response";
export const useGenerateAdvanceAnalytics = ({
	projectId,
	dataSourceId,
}: {
	projectId: string;
	dataSourceId: string;
}) => {
	const BASE_PATH = `/DataProfile/GenerateAdvance`; // apiFetch will prefix NEXT_PUBLIC_API_URL
	const generateAdvanceAnalyticsQuery = useQuery({
		queryKey: ["generate-advance-analytics", dataSourceId],
		queryFn: async () => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<RunResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "POST",
					body: JSON.stringify({ projectId, dataSourceIds: [dataSourceId] }),
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Success-Toast": "true",
					},
				}
			);
			return response;
		},
	});

	return generateAdvanceAnalyticsQuery;
};
