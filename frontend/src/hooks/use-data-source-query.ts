import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { DatasourceListResponse } from "@/models/api-responses";

export const useDataSourceQuery = ({ projectId }: { projectId: string | undefined }) => {
	const BASE_PATH = `/dataimport/datasource?projectId=${projectId}`;
	const dataSourceQuery = useQuery({
		queryKey: ["data-source", projectId],
		queryFn: async () => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<DatasourceListResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "GET",
				}
			);
			if (!response || !Array.isArray(response.value)) {
				return { ...response, value: [] };
			}
			return response;
		},
		enabled: !!projectId,
		select: (data) => {
			if (!data.value || data.value.length === 0) return [];
			const sortedData = data.value.sort(
				(a, b) =>
					new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
			);
			return sortedData;
		},
	});
	return dataSourceQuery;
};
