import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { RawDataResponse } from "@/models/api-responses";
import { NO_CACHE_QUERY_OPTIONS, noCacheHeaders } from "@/utils/query-options";



export const useDataSourceDataQuery = ({
	dataSourceId,
	page,
	pageSize,
}: {
	dataSourceId: string;
	page?: number;
	pageSize?: number;
}) => {
	const BASE_PATH = `/dataimport/datasource/data?id=${dataSourceId}&pageNumber=${page}&pageSize=${pageSize}`;
	const dataSourceQuery = useQuery({
		queryKey: ["raw-data", dataSourceId, page, pageSize],
		queryFn: async () => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<RawDataResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "GET",
					headers: noCacheHeaders(),
				}
			);
			if (!response?.value) throw new Error("Failed to load data source data");
			return response;
		},
		enabled: !!dataSourceId,
		...NO_CACHE_QUERY_OPTIONS,
	});
	return dataSourceQuery;
};

