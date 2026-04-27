import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { DataSourceHeadersResponse } from "@/models/datasource-headers-response";

export const useDataSourceHeadersQuery = ({
	id,
}: {
	id: string | undefined;
}) => {
	const BASE_PATH = `/DataImport/DataSource/Headers/${id}`;
	const dataSourceQuery = useQuery({
		queryKey: ["data-source-headers", id],
		queryFn: async () => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<DataSourceHeadersResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "GET",
					// you can add X-Skip-Loader: "true" here if you don't want loader
				}
			);
			return response;
		},
		enabled: !!id,
	});
	return dataSourceQuery;
};
