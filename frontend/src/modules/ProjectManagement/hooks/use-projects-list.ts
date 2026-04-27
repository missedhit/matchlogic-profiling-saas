import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { ListProjectResponse } from "@/models/api-responses";

export function useProjectsList() {
	const BASE_PATH = `/projects`; // apiFetch will prefix NEXT_PUBLIC_API_URL
	const projectsQuery = useQuery({
		queryKey: ["projects"],
		queryFn: async () => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<ListProjectResponse>(
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
	});
	return projectsQuery;
}
