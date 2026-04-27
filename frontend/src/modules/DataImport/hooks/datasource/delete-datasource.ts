import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation } from "@tanstack/react-query";
import { queryClient } from "@/store/query-client";
import { DeleteDatasourceResponse } from "@/models/api-responses";
import { useAppSelector } from "@/hooks/use-store";
import { useRouter } from "next/navigation";


export function useDeleteDataSourceMutation() {
	const { selectedProject } = useAppSelector((s) => s.projects);
	const router = useRouter();
	const mutation = useMutation({
		mutationFn: async ({ id }: { id: string }) => {
			const url = `/dataimport/DataSource/${encodeURIComponent(id)}?projectId=${selectedProject?.id}`;
			const response = await apiFetch<DeleteDatasourceResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "DELETE",
					headers: {
						"Content-Type": "application/json",
					},
					toastConfig: {
						successTitle: "Data Source Deleted",
						successDescription: "Successfully deleted data source",
						errorTitle: "Delete Failed",
						errorDescription: "Failed to delete data source",
					},
				}
			);
			return response;
		},
		onSuccess: async () => {
			// Check if this was the last datasource — redirect to /data-import
			const cached = queryClient.getQueryData<any>(["data-source", selectedProject?.id]);
			const currentList = Array.isArray(cached?.value) ? cached.value : Array.isArray(cached) ? cached : [];
			if (currentList.length <= 1 && selectedProject?.id) {
				router.push(`/data-import?projectId=${selectedProject.id}`);
			}

			// Refetch datasource list (license invalidation removed in saas-extract)
			queryClient.invalidateQueries({ queryKey: ["data-source"] });
			queryClient.invalidateQueries({ queryKey: ["data-profile"] });
			queryClient.invalidateQueries({ queryKey: ["advance-analytics"] });
		},
	});
	return mutation;
}
