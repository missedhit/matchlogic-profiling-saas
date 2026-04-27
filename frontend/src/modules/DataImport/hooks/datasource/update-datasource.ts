import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation } from "@tanstack/react-query";
import { queryClient } from "@/store/query-client";
import { UpdateDatasourceResponse } from "@/models/api-responses";



export function useUpdateDataSourceMutation() {
	const updateDataSourceMutation = useMutation({
		mutationFn: async ({ id, name }: { id: string; name: string }) => {
			const url = `/dataimport/DataSource/`;
			const payload = {
				id,
				name,
			};
			const response = await apiFetch<UpdateDatasourceResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "PATCH",
					body: JSON.stringify(payload),
					headers: {
						"Content-Type": "application/json",
					},
					toastConfig: {
						successTitle: "Data Source Updated",
						successDescription: "Successfully updated data source",
						errorTitle: "Update Failed",
						errorDescription: "Failed to update data source",
					}
				}
			);
			return response;
		},
		onSuccess: (data) => {
			queryClient.invalidateQueries({
				queryKey: ["data-source"],
			});
		},
		onError: () => {
			console.error("Failed to update data source")
		},
	});
	return updateDataSourceMutation;
}
