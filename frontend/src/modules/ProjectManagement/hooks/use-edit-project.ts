import { apiFetch } from "@/utils/apiFetch";
import { store } from "@/store";
import { useMutation } from "@tanstack/react-query";
import { queryClient } from "@/store/query-client";
import { UpdateProjectResponse } from "@/models/api-responses";

export function useEditProject() {
	const BASE_PATH = `/projects`;
	const editProjectMutation = useMutation({
		mutationFn: async ({
			id,
			name,
			description,
		}: {
			id: string;
			name: string;
			description: string;
		}) => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<UpdateProjectResponse>(store.dispatch, store.getState, url, {
				method: "PUT",
				headers: {
					"Content-Type": "application/json",
				},
				body: JSON.stringify({
					name,
					description,
					id,
				}),
				toastConfig: {
					successTitle: "Project Updated",
					successDescription: `Successfully updated project "${name}"`,
					errorTitle: "Failed to Update Project",
					errorDescription: `Unable to update project "${name}". Please try again.`
				}
			});
			return response;
		},
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["projects"] });
		},
	});
	return editProjectMutation;
}