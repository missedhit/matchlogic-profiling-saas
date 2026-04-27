import { apiFetch } from "@/utils/apiFetch";
import { store } from "@/store";
import { useMutation } from "@tanstack/react-query";
import { queryClient } from "@/store/query-client";
import { useRouter } from "next/navigation";
import { bindActionCreators } from "@reduxjs/toolkit";
import { projectActions } from "../store/projectSlice";
import { CreateProjectResponse } from "@/models/api-responses";
import { useRouteGuard } from "@/providers/route-guard-provider";

export function useCreateProject() {
	const { setDataSourceId } = useRouteGuard()
	const { setSelectedProject } = bindActionCreators(projectActions, store.dispatch);
	const BASE_PATH = `/projects`;
	const createProjectMutation = useMutation({
		mutationFn: async ({
			name,
			description,
		}: {
			name: string;
			description: string;
		}) => {
			const url = `${BASE_PATH}`;
			const response = await apiFetch<CreateProjectResponse>(store.dispatch, store.getState, url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
				},
				body: JSON.stringify({
					name,
					description,
				}),
				toastConfig: {
					successTitle: "Project Created",
					successDescription: `Successfully created project "${name}"`,
					errorTitle: "Failed to Create Project",
					errorDescription: `Unable to create project "${name}". Please try again.`
				}
			});
			return response;
		},
		onSuccess: (data) => {
			setDataSourceId("")
			queryClient.invalidateQueries({ queryKey: ["projects"] }).then(() => {
				if (data?.value?.id) { setSelectedProject(data.value); }
			});
		},
	});
	return createProjectMutation;
}
