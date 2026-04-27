import { apiFetch } from "@/utils/apiFetch";
import { store } from "@/store";
import { useMutation } from "@tanstack/react-query";
import { queryClient } from "@/store/query-client";
import { useAppSelector } from "@/hooks/use-store";
import { bindActionCreators } from "@reduxjs/toolkit";
import { projectActions } from "../store/projectSlice";
import { DeleteProjectResponse } from "@/models/api-responses";

export function useDeleteProject({ id, name }: { id: string, name: string }) {
	const BASE_PATH = `/projects`;
	const { selectedProject } = useAppSelector((s) => s.projects);
	const { setSelectedProject } = bindActionCreators(projectActions, store.dispatch);
	const deleteProjectMutation = useMutation({
		mutationFn: async () => {
			const url = `${BASE_PATH}/${id}`;
			const response = await apiFetch<DeleteProjectResponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "DELETE",
					toastConfig: {
						successTitle: "Project Deleted",
						successDescription: "Project has been successfully deleted",
						errorTitle: "Failed to Delete Project",
						errorDescription: "Unable to delete project. Please try again."
					}
				}
			);
			return response;
		},
		onSuccess: () => {
			if (selectedProject?.id === id) {
				setSelectedProject(null);
			}
			queryClient.invalidateQueries({ queryKey: ['projects'] });
			queryClient.invalidateQueries({ queryKey: ["license-status"], refetchType: "all" });
			// Remove all caches related to the deleted project
			queryClient.removeQueries({ queryKey: ["data-source", id] });
			queryClient.removeQueries({ queryKey: ["match-summary", id] });
			queryClient.removeQueries({ queryKey: ["match-configuration", id] });
			queryClient.removeQueries({ queryKey: ["match-definition", id] });
		}
	});
	return deleteProjectMutation;
}
