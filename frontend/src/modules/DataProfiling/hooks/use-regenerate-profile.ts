import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { RunResponse } from "@/models/run-response";
import { useJobState } from "@/providers/job-state-provider";

interface RegenerateProfileParams {
  projectId: string;
  dataSourceId: string;
  projectName: string;
}

export const useRegenerateProfile = () => {
  const queryClient = useQueryClient();
  const { startProcessing } = useJobState();
  const BASE_PATH = `/DataProfile/GenerateAdvance`;

  return useMutation({
    mutationFn: async ({ projectId, dataSourceId }: RegenerateProfileParams) => {
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
    onSuccess: (data, variables) => {
      if (data && data.value && data.value.projectRun && data.value.projectRun.id) {
        startProcessing(
          data.value.projectRun.id,
          "/data-profiling",
          {
            projectId: variables.projectId,
            dataSourceId: variables.dataSourceId,
            queryKey: ["advance-analytics", variables.dataSourceId],
          },
          variables.projectName,
          "Regenerate Profile"
        );
      }
    },
  });
};
