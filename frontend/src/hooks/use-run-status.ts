import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/utils/apiFetch";
import { RunStatus } from "@/models/run-status";
import { store } from "@/store";

export const useRunStatus = ({runId}: {runId: string | undefined}) => {
  const RUN_PATH = `/Run/Status/${runId}`;
  const runQuery = useQuery({
    queryKey: ["run-info", runId],
    queryFn: async () => {
      const url = `${RUN_PATH}`;
      const response = await apiFetch<RunStatus>(
        store.dispatch,
        store.getState,
        url,
        {
          method: "GET",
          headers: {
            "X-Skip-Loader": "true",
            "X-Skip-Toast": "true",
          },
        }
      );
      return response;
    },
    enabled: !!runId,
  });
  return runQuery;
};
