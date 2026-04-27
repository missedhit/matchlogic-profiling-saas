import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";
import { AdvanceAnalytics } from "@/modules/DataProfiling/models/advance-analytics";

export const useAdvanceAnalyticsQuery = ({
  dataSourceId,
}: {
  dataSourceId: string | undefined;
}) => {
  const BASE_PATH = `/DataProfile/AdvanceAnalytics?dataSourceId=${dataSourceId}`;
  const advanceAnalyticsQuery = useQuery({
    queryKey: ["advance-analytics", dataSourceId],
    queryFn: async () => {
      const url = `${BASE_PATH}`;
      const response = await apiFetch<AdvanceAnalytics>(
        store.dispatch,
        store.getState,
        url,
        {
          method: "GET",
        }
      );
      if (!response || !("value" in response)) {
        throw new Error("Profiling data not available");
      }
      return response;
    },
    enabled: !!dataSourceId,
  });
  return advanceAnalyticsQuery;
};
