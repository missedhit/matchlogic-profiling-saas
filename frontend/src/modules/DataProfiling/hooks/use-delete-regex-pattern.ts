import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQueryClient } from "@tanstack/react-query";

export interface DeleteRegexPatternResponse {
  value: boolean;
  status: number;
  isSuccess: boolean;
  successMessage: string;
  correlationId: string;
  location: string;
  errors: string[];
  validationErrors: Array<{
    identifier: string;
    errorMessage: string;
    errorCode: string;
    severity: number;
  }>;
}

export const useDeleteRegexPattern = () => {
  const queryClient = useQueryClient();
  const BASE_PATH = `/regex-patterns`;

  const mutation = useMutation({
    mutationFn: async (id: string) => {
      const url = `${BASE_PATH}/${id}`;
      const response = await apiFetch<DeleteRegexPatternResponse>(
        store.dispatch,
        store.getState,
        url,
        {
          method: "DELETE",
        }
      );
      return response;
    },
    onSuccess: () => {
      // Invalidate and refetch the patterns list
      queryClient.invalidateQueries({ queryKey: ["regex-patterns"] });
    },
  });

  return mutation;
};
