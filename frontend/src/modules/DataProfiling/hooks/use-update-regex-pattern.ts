import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { RegexPattern } from "./use-regex-patterns-query";

export interface UpdateRegexPatternRequest {
  id: string;
  name: string;
  description: string;
  regexExpression: string;
  isDefault: boolean;
}

export interface UpdateRegexPatternResponse {
  value: RegexPattern & {
    isDeleted: boolean;
  };
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

export const useUpdateRegexPattern = () => {
  const queryClient = useQueryClient();
  const BASE_PATH = `/regex-patterns`;

  const mutation = useMutation({
    mutationFn: async (request: UpdateRegexPatternRequest) => {
      const url = `${BASE_PATH}`;
      const response = await apiFetch<UpdateRegexPatternResponse>(
        store.dispatch,
        store.getState,
        url,
        {
          method: "PUT",
          body: JSON.stringify(request),
          headers: {
            "Content-Type": "application/json",
          },
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
