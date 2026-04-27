import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { RegexPattern } from "./use-regex-patterns-query";

export interface CreateRegexPatternRequest {
  name: string;
  description: string;
  regexExpression: string;
  isDefault: boolean;
}

export interface CreateRegexPatternResponse {
  value: RegexPattern;
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

export const useCreateRegexPattern = () => {
  const queryClient = useQueryClient();
  const BASE_PATH = `/regex-patterns`;

  const mutation = useMutation({
    mutationFn: async (request: CreateRegexPatternRequest) => {
      const url = `${BASE_PATH}`;
      const response = await apiFetch<CreateRegexPatternResponse>(
        store.dispatch,
        store.getState,
        url,
        {
          method: "POST",
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
