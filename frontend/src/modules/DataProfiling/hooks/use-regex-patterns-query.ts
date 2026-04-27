import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useQuery } from "@tanstack/react-query";

export interface RegexPattern {
  id: string;
  name: string;
  description: string;
  regexExpression: string;
  isDefault: boolean;
  isSystem: boolean;
  isSystemDefault: boolean;
  version: number;
}

export interface RegexPatternsResponse {
  value: RegexPattern[];
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

export const useRegexPatternsQuery = () => {
  const BASE_PATH = `/regex-patterns`;

  const regexPatternsQuery = useQuery({
    queryKey: ["regex-patterns"],
    queryFn: async () => {
      const url = `${BASE_PATH}`;
      const response = await apiFetch<RegexPatternsResponse>(
        store.dispatch,
        store.getState,
        url,
        {
          method: "GET",
          headers: {
            "X-Skip-Toast": "true",
          },
        }
      );
      if (!Array.isArray(response?.value)) throw new Error("Failed to load regex patterns");
      return response;
    },
  });

  return regexPatternsQuery;
};
