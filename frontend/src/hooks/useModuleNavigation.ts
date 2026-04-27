import { useRouter } from "next/navigation";
import { useCallback } from 'react';
import { useSelector } from 'react-redux';

// Define types for the selector functions
type SelectorFunction<T = any> = (state: any) => T;
type ParamSelectors = Record<string, SelectorFunction>;

export const useModuleNavigation = (paramSelectors: ParamSelectors = {}) => {
  const router = useRouter();

  // Collect URL params from Redux state based on selectors provided
  const params = useSelector((state) => {
    const entries = Object.entries(paramSelectors).map(
      ([key, selector]: [string, SelectorFunction]) => [key, selector(state)]
    );
    return Object.fromEntries(entries);
  });

  // Navigate function to a given path with dynamic params
  const navigate = useCallback((path: string, additionalParams = {}) => {
    const combinedParams = { ...params, ...additionalParams };
    const queryString = new URLSearchParams(combinedParams).toString();

    router.push(`${path}?${queryString}`);
  }, [router, params]);

  // Function to generate links (useful for <Link> component)
  const generateLink = useCallback((path: string, additionalParams = {}) => {
    const combinedParams = { ...params, ...additionalParams };
    const queryString = new URLSearchParams(combinedParams).toString();

    return `${path}?${queryString}`;
  }, [params]);

  return { navigate, generateLink, params };
};
