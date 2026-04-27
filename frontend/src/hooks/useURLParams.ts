import { useEffect, useCallback, useMemo, useRef, useLayoutEffect } from "react";
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import { useAppDispatch, useAppSelector } from "./use-store";
import {
  setURLParams,
  syncURLParams,
  setInitialized,
  updateSingleParam,
  selectURLParams,
  selectIsURLParamsInitialized,
  type URLParams,
  type RouteConfig,
} from "@/store/urlParamsSlice";

interface UseURLParamsOptions {
  routeConfig?: RouteConfig;
  debounceMs?: number;
  autoSync?: boolean;
}

export function useURLParams(options: UseURLParamsOptions = {}) {
  const { routeConfig, debounceMs = 300, autoSync = true } = options;
  
  const dispatch = useAppDispatch();
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  
  const urlParams = useAppSelector(selectURLParams);
  const isInitialized = useAppSelector(selectIsURLParamsInitialized);

  
  // Store routeConfig in a ref to avoid dependency issues
  const routeConfigRef = useRef(routeConfig);
  routeConfigRef.current = routeConfig;

  // Add a syncing flag to prevent infinite loops
  const isSyncingRef = useRef(false);
  
  // Track previous URL params to detect actual URL changes
  const prevURLParamsRef = useRef<string>("");

  // Convert URLSearchParams to our URLParams type
  const currentURLParams: URLParams = useMemo(() => {
    const params: URLParams = {};
    const allowedKeys = ["projectId", "workflowId", "runId", "page", "tab"];
    
    searchParams.forEach((value, key) => {
      if (allowedKeys.includes(key) && value) {
        (params as any)[key] = value;
      }
    });
    
    return params;
  }, [searchParams]);

  // Create a stable string representation for comparison
  const currentParamsString = useMemo(() => {
    return JSON.stringify(currentURLParams);
  }, [currentURLParams]);


  // Synchronous initialization using layoutEffect for immediate processing
  useLayoutEffect(() => {
    if (!isInitialized && Object.keys(currentURLParams).length > 0) {
      dispatch(setURLParams(currentURLParams));
      dispatch(setInitialized(true));
      prevURLParamsRef.current = currentParamsString;
      
      if (autoSync && routeConfigRef.current) {
        dispatch(syncURLParams({ params: currentURLParams, routeConfig: routeConfigRef.current }));
      }
    }
  }, [currentURLParams, isInitialized, currentParamsString, autoSync, dispatch]);

  // Update URL when store params change
  const updateURL = useCallback(
    (params: URLParams, replace = false) => {
      const newSearchParams = new URLSearchParams(searchParams.toString());
      
      // Update or remove parameters
      Object.entries(params).forEach(([key, value]) => {
        if (value === undefined || value === null || value === "") {
          newSearchParams.delete(key);
        } else {
          newSearchParams.set(key, value);
        }
      });

      const newURL = `${pathname}${
        newSearchParams.toString() ? `?${newSearchParams.toString()}` : ""
      }`;
      
      if (replace) {
        router.replace(newURL);
      } else {
        router.push(newURL);
      }
    },
    [router, pathname, searchParams]
  );

    // Only sync when URL actually changes (not when store changes)
  useEffect(() => {
    if (!isInitialized || isSyncingRef.current) return;

    // Check if URL params actually changed from previous state
    const urlActuallyChanged = currentParamsString !== prevURLParamsRef.current;

    if (urlActuallyChanged) {
      // Update the previous state reference
      prevURLParamsRef.current = currentParamsString;
      
      isSyncingRef.current = true;
      dispatch(setURLParams(currentURLParams));
      
      if (autoSync && routeConfigRef.current) {
        dispatch(syncURLParams({ params: currentURLParams, routeConfig: routeConfigRef.current }))
          .finally(() => {
            setTimeout(() => {
              isSyncingRef.current = false;
            }, 100);
          });
      } else {
        isSyncingRef.current = false;
      }
    }
  }, [dispatch, isInitialized, autoSync, currentParamsString]);

  // Debounced URL update function
  const debouncedUpdateURL = useCallback(
    (() => {
      let timeoutId: NodeJS.Timeout;
      return (params: URLParams, replace = false) => {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(() => {
          updateURL(params, replace);
        }, debounceMs);
      };
    })(),
    [updateURL, debounceMs]
  );

  // Public API methods
  const setParam = useCallback(
    (key: keyof URLParams, value: string | undefined, updateURL = true) => {
      dispatch(updateSingleParam({ key, value }));
      
      if (updateURL) {
        const newParams = { ...urlParams, [key]: value };
        debouncedUpdateURL(newParams, true);
      }
    },
    [dispatch, urlParams, debouncedUpdateURL]
  );

  const setParams = useCallback(
    (params: Partial<URLParams>, updateURL = true) => {
      dispatch(setURLParams(params as URLParams));
      
      if (updateURL) {
        const newParams = { ...urlParams, ...params };
        debouncedUpdateURL(newParams, true);
      }
    },
    [dispatch, urlParams, debouncedUpdateURL]
  );

  const getParam = useCallback(
    (key: keyof URLParams): string | undefined => {
      return urlParams[key];
    },
    [urlParams]
  );

  const hasParam = useCallback(
    (key: keyof URLParams): boolean => {
      return urlParams[key] !== undefined && urlParams[key] !== null && urlParams[key] !== "";
    },
    [urlParams]
  );

  const removeParam = useCallback(
    (key: keyof URLParams) => {
      setParam(key, undefined);
    },
    [setParam]
  );

  const manualSync = useCallback(
    () => {
      if (routeConfigRef.current) {
        dispatch(syncURLParams({ params: urlParams, routeConfig: routeConfigRef.current }));
      }
    },
    [dispatch, urlParams]
  );

  return {
    // Current URL parameters
    params: urlParams,
    
    // Parameter manipulation
    setParam,
    setParams,
    getParam,
    hasParam,
    removeParam,
    
    // Manual sync trigger
    sync: manualSync,
    
    // State
    isInitialized,
    
    // Raw URL params for debugging
    rawURLParams: currentURLParams,
  };
}