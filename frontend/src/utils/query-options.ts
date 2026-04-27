/**
 * Reusable React Query option presets.
 *
 * Usage — no-cache query (always fetches fresh from the server):
 *
 *   useQuery({
 *     queryKey: [...],
 *     queryFn: async () => {
 *       return apiFetch(dispatch, getState, url, {
 *         method: "GET",
 *         headers: noCacheHeaders(),
 *       });
 *     },
 *     ...NO_CACHE_QUERY_OPTIONS,
 *   });
 *
 * `noCacheHeaders()` adds `Cache-Control: no-cache` + `Pragma: no-cache` so
 * the browser's HTTP cache is also bypassed.
 *
 * `NO_CACHE_QUERY_OPTIONS` sets staleTime=0 and gcTime=0 so React Query
 * never serves a cached value from its in-memory store.
 */

/** React Query options that disable all in-memory caching. */
export const NO_CACHE_QUERY_OPTIONS = {
  staleTime: 0,
  gcTime: 0,
} as const;

/**
 * Returns request headers that instruct both the browser and any
 * intermediate HTTP cache to skip cached responses.
 *
 * Pass the result to `apiFetch` options as `headers`:
 *   `headers: noCacheHeaders()`
 * or merge with existing headers:
 *   `headers: { ...noCacheHeaders(), "X-Skip-Loader": "true" }`
 */
export function noCacheHeaders(): Record<string, string> {
  return {
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
  };
}
