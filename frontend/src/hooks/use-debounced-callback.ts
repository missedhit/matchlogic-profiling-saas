import { useCallback, useRef, useEffect } from "react";

/**
 * Returns a debounced version of the callback that delays invocation
 * until `delay` ms after the last call. Exposes `.flush()` to fire
 * immediately (e.g. on Enter key) and `.cancel()` to discard pending.
 */
export function useDebouncedCallback<T extends (...args: any[]) => void>(
  callback: T,
  delay: number
): ((...args: Parameters<T>) => void) & { flush: () => void; cancel: () => void } {
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const callbackRef = useRef(callback);
  const argsRef = useRef<any[]>([]);

  // Keep callback ref fresh
  callbackRef.current = callback;

  const cancel = useCallback(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  const flush = useCallback(() => {
    if (timerRef.current) {
      cancel();
      callbackRef.current(...argsRef.current);
    }
  }, [cancel]);

  const debounced = useCallback(
    (...args: any[]) => {
      argsRef.current = args;
      cancel();
      timerRef.current = setTimeout(() => {
        timerRef.current = null;
        callbackRef.current(...args);
      }, delay);
    },
    [delay, cancel]
  ) as ((...args: Parameters<T>) => void) & { flush: () => void; cancel: () => void };

  debounced.flush = flush;
  debounced.cancel = cancel;

  // Cleanup on unmount
  useEffect(() => cancel, [cancel]);

  return debounced;
}
