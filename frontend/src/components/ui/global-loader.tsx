"use client";

import { useAppSelector } from "@/hooks/use-store";

export default function GlobalLoader() {
  const showLoader = useAppSelector((state) => state.uiState.showLoader);

  if (!showLoader) return null;

  return (
    <div
      aria-live="assertive"
      className="fixed inset-0 z-50 flex items-center justify-center"
    >
      <div className="flex flex-col items-center gap-3 rounded-2xl px-8 py-6">
        <div className="relative w-10 h-10">
          <div className="absolute inset-0 rounded-full border-[3px] border-primary/15" />
          <div className="absolute inset-0 rounded-full border-[3px] border-transparent border-t-primary animate-spin" />
        </div>
        <span className="text-sm font-medium text-gray-500 select-none tracking-wide">
          Loading…
        </span>
      </div>
    </div>
  );
}
