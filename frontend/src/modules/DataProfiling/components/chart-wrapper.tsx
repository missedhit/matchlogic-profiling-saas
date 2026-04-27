"use client";

import type React from "react";

import { useEffect, useState } from "react";
import { Skeleton } from "@/components/ui/skeleton";

interface ChartWrapperProps {
  children: React.ReactNode;
  height?: string | number;
}

export function ChartWrapper({
  children,
  height = "300px",
}: ChartWrapperProps) {
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);
  }, []);

  if (!isMounted) {
    return (
      <div
        style={{ height }}
        className="w-full flex items-center justify-center"
      >
        <Skeleton className="h-[90%] w-[90%] rounded-md" />
      </div>
    );
  }

  return (
    <div style={{ height }} className="w-full [&_.recharts-wrapper]:!outline-none [&_.recharts-surface]:!outline-none [&_.recharts-sector]:!outline-none [&_.recharts-layer]:!outline-none">
      {children}
    </div>
  );
}
