"use client";

import { Progress } from "@/components/ui/progress";
import { useMemo } from "react";

interface DistinctValueTableProps {
  valueDistribution: { [key: string]: number };
  totalRecords: number;
}

interface DistinctValueRow {
  value: string;
  count: number;
  percentage: number;
}

function formatDisplayValue(value: string): { text: string; isSpecial: boolean } {
  if (value === "" || value.trim() === "") {
    return { text: "(empty)", isSpecial: true };
  }
  const lower = value.toLowerCase();
  if (lower === "null") {
    return { text: "(null)", isSpecial: true };
  }
  if (lower === "undefined") {
    return { text: "(undefined)", isSpecial: true };
  }
  return { text: value, isSpecial: false };
}

export function DistinctValueTable({
  valueDistribution,
  totalRecords,
}: DistinctValueTableProps) {
  const rows = useMemo<DistinctValueRow[]>(() => {
    return Object.entries(valueDistribution)
      .map(([value, count]) => ({
        value,
        count,
        percentage: totalRecords > 0 ? (count / totalRecords) * 100 : 0,
      }))
      .sort((a, b) => b.count - a.count);
  }, [valueDistribution, totalRecords]);

  if (rows.length === 0) {
    return (
      <div className="flex items-center justify-center h-32 text-sm text-muted-foreground">
        No distinct value data available.
      </div>
    );
  }

  return (
    <div className="border border-gray-200 rounded-md h-full flex flex-col">
      {/* Fixed header */}
      <div className="shrink-0 border-b bg-muted/50">
        <div className="grid grid-cols-[1fr_80px_1fr] px-3 py-2">
          <span className="text-xs font-medium text-muted-foreground">Value</span>
          <span className="text-xs font-medium text-muted-foreground text-right">Count</span>
          <span className="text-xs font-medium text-muted-foreground pl-3">Percentage</span>
        </div>
      </div>
      {/* Scrollable body */}
      <div className="flex-1 overflow-auto min-h-0">
        {rows.map((row, index) => {
          const display = formatDisplayValue(row.value);
          return (
            <div
              key={`${row.value}-${index}`}
              className="grid grid-cols-[1fr_80px_1fr] px-3 py-2 border-b border-gray-100 last:border-b-0 hover:bg-muted/20 transition-colors"
            >
              <span className="text-xs font-medium truncate pr-2" title={display.isSpecial ? undefined : display.text}>
                {display.isSpecial ? (
                  <span className="italic text-muted-foreground">{display.text}</span>
                ) : (
                  display.text
                )}
              </span>
              <span className="text-xs tabular-nums text-right">
                {row.count.toLocaleString()}
              </span>
              <div className="flex items-center gap-2 pl-3">
                <Progress
                  value={row.percentage}
                  className="h-1.5 flex-1 bg-gray-200"
                />
                <span className="text-xs text-muted-foreground tabular-nums w-12 text-right shrink-0">
                  {row.percentage.toFixed(1)}%
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
