"use client";

import { useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from "recharts";
import { ChartConfig, ChartContainer } from "@/components/ui/chart";
import { BRAND_PRIMARY, BRAND_PURPLE_LIGHT_BG } from "@/lib/brand-colors";

interface DistinctValueChartProps {
  valueDistribution: { [key: string]: number };
  totalRecords: number;
}

interface ChartDataItem {
  name: string;
  fullName: string;
  count: number;
  percentage: number;
}

const chartConfig = {
  count: {
    label: "Count",
    color: BRAND_PRIMARY,
  },
} satisfies ChartConfig;

function formatLabel(value: string): string {
  if (value === "" || value.trim() === "") return "(empty)";
  if (value.toLowerCase() === "null") return "(null)";
  if (value.toLowerCase() === "undefined") return "(undefined)";
  return value;
}

function truncateLabel(value: string, maxLen: number = 12): string {
  const formatted = formatLabel(value);
  if (formatted.length > maxLen) {
    return formatted.slice(0, maxLen) + "\u2026";
  }
  return formatted;
}

function CustomTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: { payload: ChartDataItem }[];
}) {
  if (!active || !payload || payload.length === 0) return null;
  const item = payload[0].payload;
  return (
    <div className="rounded-md border bg-background px-3 py-2 shadow-md text-xs">
      <div className="font-medium mb-1">{formatLabel(item.fullName)}</div>
      <div className="text-muted-foreground">
        Count: {item.count.toLocaleString()}
      </div>
      <div className="text-muted-foreground">
        Percentage: {item.percentage.toFixed(1)}%
      </div>
    </div>
  );
}

export function DistinctValueChart({
  valueDistribution,
  totalRecords,
}: DistinctValueChartProps) {
  const chartData = useMemo<ChartDataItem[]>(() => {
    return Object.entries(valueDistribution)
      .map(([value, count]) => ({
        name: truncateLabel(value),
        fullName: value,
        count,
        percentage: totalRecords > 0 ? (count / totalRecords) * 100 : 0,
      }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 8);
  }, [valueDistribution, totalRecords]);

  if (chartData.length === 0) {
    return (
      <div className="flex items-center justify-center h-32 text-sm text-muted-foreground">
        No distinct value data available for chart.
      </div>
    );
  }

  return (
    <div className="w-full min-h-[300px] h-full">
      <ChartContainer config={chartConfig} className="h-full w-full min-h-[300px]">
        <BarChart
          accessibilityLayer
          data={chartData}
          barSize={48}
          margin={{ top: 10, right: 20, left: 0, bottom: 50 }}
        >
          <CartesianGrid vertical={false} />
          <XAxis
            dataKey="name"
            tickLine={false}
            axisLine={false}
            tickMargin={8}
            interval={0}
            tick={({ x, y, payload }: any) => (
              <text
                x={x}
                y={y + 10}
                textAnchor="middle"
                fontSize={10}
                fill="#71717a"
              >
                {payload.value}
              </text>
            )}
          />
          <YAxis
            tickLine={false}
            axisLine={false}
            tickMargin={8}
            tick={{ fontSize: 12 }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Bar
            dataKey="count"
            fill="var(--color-count)"
            radius={[4, 4, 0, 0]}
            name="Count"
          />
        </BarChart>
      </ChartContainer>
    </div>
  );
}
