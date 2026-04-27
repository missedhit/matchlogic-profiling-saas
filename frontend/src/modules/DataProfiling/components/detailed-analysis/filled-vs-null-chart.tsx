"use client";
import { BRAND_PURPLE_DEEP, BRAND_PURPLE_VIOLET_MEDIUM, CHART_BRUSH_STROKE } from "@/lib/brand-colors";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { ChartConfig, ChartContainer } from "@/components/ui/chart";
import React from "react";
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Legend,
  Tooltip,
} from "recharts";
import { DataProfile } from "../../models/column-profile";

interface FilledVsNullChartProps {
  data: DataProfile;
}

interface FilledVsNullChartData {
  name: string;
  value: number;
}

const COLORS = [BRAND_PURPLE_DEEP, BRAND_PURPLE_VIOLET_MEDIUM];

const chartConfig = {} satisfies ChartConfig;

function FilledNullTooltip({ active, payload }: { active?: boolean; payload?: { name: string; value: number; payload: { color?: string } }[] }) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="rounded-md border bg-background px-3 py-2 shadow-md text-xs">
      {payload.map((entry) => (
        <div key={entry.name}>
          {entry.name}: {entry.value} records
        </div>
      ))}
    </div>
  );
}

export const FilledVsNullChart = ({ data }: FilledVsNullChartProps) => {
  const [processedData, setProcessedData] = React.useState<FilledVsNullChartData[]>([]);

  React.useEffect(() => {
    setProcessedData([
      { name: "Filled", value: data.filled },
      { name: "Null", value: data.null },
    ]);
  }, [data]);
  return (
    <Card>
      <CardHeader>
        <CardTitle>Filled vs Null</CardTitle>
        <CardDescription>
          Shows the total filled and null record count.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="h-64 w-full">
          <PieChart>
            <Pie
              className="focus:outline-none"
              data={processedData}
              stroke="none"
              cx="50%"
              cy="50%"
              fill={CHART_BRUSH_STROKE}
              startAngle={90}
              endAngle={-270}
              dataKey="value"
            >
              {processedData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={COLORS[index % COLORS.length]}
                />
              ))}
            </Pie>
            <Tooltip content={<FilledNullTooltip />} />
            <Legend
              iconType="circle"
              formatter={(value, entry, index) => {
                return (
                  <span className="text-xs text-foreground">
                    {value}
                  </span>
                );
              }}
            />
          </PieChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
};
