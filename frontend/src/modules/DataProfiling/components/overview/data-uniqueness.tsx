"use client";
import { BRAND_PRIMARY, BRAND_PURPLE_LIGHT_BG, CHART_BRUSH_STROKE } from "@/lib/brand-colors";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Brush,
} from "recharts";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";
import { useEffect, useState } from "react";
import { ChartConfig, ChartContainer } from "@/components/ui/chart";

interface DataUniquenessProps {
  data: ColumnProfile;
  totalRecords: number;
}

interface UniquenessData {
  name: string;
  distinct: number;
  total: number;
}

const chartConfig = {
  distinct: {
    label: "Distinct",
    color: BRAND_PRIMARY,
  },
  total: {
    label: "Total",
    color: BRAND_PURPLE_LIGHT_BG,
  },
} satisfies ChartConfig

export function DataUniqueness({ data, totalRecords }: DataUniquenessProps) {
  const [processedData, setProcessedData] = useState<UniquenessData[]>([]);
  // No tilt — Brush scrollbar handles overflow for many columns

  useEffect(() => {
    const formattedData = Object.entries(data).map(([key, value]) => ({
      name: key,
      distinct: value.distinct,
      total: value.total,
    }));
    setProcessedData(formattedData);
  }, [data]);
  
  return (
    <Card>
    <CardHeader className="pb-4">
      <CardTitle className="text-base font-medium">Data Uniqueness and Distinct Count</CardTitle>
      <CardDescription>
        Display the number of distinct values per column.
      </CardDescription>
    </CardHeader>
    <CardContent className="h-[400px] hover-scrollbar-x custom-scrollbar">
      <ChartContainer config={chartConfig} className="min-h-[300px] h-full w-full min-w-[800px]">
        <BarChart accessibilityLayer data={processedData} barSize={40} margin={{ top: 5, right: 20, left: 0, bottom: 50 }}>
          <CartesianGrid vertical={false} />
          <XAxis
            dataKey="name"
            tickLine={false}
            axisLine={false}
            tickMargin={8}
            interval={0}
            tick={({ x, y, payload }: any) => {
              const label = payload.value.length > 6 ? payload.value.slice(0, 6) + "…" : payload.value;
              return (
                <text x={x} y={y + 10} textAnchor="middle" fontSize={10} fill="#71717a">
                  {label}
                </text>
              );
            }}
          />
          <YAxis domain={[0, totalRecords]} tickLine={false} axisLine={false} tickMargin={8} tick={{ fontSize: 12 }} />
          <Brush height={30} stroke={CHART_BRUSH_STROKE} />
          <Tooltip />
          <Bar 
            dataKey="distinct"
            name="Distinct Count"
            fill="var(--color-distinct)" 
            radius={[4, 4, 0, 0]}
            background={{ fill: "var(--color-total)", radius: 4 }}
          />
        </BarChart>
      </ChartContainer>
    </CardContent>
  </Card>
  );
}
