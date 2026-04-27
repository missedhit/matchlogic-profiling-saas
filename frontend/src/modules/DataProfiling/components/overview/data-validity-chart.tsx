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
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";
import { useEffect, useState, useMemo } from "react";
import { ChartConfig, ChartContainer } from "@/components/ui/chart";
import { calculateValidityCounts } from "../../utils/calculate-validity";

interface DataValidityChartProps {
  data: ColumnProfile;
  totalRecords: number;
  dataSourceId: string;
}

interface ValidityData {
  name: string;
  valid: number;
  invalid: number;
  validity: number;
}

const chartConfig = {
  valid: {
    label: "Valid",
    color: BRAND_PRIMARY,
  },
  total: {
    label: "Total",
    color: BRAND_PURPLE_LIGHT_BG,
  },
} satisfies ChartConfig;

export function DataValidityChart({
  data,
  totalRecords,
  dataSourceId,
}: DataValidityChartProps) {
  const [processedData, setProcessedData] = useState<ValidityData[]>([]);
  const validityCounts = useMemo(() => calculateValidityCounts(data), [data]);
  // No tilt — Brush scrollbar handles overflow for many columns

  useEffect(() => {
    const formattedData = Object.entries(data).map(([key, value]) => {
      const valid = validityCounts[key]?.valid || 0;
      const invalid = validityCounts[key]?.invalid || 0;
      const total = valid + invalid;
      const validity = total > 0 ? (valid / total) * 100 : 0;

      return {
        name: key,
        valid,
        invalid,
        validity,
      };
    });
    setProcessedData(formattedData);
  }, [data, validityCounts]);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">
          Data Validity vs. Invalidity
        </CardTitle>
        <CardDescription className="text-sm">
          Compare the valid vs invalid values across fields
        </CardDescription>
      </CardHeader>
      <CardContent className="h-[400px] hover-scrollbar-x custom-scrollbar">
        <ChartContainer
          config={chartConfig}
          className="min-h-[300px] h-full w-full min-w-[800px]"
        >
          <BarChart
            accessibilityLayer
            data={processedData}
            barSize={40}
            margin={{ top: 5, right: 20, left: 0, bottom: 50 }}
          >
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
            <YAxis
              domain={[0, 100]}
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              tick={{ fontSize: 12 }}
            />
            <Brush height={30} stroke={CHART_BRUSH_STROKE} />
            <Tooltip
              formatter={(value, name) => [`${Number(value).toFixed(2)}%`, name]}
            />
            <Bar
              dataKey="validity"
              fill="var(--color-valid)"
              radius={[4, 4, 0, 0]}
              background={{ fill: "var(--color-total)", radius: 4 }}
              name="Validity"
            />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
