"use client";
import { BRAND_PURPLE_VIOLET, BRAND_PURPLE_VIOLET_LIGHT, CHART_BRUSH_STROKE } from "@/lib/brand-colors";
import React from "react";
import { PieChart, Pie, Cell, Legend, Tooltip } from "recharts";
import { ChartContainer } from "@/components/ui/chart";
import { ChartConfig } from "@/components/ui/chart";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { DataProfile } from "@/modules/DataProfiling/models/column-profile";
import { calculateColumnValidity } from "@/modules/DataProfiling/utils/calculate-validity";

interface ValidityGraphProps {
  data: DataProfile;
  dataSourceId: string;
  fieldName: string;
}

interface ValidityGraphData {
  name: string;
  value: number;
}

const COLORS = [BRAND_PURPLE_VIOLET, BRAND_PURPLE_VIOLET_LIGHT];

const chartConfig = {} satisfies ChartConfig;

export const ValidityGraph = ({ data, dataSourceId, fieldName }: ValidityGraphProps) => {
  const [processedData, setProcessedData] = React.useState<ValidityGraphData[]>([]);

  React.useEffect(() => {
    // Calculate validity counts directly from column data
    const { valid, invalid } = calculateColumnValidity(data);

    setProcessedData([
      { name: "Valid", value: valid },
      { name: "Invalid", value: invalid },
    ]);
  }, [data]);

  // Check if there's no data to display
  const hasData = processedData.some(item => item.value > 0);
  return (
    <Card>
      <CardHeader>
        <CardTitle>Validity Graph</CardTitle>
        <CardDescription>
          Displays the ratio of valid vs. invalid records.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {!hasData ? (
          <div className="flex h-64 w-full items-center justify-center text-muted-foreground">
            <div className="text-center">
              <p className="text-sm">No validity data available</p>
              <p className="text-xs mt-1">No records to analyze</p>
            </div>
          </div>
        ) : (
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
                {processedData.map((entry: ValidityGraphData, index: number) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={COLORS[index % COLORS.length]}
                  />
                ))}
              </Pie>
              <Tooltip
                formatter={(value) => {
                  const total = processedData.reduce((sum, item) => sum + item.value, 0);
                  const percentage = total > 0 ? Math.min(100, (Number(value) / total) * 100) : 0;
                  return [`${percentage.toFixed(2)}%`, "Percentage"];
                }}
              />
              <Legend iconType="circle" />
            </PieChart>
          </ChartContainer>
        )}
      </CardContent>
    </Card>
  );
};
