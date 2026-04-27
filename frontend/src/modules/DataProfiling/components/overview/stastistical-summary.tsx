"use client";
import { BRAND_PRIMARY } from "@/lib/brand-colors";

import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { ChartWrapper } from "@/modules/DataProfiling/components/chart-wrapper";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";

interface StatisticalSummaryProps {
  data: {
    field: string;
    min: number;
    q1: number;
    median: number;
    q3: number;
    max: number;
  }[];
}

export function StatisticalSummary({ data }: StatisticalSummaryProps) {
  // Transform data for box plot visualization
  const boxPlotData = data.flatMap((item, index) => {
    return [
      { x: index, y: item.min, z: 0, field: item.field },
      { x: index, y: item.q1, z: 1, field: item.field },
      { x: index, y: item.median, z: 2, field: item.field },
      { x: index, y: item.q3, z: 3, field: item.field },
      { x: index, y: item.max, z: 4, field: item.field },
    ];
  });

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">
          Statistical Summary
        </CardTitle>
        <CardDescription className="text-sm">
          Summarize numerical fields and data ranges
        </CardDescription>
      </CardHeader>
      <CardContent className="h-[350px]">
        <ChartWrapper height={300}>
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart
              margin={{
                top: 20,
                right: 20,
                bottom: 20,
                left: 20,
              }}
            >
              <CartesianGrid />
              <XAxis
                type="number"
                dataKey="x"
                name="field"
                tick={false}
                axisLine={false}
                tickLine={false}
                domain={[-0.5, data.length - 0.5]}
              />
              <YAxis type="number" dataKey="y" name="value" />
              <ZAxis type="number" dataKey="z" range={[60, 60]} />
              <Tooltip
                cursor={{ strokeDasharray: "3 3" }}
                formatter={(value, name, props) => {
                  if (!props || !props.payload) return [value, name];

                  const fieldIndex = Math.floor(props.payload.x);
                  const field = data[fieldIndex]?.field || "Unknown";
                  const zValue = props.payload.z;
                  let label = "";

                  switch (zValue) {
                    case 0:
                      label = "Min";
                      break;
                    case 1:
                      label = "Q1";
                      break;
                    case 2:
                      label = "Median";
                      break;
                    case 3:
                      label = "Q3";
                      break;
                    case 4:
                      label = "Max";
                      break;
                  }

                  return [`${value} (${label})`, field];
                }}
              />
              <Scatter
                data={boxPlotData}
                fill={BRAND_PRIMARY}
                line={{ stroke: BRAND_PRIMARY, strokeWidth: 1 }}
                lineType="fitting"
                shape={(props: { cx?: number; cy?: number; fill?: string }) => {
                  const { cx, cy, fill } = props;
                  return <circle cx={cx} cy={cy} r={4} fill={fill} />;
                }}
              />
            </ScatterChart>
          </ResponsiveContainer>
        </ChartWrapper>
      </CardContent>
    </Card>

  );
}
