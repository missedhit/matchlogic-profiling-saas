"use client";
import {
  BRAND_PRIMARY,
  BRAND_PURPLE_MID,
  BRAND_PURPLE_TINT,
  BRAND_PURPLE_SOFT,
  BRAND_PURPLE_PALE,
  CHART_BRUSH_STROKE,
} from "@/lib/brand-colors";

import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Legend,
  Tooltip,
} from "recharts";
import { ChartWrapper } from "@/modules/DataProfiling/components/chart-wrapper";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";
import { useEffect, useState } from "react";

interface DataTypeDistributionProps {
  data: ColumnProfile;
}

const typeColorMap: Record<string, string> = {
  "String": BRAND_PRIMARY,
  "DateTime": BRAND_PURPLE_MID,
  "Decimal": BRAND_PURPLE_TINT,
  "Integer": BRAND_PURPLE_SOFT,
  "Boolean": BRAND_PURPLE_PALE,

};

export function DataTypeDistribution({ data }: DataTypeDistributionProps) {
  const [formattedData, setFormattedData] = useState<{name: string, value: number, color: string}[]>([])

  useEffect(() => {
    const typesList = Object.values(data).reduce((acc, item) => {
      const dataType = item.typeDetectionResults?.[0]?.dataType;
      if (dataType) {
        acc[dataType] = (acc[dataType] || 0) + 1;
      }
      return  acc
    }, {} as Record<string, number>)
    const total = Object.values(typesList).reduce((acc, item) => acc + item, 0);
    const customData = Object.entries(typesList).map(([type, count]) => ({
      name: type,
      value: Math.round((count / total) * 100),
      color: typeColorMap[type],
    }))
    setFormattedData(customData);
  }, [data]);

  return (
  <Card>
    <CardHeader className="pb-2">
      <CardTitle className="text-base font-medium">
        Data Type Distribution
      </CardTitle>
      <CardDescription className="text-sm">
        Analysis of Data Type Distribution
      </CardDescription>
    </CardHeader>
    <CardContent className="h-[350px]">
      <ChartWrapper height={300}>
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={formattedData}
              outerRadius={120}
              stroke="none"
              fill={CHART_BRUSH_STROKE}
              dataKey="value"
              label={false}
            >
              {formattedData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
            <Tooltip
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const { name, value } = payload[0].payload;
                return (
                  <div className="rounded-md border bg-background px-3 py-1.5 shadow-md text-xs">
                    {name}: {value}%
                  </div>
                );
              }}
            />
            <Legend
              layout="horizontal"
              verticalAlign="bottom"
              align="center"
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
        </ResponsiveContainer>
      </ChartWrapper>
    </CardContent>
  </Card>

  );
}
