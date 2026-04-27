"use client";
import { BRAND_PRIMARY, BRAND_PURPLE_SOFT, BRAND_PURPLE_FAINT } from "@/lib/brand-colors";
import { PieChart, Pie, Cell, ResponsiveContainer } from "recharts";
import { ChartWrapper } from "@/modules/DataProfiling/components/chart-wrapper";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { DatasetQuality } from "../../models/profile-result";

interface DataQualityScoreProps {
  datasetQuality: DatasetQuality;
}

export function DataQualityScore({ datasetQuality }: DataQualityScoreProps) {
  const data = [
    { name: "Complete", value: datasetQuality.overallScore, color: BRAND_PRIMARY },
    { name: "Missing", value: 100 - datasetQuality.overallScore, color: BRAND_PURPLE_SOFT },
    { name: "Invalid", value: 0, color: BRAND_PURPLE_FAINT },
  ]
  return (
    <Card className="md:col-span-1">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">
          Data Quality Score
        </CardTitle>
        <CardDescription className="text-sm">
          Overall data completeness and quality
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartWrapper height={220}>
          <div className="flex flex-col items-center">
            <div className="relative h-[180px] w-[180px]">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={data}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={80}
                    paddingAngle={0}
                    dataKey="value"
                    startAngle={90}
                    endAngle={-270}
                  >
                    {data.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className="absolute inset-0 flex items-center justify-center flex-col">
                <div className="text-3xl font-bold text-primary">
                  {data[0].value}%
                </div>
              </div>
            </div>
          </div>
        </ChartWrapper>
      </CardContent>
    </Card>
    
  );
}
