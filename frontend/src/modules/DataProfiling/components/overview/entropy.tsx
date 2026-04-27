"use client";

import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { ColumnProfile } from "../../models/column-profile";
import { useEffect, useState } from "react";

interface EntropyProps {
  data: ColumnProfile;
  title: string;
  description: string;
}

interface EntropyData {
  field: string;
  entropy: number;
}

export function Entropy({ data, title, description }: EntropyProps) {
  const [processedData, setProcessedData] = useState<EntropyData[]>([]);
  useEffect(() => {
    setProcessedData(
      Object.entries(data).map(([key, value]) => ({
        field: key,
        entropy: value.qualityScore?.overallScore ?? 0,
      }))
    );
  }, [data]);
  return (
    <Card className="md:col-span-1">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">
            {title}
        </CardTitle>
        <CardDescription className="text-sm">
          {description}
        </CardDescription>
      </CardHeader>
      <CardContent className="h-[260px] hover-scrollbar custom-scrollbar">
        <div className="space-y-4">
          {processedData.map((item, index) => (
            <div key={index} className="space-y-1">
              <div className="flex justify-between text-xs">
                <span>{item.field}</span>
                <span className="font-medium">{item.entropy}%</span>
              </div>
              <Progress
                value={item.entropy}
                className="h-2"
                indicatorClassName="bg-primary"
              />
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
    
  );
}
