"use client";

import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { DatasetQuality } from "../../models/profile-result";
import { useEffect, useState } from "react";

interface FieldConsistencyProps {
  datasetQuality: DatasetQuality;
  title: string;
  description: string;
}

export function FieldConsistency({ datasetQuality, title, description }: FieldConsistencyProps) {
  const [data, setData] = useState<{ field: string; consistency: number }[]>([]);
  useEffect(() => {
    setData(Object.entries(datasetQuality?.columnScores || {}).map(([key, value]) => {
      return {
        field: key,
        consistency: value,
      };
    }));
  }, [datasetQuality]);
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
          {data.map((item, index) => (
            <div key={index} className="space-y-1">
              <div className="flex justify-between text-xs">
                <span>{item.field}</span>
                <span className="font-medium">{item.consistency}%</span>
              </div>
              <Progress
                value={item.consistency}
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
