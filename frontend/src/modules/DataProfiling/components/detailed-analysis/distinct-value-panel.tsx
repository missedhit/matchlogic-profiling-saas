"use client";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { DistinctValueTable } from "./distinct-value-table";
import { DistinctValueChart } from "./distinct-value-chart";
import { BarChart3 } from "lucide-react";

interface DistinctValuePanelProps {
  valueDistribution: { [key: string]: number };
  totalRecords: number;
  fieldName: string;
}

export function DistinctValuePanel({
  valueDistribution,
  totalRecords,
  fieldName,
}: DistinctValuePanelProps) {
  const distinctCount = Object.keys(valueDistribution).length;

  if (distinctCount === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-center px-4 py-12">
        <div className="h-12 w-12 rounded-full bg-muted flex items-center justify-center mb-4">
          <BarChart3 className="h-6 w-6 text-muted-foreground" />
        </div>
        <h3 className="text-sm font-semibold mb-1">
          Distinct value distribution not available
        </h3>
        <p className="text-xs text-muted-foreground max-w-sm">
          The value distribution for <span className="font-medium">{fieldName}</span> could not be generated. This typically happens with large datasets where the number of unique values exceeds the processing threshold.
        </p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between mt-3 mb-3">
        <div className="flex items-center gap-2 text-sm">
          <span className="text-muted-foreground">Distinct values for</span>
          <span className="font-semibold text-foreground">{fieldName}</span>
          <span className="text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded-full">
            {distinctCount.toLocaleString()} value{distinctCount !== 1 ? "s" : ""}
          </span>
        </div>
      </div>

      <Tabs defaultValue="table" className="flex flex-col flex-1 min-h-0">
        <TabsList className="bg-transparent border-b rounded-none w-full justify-start gap-0 h-auto p-0 shrink-0">
          <TabsTrigger
            value="table"
            className="bg-transparent text-muted-foreground border-transparent rounded-none text-xs font-semibold border-b-2 transition-colors duration-200 data-[state=active]:text-primary data-[state=active]:border-b-primary data-[state=active]:bg-transparent data-[state=active]:shadow-none px-4 py-2"
          >
            Stats Detail
          </TabsTrigger>
          <TabsTrigger
            value="chart"
            className="bg-transparent text-muted-foreground border-transparent rounded-none text-xs font-semibold border-b-2 transition-colors duration-200 data-[state=active]:text-primary data-[state=active]:border-b-primary data-[state=active]:bg-transparent data-[state=active]:shadow-none px-4 py-2"
          >
            Stats Detail (Graphical)
          </TabsTrigger>
        </TabsList>
        <div className="flex-1 min-h-0 mt-3">
          <TabsContent value="table" className="m-0 h-full">
            <DistinctValueTable
              valueDistribution={valueDistribution}
              totalRecords={totalRecords}
            />
          </TabsContent>
          <TabsContent value="chart" className="m-0 h-full overflow-auto">
            <DistinctValueChart
              valueDistribution={valueDistribution}
              totalRecords={totalRecords}
            />
          </TabsContent>
        </div>
      </Tabs>
    </div>
  );
}
