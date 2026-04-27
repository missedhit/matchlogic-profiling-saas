import React from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { DataProfile } from "@/modules/DataProfiling/models/column-profile";
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card";
import { FilledVsNullChart } from "./filled-vs-null-chart";
import { PatternClassificationChart } from "./pattern-classification-chart";
import { ValidityGraph } from "./validity-graph";
import { Button } from "@/components/ui/button";

interface GraphicalRepresentationButtonProps {
  data: DataProfile;
  dataSourceId: string;
  fieldName: string;
}

export const GraphicalRepresentationButton = ({
  data,
  dataSourceId,
  fieldName,
}: GraphicalRepresentationButtonProps) => {
  return (
    <Sheet>
      <SheetTrigger
        asChild
        className="fixed top-1/2 right-0 -translate-y-1/2 z-50 bg-primary text-primary-foreground px-1 py-4 rounded-r-lg flex items-center justify-center h-64 w-9 cursor-pointer"
      >
        <span className="[writing-mode:vertical-rl] rotate-180 whitespace-nowrap font-medium text-sm tracking-wider">
          Graphical Representation
        </span>
      </SheetTrigger>
      <SheetContent
        showCloseButton={false}
        className="sm:max-w-5xl w-full sm:w-3/4 lg:w-3/4 xl:w-3/4 p-0"
      >
        <SheetHeader>
          <SheetTitle className="text-2xl font-bold">
            Graphical Representation
          </SheetTitle>
        </SheetHeader>
        <div className="p-6 space-y-6 bg-white h-full overflow-y-auto">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <PatternClassificationChart data={data} />

            <ValidityGraph data={data} dataSourceId={dataSourceId} fieldName={fieldName} />

            <FilledVsNullChart data={data} />
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
};
