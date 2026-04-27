"use client";
import { BRAND_PRIMARY, BRAND_PURPLE_MID } from "@/lib/brand-colors";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { ChartWrapper } from "@/modules/DataProfiling/components/chart-wrapper";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Maximize2, ShieldCheck } from "lucide-react";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";
import { useState, useEffect } from "react";

interface OutlierDetectionProps {
  data: ColumnProfile;
}

interface OutlierData {
    name: string;
    value: number;
}

export function OutlierDetection({ data }: OutlierDetectionProps) {
    const [processedData, setProcessedData] = useState<OutlierData[]>([]);
    const [modalOpen, setModalOpen] = useState(false);

    useEffect(() => {
        const formattedData = Object.entries(data)
            .map(([key, value]) => {
                const outliers: Array<{ value: string }> = value.outliers ?? [];
                const dist: Record<string, number> | null = value.valueDistribution ?? null;
                const count = outliers.reduce((sum, o) => {
                    if (dist && o.value != null && o.value in dist) {
                        return sum + (dist[o.value] ?? 1);
                    }
                    return sum + 1;
                }, 0);
                return { name: key, value: count };
            })
;
        setProcessedData(formattedData);
      }, [data]);

  const tooltipFormatter = (value: unknown) => [
    `${value} ${Number(value) === 1 ? "record" : "records"}`,
    "Anomalies",
  ];

  const cardData = processedData.filter((d) => d.value > 0);

  const chartContent = (angled: boolean, chartData: OutlierData[] = processedData) => (
    <LineChart
      data={chartData}
      margin={{
        top: 20,
        right: 30,
        left: 20,
        bottom: angled ? 80 : 5,
      }}
    >
      <CartesianGrid strokeDasharray="3 3" vertical={false} />
      <XAxis
        dataKey="name"
        interval={0}
        angle={angled ? -45 : 0}
        textAnchor={angled ? "end" : "middle"}
        tick={angled
          ? { fontSize: 11 }
          : ({ x, y, payload }: any) => {
              const label = payload.value.length > 6 ? payload.value.slice(0, 6) + "…" : payload.value;
              return (
                <text x={x} y={y + 10} textAnchor="middle" fontSize={10} fill="#71717a">
                  {label}
                </text>
              );
            }
        }
      />
      <YAxis tick={{ fontSize: 12 }} />
      <Tooltip formatter={tooltipFormatter} />
      <Line
        type="monotone"
        dataKey="value"
        stroke={BRAND_PRIMARY}
        strokeWidth={2}
        dot={{ r: 4, fill: BRAND_PRIMARY }}
        activeDot={{ r: 6, fill: BRAND_PURPLE_MID }}
      />
    </LineChart>
  );

  const hasAnomalies = cardData.length > 0;

  return (
    <>
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="text-base font-medium">
                Anomaly Detection
              </CardTitle>
              <CardDescription className="text-sm">
                Detect anomalies per column
              </CardDescription>
            </div>
            {hasAnomalies && (
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7 shrink-0"
                onClick={() => setModalOpen(true)}
                aria-label="Expand chart"
              >
                <Maximize2 className="h-4 w-4" />
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent className="h-[350px]">
          {hasAnomalies ? (
            <ChartWrapper height={300}>
              <ResponsiveContainer width="100%" height="100%">
                {chartContent(false, cardData)}
              </ResponsiveContainer>
            </ChartWrapper>
          ) : (
            <div className="flex h-full flex-col items-center justify-center gap-2">
              <ShieldCheck className="h-8 w-8 text-muted-foreground/40" />
              <p className="text-sm text-muted-foreground">No anomalies detected</p>
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={modalOpen} onOpenChange={setModalOpen}>
        <DialogContent className="!max-w-[80vw] w-[80vw]">
          <DialogHeader>
            <DialogTitle>Anomaly Detection</DialogTitle>
          </DialogHeader>
          <div className="h-[55vh]">
            <ChartWrapper height="100%">
              <ResponsiveContainer width="100%" height={400}>
                {chartContent(true)}
              </ResponsiveContainer>
            </ChartWrapper>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
