"use client";
import {
  BRAND_PRIMARY,
  BRAND_PURPLE_MID,
  BRAND_PURPLE_MID_ALT,
  BRAND_PURPLE_BRIGHT,
  BRAND_PURPLE_TINT,
  BRAND_PURPLE_SOFT,
  BRAND_PURPLE_PALE,
} from "@/lib/brand-colors";

import { Treemap, ResponsiveContainer, Tooltip } from "recharts";
import { ChartWrapper } from "@/modules/DataProfiling/components/chart-wrapper";
import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";

interface PatternClassificationProps {
  data: ColumnProfile;
}

const colors = [BRAND_PRIMARY, BRAND_PURPLE_MID, BRAND_PURPLE_MID_ALT, BRAND_PURPLE_BRIGHT, BRAND_PURPLE_TINT, BRAND_PURPLE_SOFT, BRAND_PURPLE_PALE];

export function PatternClassification({ data }: PatternClassificationProps) {
  const [processedData, setProcessedData] = useState<any[]>([]);
  const [selectedOption, setSelectedOption] = useState<string>("");
  // Process data on client side to ensure it's in the right format for Treemap
  useEffect(() => {
    if(!selectedOption || selectedOption === "") return;
    const column = data[selectedOption];
    const patterns = column?.discoveredPatterns;
    if (!patterns || patterns.length === 0) {
      setProcessedData([]);
      return;
    }
    const formattedData = [
      {
        name: "Pattern Classification",
        children: patterns.map((pattern) => ({
          name: pattern.pattern,
          size: pattern.count,
          value: pattern.count,
        })),
      },
    ];
    setProcessedData(formattedData);
  }, [selectedOption, data]);

  useEffect(() => {
    const keys = Object.keys(data);
    if (keys.length > 0) {
      setSelectedOption(keys[0]);
    }
  }, [data]);

  

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium flex items-center justify-between gap-2">
          <span>Pattern Classification</span>
          <Select value={selectedOption} onValueChange={setSelectedOption}>
            <SelectTrigger>
              <SelectValue placeholder="Select" />
            </SelectTrigger>
            <SelectContent className="max-h-[200px] overflow-y-auto">
              {Object.keys(data).map((key) => (
                <SelectItem key={key} value={key}>
                  {key}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </CardTitle>
        <CardDescription className="text-sm">
          Highlights common patterns, anomalies, or inconsistencies in the
          data
        </CardDescription>
      </CardHeader>
      <CardContent className="h-[350px]">
        <ChartWrapper height={300}>
          {processedData.length === 0 ? (
            <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
              No patterns identified
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%" >
              <Treemap
                data={processedData}
                dataKey="size"
                aspectRatio={4 / 3}
                stroke="#fff"
                fill={BRAND_PRIMARY}
                content={<CustomizedContent />}
              >
                <Tooltip
                  content={({ active, payload }) => {
                    if (active && payload && payload.length > 0) {
                      const data = payload[0].payload;
                      return (
                        <div className="bg-white p-2 border rounded shadow-sm">
                          <p className="font-medium">{data?.name || "Unknown"}</p>
                          <p className="text-sm">{data?.value ? `${data.value} records` : ""}</p>
                        </div>
                      );
                    }
                    return null;
                  }}
                />
              </Treemap>
            </ResponsiveContainer>
          )}
        </ChartWrapper>
      </CardContent>
    </Card>

  );
}

// Custom content renderer for treemap cells
const CustomizedContent = (props: any) => {
  const { x, y, width, height, index, payload, root, name } = props;

  if (!width || !height || width < 0 || height < 0) {
    return null;
  }

  // Get the actual data item from the children array
  if (!root || !root.children || !root.children[index]) {
    return null;
  }

  const item = root.children[index];

  // Estimate how many characters fit in the cell (approx 7px per char at text-xs)
  const maxChars = Math.max(0, Math.floor((width - 16) / 7));
  const displayName = name && name.length > maxChars ? name.slice(0, maxChars) + "…" : name;

  return (
    <g>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={colors[index % colors.length]}
        className="bg-profile"
      />
      {width > 30 && height > 30 && (
        <text
          x={x + width / 2}
          y={y + height / 2 - 5}
          textAnchor="middle"
          fill="#fff"
          stroke="none"
          className="font-manrope font-medium text-xs text-white"
        >
          {displayName}
          <title>{name}</title>
        </text>
      )}
    </g>
  );
};