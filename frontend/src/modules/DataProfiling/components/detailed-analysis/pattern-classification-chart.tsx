'use client';
import {
  BRAND_PRIMARY,
  BRAND_PURPLE_MID,
  BRAND_PURPLE_MID_ALT,
  BRAND_PURPLE_BRIGHT,
  BRAND_PURPLE_TINT,
  BRAND_PURPLE_SOFT,
  BRAND_PURPLE_PALE,
} from '@/lib/brand-colors';
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from '@/components/ui/card';
import { ChartConfig, ChartContainer } from '@/components/ui/chart';
import { Skeleton } from '@/components/ui/skeleton';
import React from 'react';
import { ResponsiveContainer, Treemap, Tooltip } from 'recharts';
import { DataProfile } from '@/modules/DataProfiling/models/column-profile';

interface PatternClassificationChartProps {
  data: DataProfile;
}

const data = [
  { name: 'Missing First Names', size: 100 },
  { name: 'Missing Email Addresses', size: 80 },
  { name: 'Missing Dates of Birth', size: 50 },
  { name: 'Age Outliers', size: 20 },
  { name: 'Outlier Transaction Amounts', size: 20 },
  { name: 'High Uniqueness in User IDs', size: 10 },
  { name: 'Low Cardinality in Department', size: 5 },
  { name: 'Skewed City Distribution', size: 5 },
  { name: 'Invalid Phone Numbers', size: 10 },
];

const colors = [
  BRAND_PRIMARY,
  BRAND_PURPLE_MID,
  BRAND_PURPLE_MID_ALT,
  BRAND_PURPLE_BRIGHT,
  BRAND_PURPLE_TINT,
  BRAND_PURPLE_SOFT,
  BRAND_PURPLE_PALE,
];

const CustomTooltip = ({ active, payload }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white p-2 border rounded shadow-lg">
        <p className="font-bold">{`${payload[0].payload.name}`}</p>
        <p>{`${payload[0].value} records`}</p>
      </div>
    );
  }
  return null;
};

const chartConfig = {} satisfies ChartConfig;

export const PatternClassificationChart = ({
  data,
}: PatternClassificationChartProps) => {
  const [processedData, setProcessedData] = React.useState<any>([]);

  React.useEffect(() => {
    console.log('data', data);
    if (data.discoveredPatterns.length > 0) {
      const processedData = data.discoveredPatterns.map((pattern: any) => ({
        name: pattern.pattern,
        size: pattern.count,
      }));
      setProcessedData(processedData);
    } else if (data.pattern) {
      setProcessedData([
        {
          name: data.pattern,
          size: 1,
        },
      ]);
    } else {
      setProcessedData([
        {
          name: 'No patterns found',
          size: 1,
        },
      ]);
    }
  }, [data]);
  return (
    <Card className="col-span-1 lg:col-span-2">
      <CardHeader>
        <CardTitle>Pattern Classification</CardTitle>
        <CardDescription>
          Highlights common patterns, anomalies, or inconsistencies in the data
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="h-64 w-full">
          <Treemap
            data={processedData}
            dataKey="size"
            stroke="#fff"
            aspectRatio={4 / 3}
            content={<CustomizedContent colors={colors} />}
          >
            <Tooltip content={<CustomTooltip />} />
          </Treemap>
        </ChartContainer>
      </CardContent>
    </Card>
  );
};

const CustomizedContent = (props: any) => {
  const { root, depth, x, y, width, height, index, payload, rank, name } =
    props;

  // Estimate how many characters fit in the cell (approx 8px per char at fontSize 14)
  const maxChars = Math.max(0, Math.floor((width - 16) / 8));
  const displayName =
    name && name.length > maxChars ? name.slice(0, maxChars) + '…' : name;

  return (
    <g>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={colors[index % colors.length]}
        stroke="none"
      />
      {width > 30 && height > 30 && (
        <text
          x={x + width / 2}
          y={y + height / 2 + 7}
          fontWeight="100"
          textAnchor="middle"
          fill="#fff"
          stroke="none"
          fontSize={14}
        >
          {displayName}
          <title>{name}</title>
        </text>
      )}
    </g>
  );
};
