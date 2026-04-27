import { useState, useEffect } from "react";
import { cn } from "@/lib/utils";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface Pattern {
  pattern: string;
  count: number;
  matchPercentage: number;
  description?: string;
  validCount?: number;
  invalidCount?: number;
  percentNotNull?: number;
  percentAll?: number;
}

interface PatternListContainerProps {
  patterns: Pattern[];
  fieldName: string;
  onPatternSelect: (pattern: Pattern) => void;
  selectedPattern?: Pattern | null;
  onPatternCellFilter?: (pattern: Pattern, columnName: string) => void;
  columnData?: any; // To access patternMatchRowDocumentIds and other data
  dataSourceId: string; // Add dataSourceId to fetch actual counts
}

const PatternBar = ({
  label,
  value,
  total,
  color,
}: {
  label: string;
  value: number;
  total: number;
  color: string;
}) => {
  const percentage = total > 0 ? (value / total) * 100 : 0;

  return (
    <Tooltip>
      <TooltipTrigger>
        <div className="w-20 h-2 bg-gray-200 rounded-full">
          <div
            className={`h-full rounded-full ${color}`}
            style={{ width: `${percentage}%` }}
          />
        </div>
      </TooltipTrigger>
      <TooltipContent>
        <span className="text-xs">
          {label}: {value} ({percentage.toFixed(1)}%)
        </span>
      </TooltipContent>
    </Tooltip>
  );
};

const columns = [
  { name: "pattern", label: "Pattern" },
  { name: "description", label: "Description" },
  { name: "validCount", label: "Valid Count" },
  { name: "invalidCount", label: "Invalid Count" },
  { name: "percentNotNull", label: "Percent (Not Null)" },
  { name: "percentAll", label: "Percent (All)" },
];

export function PatternListContainer({
  patterns,
  fieldName,
  onPatternSelect,
  selectedPattern,
  onPatternCellFilter,
  columnData,
  dataSourceId,
}: PatternListContainerProps) {
  const [selectedCell, setSelectedCell] = useState<{
    row: string;
    col: string;
  } | null>(null);

  // Clear selected cell when pattern is deselected
  useEffect(() => {
    if (!selectedPattern) {
      setSelectedCell(null);
    }
  }, [selectedPattern]);

  const filteredPatterns = patterns.filter(
    (pattern) => pattern.pattern !== "Unclassified" && pattern.count > 0
  );

  // Helper function to get pattern counts directly from pattern data
  const getPatternCounts = (pattern: Pattern) => {
    // Get the field data
    const fieldData = columnData?.[fieldName];
    if (!fieldData) {
      return {
        valid: pattern.count,
        invalid: 0,
        nullCount: 0,
        total: pattern.count,
        filled: pattern.count,
        fieldTotal: pattern.count,
      };
    }

    // For each pattern, the valid count is the pattern's own count
    // Invalid count is calculated based on whether this is the identified pattern
    const identifiedPattern = fieldData.pattern;
    const isIdentifiedPattern = pattern.pattern === identifiedPattern;

    const validCount = pattern.count;
    // For the identified pattern, invalid = total - pattern count
    // For other patterns, we don't show invalid (they are just variants)
    const invalidCount = isIdentifiedPattern
      ? Math.max(0, fieldData.total - pattern.count)
      : 0;

    // Get field-level data
    const fieldTotalCount = fieldData.total || 0;
    const fieldFilledCount = fieldData.filled || 0;
    const nullCount = fieldData.null || 0;

    return {
      valid: validCount,
      invalid: invalidCount,
      nullCount: nullCount,
      total: validCount + invalidCount,
      filled: fieldFilledCount,
      fieldTotal: fieldTotalCount,
    };
  };

  if (filteredPatterns.length === 0) {
    return (
      <div className="p-4 text-center text-gray-500 border border-gray-200 rounded-md">
        No patterns available for {fieldName}
      </div>
    );
  }

  const handleCellClick = (
    pattern: Pattern,
    columnName: string,
    event: React.MouseEvent
  ) => {
    event.stopPropagation();

    // Handle specific column clicks
    if (columnName === "pattern" || columnName === "description") {
      // For pattern name and description, handle selection/deselection
      // Clear selected cell when changing pattern selection
      setSelectedCell(null);
      onPatternSelect(pattern);
    } else if (columnName === "validCount" || columnName === "invalidCount") {
      // For valid/invalid counts, handle filtering and set selected cell
      setSelectedCell({ row: pattern.pattern, col: columnName });
      onPatternCellFilter?.(pattern, columnName);
    } else {
      // For other columns, set selected cell and select the pattern
      setSelectedCell({ row: pattern.pattern, col: columnName });
      onPatternSelect(pattern);
    }
  };

  const getCellClassName = (pattern: Pattern, columnName: string) => {
    const isSelected =
      selectedCell?.row === pattern.pattern && selectedCell?.col === columnName;
    return cn("cursor-pointer hover:bg-gray-50", {
      "bg-primary/20 rounded": isSelected,
    });
  };

  const getCellStyle = (_pattern: Pattern, _columnName: string) => {
    return {};
  };

  // Calculate dynamic height based on content - smaller row height for pattern table
  const calculateHeight = () => {
    const headerHeight = 42; // Header height
    const rowHeight = 45; // Reduced row height for more compact display
    const contentHeight = headerHeight + filteredPatterns.length * rowHeight;
    const maxHeight = 300;
    return Math.min(maxHeight, contentHeight);
  };

  return (
    <Table
      parentClassName="border border-gray-200"
      style={{ height: `${calculateHeight()}px` }}
    >
      <TableHeader>
        <TableRow className="[&_th]:font-medium [&_th]:border-r [&_th]:border-b [&_th]:border-gray-200 [&_th]:py-2 [&_th]:px-5">
          {columns.map((column) => (
            <TableHead key={column.name}>{column.label}</TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody className="text-xs">
        {filteredPatterns.map((pattern, index) => (
          <TableRow
            key={`${pattern.pattern}-${index}`}
            className={cn(
              "[&_td]:border [&_td]:border-gray-200 [&_td]:py-2 [&_td]:px-5",
              {
                "bg-primary/10": selectedPattern?.pattern === pattern.pattern,
              }
            )}
          >
            <TableCell
              className={cn(
                "font-medium",
                getCellClassName(pattern, "pattern")
              )}
              style={getCellStyle(pattern, "pattern")}
              onClick={(e) => handleCellClick(pattern, "pattern", e)}
            >
              {pattern.pattern}
            </TableCell>
            <TableCell
              className={getCellClassName(pattern, "description")}
              style={getCellStyle(pattern, "description")}
              onClick={(e) => handleCellClick(pattern, "description", e)}
            >
              {pattern.description || pattern.pattern}
            </TableCell>
            <TableCell
              className={getCellClassName(pattern, "validCount")}
              style={getCellStyle(pattern, "validCount")}
              onClick={(e) => handleCellClick(pattern, "validCount", e)}
            >
              <span className="text-xs font-medium">
                {getPatternCounts(pattern).valid}
              </span>
            </TableCell>
            <TableCell
              className={getCellClassName(pattern, "invalidCount")}
              style={getCellStyle(pattern, "invalidCount")}
              onClick={(e) => handleCellClick(pattern, "invalidCount", e)}
            >
              <span className="text-xs font-medium">
                {getPatternCounts(pattern).invalid}
              </span>
            </TableCell>
            <TableCell>
              <div className="flex items-center justify-between">
                <PatternBar
                  label="Valid/Filled"
                  value={getPatternCounts(pattern).valid}
                  total={getPatternCounts(pattern).filled}
                  color="bg-primary"
                />
              </div>
            </TableCell>
            <TableCell>
              <div className="flex items-center justify-between">
                <PatternBar
                  label="Valid/Total"
                  value={getPatternCounts(pattern).valid}
                  total={getPatternCounts(pattern).fieldTotal}
                  color="bg-primary"
                />
              </div>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
