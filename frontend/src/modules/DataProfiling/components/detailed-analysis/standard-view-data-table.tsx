import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { useDataSourceDataQuery } from "@/modules/DataProfiling/hooks/use-data-source-data-query";
import { useEffect, useState } from "react";
import {
  CellFilterInfo,
  CHARACTER_COMPOSITION_FILTERS,
  VALIDITY_FILTERS,
  FILLED_NULL_FILTERS,
} from "@/modules/DataProfiling/models/cell-filteration";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Loader2, Filter } from "lucide-react";
import { GeneralCloseIcon } from "@/assets/icons";

interface StandardViewDataTableProps {
  highlightedColumn: string;
  dataSourceId: string;
  headers: string[];
  selectedCharacteristic: string;
  activeFilter?: CellFilterInfo | null;
  onFilterApply?: (documentId: string) => void;
  onFilterClear?: () => void;
  columnData?: any;
  directData?: Array<{
    rowData: Record<string, any>;
    rowNumber: number;
  }> | null;
}

const FilterEmptyState = ({ filterType }: { filterType?: string }) => {
  const getFilterTypeName = () => {
    switch (filterType) {
      case "validity":
        return "validity";
      case "characterComposition":
        return "character composition";
      case "filledOrNull":
        return "filled/null";
      case "anomalies":
        return "anomalies";
      default:
        return "filter";
    }
  };

  return (
    <div className="flex flex-col items-center justify-center p-12 text-center text-gray-500 bg-gray-50 rounded-lg border-2 border-dashed border-gray-200">
      <Filter className="w-12 h-12 mb-4 text-gray-400" />
      <h3 className="text-lg font-medium text-gray-700 mb-2">
        No Filter Selected
      </h3>
      <p className="text-sm text-gray-500">
        Please select a {getFilterTypeName()} filter from the dropdown above to
        view the filtered data.
      </p>
    </div>
  );
};

export function StandardViewDataTable({
  highlightedColumn,
  dataSourceId,
  headers,
  selectedCharacteristic,
  activeFilter,
  onFilterApply,
  onFilterClear,
  columnData,
  directData,
}: StandardViewDataTableProps) {
  const [selectedFilterValue, setSelectedFilterValue] = useState<string>("");
  const [isFilterCleared, setIsFilterCleared] = useState(false);
  const { data, isLoading } = useDataSourceDataQuery({
    dataSourceId,
    documentId: selectedCharacteristic,
    enabled: !directData && !!selectedCharacteristic,
  });

  // Reset selectedFilterValue when activeFilter changes (switching rows/fields)
  useEffect(() => {
    if (activeFilter) {
      setSelectedFilterValue("");
      setIsFilterCleared(false);
    }
  }, [activeFilter]);

  // Set default filter values based on filter type
  useEffect(() => {
    if (
      activeFilter &&
      columnData &&
      !isFilterCleared &&
      !selectedFilterValue
    ) {
      let defaultValue = "";

      if (activeFilter.filterType === "validity") {
        defaultValue = "Valid";
      } else if (activeFilter.filterType === "filledOrNull") {
        defaultValue = "Filled";
      } else if (activeFilter.filterType === "characterComposition") {
        defaultValue = "Letters";
      }

      if (defaultValue) {
        setSelectedFilterValue(defaultValue);
      }
    }
  }, [activeFilter, columnData, selectedFilterValue, isFilterCleared]);

  // Apply filter when selectedFilterValue changes (for default filters)
  useEffect(() => {
    if (selectedFilterValue && activeFilter && columnData && onFilterApply) {
      const filterOptions = getFilterOptions();
      const option = filterOptions.find(
        (opt) => opt.value === selectedFilterValue
      );
      if (option?.documentId) {
        onFilterApply(option.documentId);
      }
    }
  }, [selectedFilterValue, activeFilter, columnData]);

  // Reset filter value when filter is cleared
  useEffect(() => {
    if (isFilterCleared) {
      setSelectedFilterValue("");
    }
  }, [isFilterCleared]);

  const getFilterOptions = () => {
    if (!activeFilter || !columnData) return [];

    const fieldData = columnData[activeFilter.fieldName];
    if (!fieldData) return [];

    if (activeFilter.filterType === "validity") {
      // For validity, first get the identified pattern, then append _Valid/_Invalid
      const identifiedPattern = fieldData.pattern;
      if (!identifiedPattern) return [];

      return VALIDITY_FILTERS.map((filter) => ({
        label: filter.label,
        value: filter.value,
        documentId:
          fieldData.patternMatchRowDocumentIds?.[
            `${identifiedPattern}_${filter.label}`
          ] || "",
      }));
    }

    if (activeFilter.filterType === "characterComposition") {
      return CHARACTER_COMPOSITION_FILTERS.map((filter) => ({
        label: filter.label,
        value: filter.value,
        documentId:
          fieldData.characteristicRowDocumentIds?.[filter.value] || "",
      }));
    }

    if (activeFilter.filterType === "filledOrNull") {
      return FILLED_NULL_FILTERS.map((filter) => ({
        label: filter.label,
        value: filter.value,
        documentId:
          fieldData.characteristicRowDocumentIds?.[filter.value] || "",
      }));
    }

    if (activeFilter.filterType === "pattern" && activeFilter.patternName) {
      return VALIDITY_FILTERS.map((filter) => ({
        label: `${activeFilter.patternName} - ${filter.label}`,
        value: `${activeFilter.patternName}_${filter.label}`,
        documentId:
          fieldData.patternMatchRowDocumentIds?.[
            `${activeFilter.patternName}_${filter.label}`
          ] || "",
      }));
    }

    return [];
  };

  const handleFilterChange = (value: string) => {
    setSelectedFilterValue(value);
    // Reset cleared state when user manually selects a filter
    if (isFilterCleared) {
      setIsFilterCleared(false);
    }
    const filterOptions = getFilterOptions();
    const option = filterOptions.find((opt) => opt.value === value);
    if (option?.documentId && onFilterApply) {
      onFilterApply(option.documentId);
    }
  };

  const handleFilterClear = () => {
    setSelectedFilterValue("");
    setIsFilterCleared(true);
    onFilterClear?.();
  };

  const shouldShowEmptyState = () => {
    // Show empty state when:
    // 1. There is an active filter of the supported types
    // 2. AND the filter has been manually cleared (X button clicked)
    // 3. OR when it's an anomalies filter but no direct data is provided
    if (
      activeFilter &&
      (activeFilter.filterType === "validity" ||
        activeFilter.filterType === "characterComposition" ||
        activeFilter.filterType === "filledOrNull")
    ) {
      return isFilterCleared;
    }
    if (activeFilter?.filterType === "anomalies" && !directData) {
      return true;
    }
    return false;
  };

  return (
    <>
      {activeFilter &&
        (activeFilter.filterType === "validity" ||
          activeFilter.filterType === "characterComposition" ||
          activeFilter.filterType === "filledOrNull" ||
          (activeFilter.filterType === "pattern" &&
            !activeFilter.documentId)) && (
          <div className="mb-3 flex items-center gap-1 text-sm mt-3">
            <span className="text-gray-700">
              Filter{" "}
              <span className="font-medium">{activeFilter.fieldName}</span> by{" "}
              <span className="font-medium">
                {{ characterComposition: "Character Composition", filledOrNull: "Filled or Null", validity: "Validity", anomalies: "Anomalies" }[activeFilter.columnName] || activeFilter.columnName}
              </span>:
            </span>
            <Select
              value={selectedFilterValue}
              onValueChange={handleFilterChange}
            >
              <SelectTrigger className="w-48 h-8">
                <SelectValue placeholder="Select filter" />
              </SelectTrigger>
              <SelectContent>
                {getFilterOptions().map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              onClick={handleFilterClear}
              variant="ghost"
              size="sm"
              className="h-8 px-2"
            >
              <GeneralCloseIcon className="w-3 h-3" />
            </Button>
          </div>
        )}
      {activeFilter && activeFilter.filterType === "anomalies" && (
        <div className="mb-3 flex items-center gap-2 text-sm mt-3">
          <span className="text-gray-700">
            Showing <span className="font-medium">anomalies</span> for{" "}
            <span className="font-medium">{activeFilter.fieldName}</span>
            {activeFilter.directData && (
              <span className="ml-1">
                ({activeFilter.directData.length} records)
              </span>
            )}
          </span>
        </div>
      )}
      {/* {activeFilter &&
        activeFilter.filterType === "pattern" &&
        activeFilter.documentId && (
          <div className="mb-3 flex items-center gap-2 text-sm mt-3">
            <span className="text-gray-700">
              Showing{" "}
              <span className="font-medium">{activeFilter.patternName}</span>{" "}
              {activeFilter.documentId.includes("_Valid") ? "Valid" : "Invalid"} records
              for <span className="font-medium">{activeFilter.fieldName}</span>
            </span>
            <button
              onClick={handleFilterClear}
              className="text-gray-500 hover:text-gray-700 ml-2"
            >
              <X className="w-3 h-3" />
            </button>
          </div>
        )} */}
      {shouldShowEmptyState() ? (
        <div className="h-full border border-gray-200 rounded-md flex items-center justify-center">
          <FilterEmptyState filterType={activeFilter?.filterType} />
        </div>
      ) : isLoading && selectedCharacteristic && !directData ? (
        <div className="h-full border border-gray-200 rounded-md flex items-center justify-center">
          <div className="flex items-center gap-2 text-gray-500"><Loader2 className="h-4 w-4 animate-spin" />Loading filtered data...</div>
        </div>
      ) : (
        <Table parentClassName="border border-gray-200">
          <TableHeader>
            <TableRow>
              {headers.map((header) => (
                <TableHead key={header}>{header}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {(directData || data?.value?.rowReferences)?.map(
              (row, rowIndex) => {
                const rowData = directData ? row.rowData : row.rowData;
                return (
                  <TableRow key={rowIndex}>
                    {headers.map((header) => (
                      <TableCell
                        key={header}
                        className={cn({
                          "bg-primary text-primary-foreground":
                            header === highlightedColumn,
                        })}
                      >
                        {rowData[header as keyof typeof rowData]}
                      </TableCell>
                    ))}
                  </TableRow>
                );
              }
            )}
          </TableBody>
        </Table>
      )}
    </>
  );
}
