import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { DataProfilingAnomaliesIcon, GeneralInfoIcon, GeneralSuccessIcon } from "@/assets/icons";
import { Dispatch, Fragment, SetStateAction, useEffect, useState, useMemo } from "react";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";
import { calculateValidityCounts } from "@/modules/DataProfiling/utils/calculate-validity";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { SemanticType } from "@/modules/DataProfiling/models/column-profile";
import { Badge } from "@/components/ui/badge";
import {
  CellFilterInfo,
  CHARACTER_COMPOSITION_FILTERS,
  VALIDITY_FILTERS,
  FILLED_NULL_FILTERS,
} from "@/modules/DataProfiling/models/cell-filteration";
import {
  useCellFiltering,
  characteristicRowDocuments,
} from "@/modules/DataProfiling/hooks/use-cell-filtering";
import {
  useColumnNotesQuery,
  useSaveColumnNotesMutation,
} from "@/modules/DataProfiling/hooks/use-column-notes";
import { ColumnNoteInput } from "./column-note-input";

interface StandardViewTableProps {
  highlightedColumn: string;
  onFieldSelect: Dispatch<SetStateAction<string>>;
  data: ColumnProfile;
  onCharacteristicSelect: Dispatch<SetStateAction<string>>;
  selectedCharacteristic?: string;
  onCellFilter?: (filterInfo: CellFilterInfo | null) => void;
  activeFilter?: CellFilterInfo | null;
  dataSourceId: string;
  onPatternCellClick?: (fieldName: string) => void;
}

interface AnomalyRowData {
  value: string;
  zScore: number;
  rowReference: {
    rowData: Record<string, any>;
    rowNumber: number;
  };
}

interface StandardViewTableColumns {
  field: string;
  pattern: string;
  type: { dataType: string; confidence: number }[];
  length: number;
  validity: {
    percentage: number;
    valid: number;
    invalid: number;
    total: number;
  };
  filledOrNull: {
    percentage: number;
    filled: number;
    nullCount: number;
    total: number;
  };
  total: number;
  distinct: number;
  characterComposition: {
    letters: number;
    lettersOnly: number;
    numbers: number;
    numbersOnly: number;
    lettersAndNumbers: number;
    punctuation: number;
    leadingSpaces: number;
    nonPrintable: number;
  };
  entropy: number;
  anomalies: Array<AnomalyRowData>;
  min: any;
  max: any;
  mean: any;
  median: any;
  mode: any;
  extreme: string;
  dataSemantic: SemanticType[];
}

const columns: {
  name: string;
  label: string;
  description: string;
  documentKey?: string | { [key: string]: string };
}[] = [
  { name: "field", label: "Field", description: "The name of the column in the dataset.", documentKey: "Total" },
  { name: "pattern", label: "Pattern", description: "The recurring format or structure detected in the column values (e.g., XXX-999 for IDs).", documentKey: "Total" },
  { name: "type", label: "Type", description: "Data type of the column (e.g., String, Integer, Date).", documentKey: "Total" },
  { name: "length", label: "Length", description: "Number of characters or digits in the column values.", documentKey: "Total" },
  {
    name: "validity",
    label: "Validity",
    description: "Percentage of values that conform to expected rules or patterns.",
    documentKey: { valid: "Valid", invalid: "Invalid" },
  },
  { name: "filledOrNull", label: "Filled or Null", description: "Count or percentage of non-empty (filled) versus empty (null) values." },
  { name: "total", label: "Total", description: "Total number of records in the column.", documentKey: "Total" },
  { name: "distinct", label: "Distinct", description: "Number of unique values in the column.", documentKey: "DistinctValue" },
  {
    name: "characterComposition",
    label: "Character Composition",
    description: "Breakdown of character types (alphabetic, numeric, special characters).",
    documentKey: "Total",
  },
  { name: "anomalies", label: "Anomalies", description: "Number of values that deviate from the expected pattern or rules.", documentKey: "outliers" },
  { name: "min", label: "Min", description: "Minimum value in the column (for numeric/date types).", documentKey: "Minimum" },
  { name: "max", label: "Max", description: "Maximum value in the column (for numeric/date types).", documentKey: "Maximum" },
  { name: "mean", label: "Mean", description: "Average value of the column (numeric types only).", documentKey: "Total" },
  { name: "median", label: "Median", description: "Middle value when all values are sorted (numeric types).", documentKey: "Total" },
  { name: "mode", label: "Mode", description: "Most frequently occurring value(s) in the column.", documentKey: "Total" },
  { name: "extreme", label: "Extreme", description: "Values that are unusually high or low compared to the majority (outliers).", documentKey: "Total" },
  { name: "dataSemantic", label: "Data Semantic", description: "The inferred meaning or category of the data (e.g., Name, Email, Phone Number, Address).", documentKey: "Total" },
  { name: "notes", label: "Notes", description: "Add a note for this column. Press Enter to save." },
];

const patternRowDocuments = {
  valid: "_Valid",
  invalid: "_Invalid",
};

export const DataTypePills = ({
  types,
}: {
  types: { dataType: string; confidence: number }[];
}) => {
  const getVariant = (type: string) => {
    const lowered = type.toLowerCase();
    return lowered === "string" ||
      lowered === "time" ||
      lowered === "integer" ||
      lowered === "decimal" ||
      lowered === "float" ||
      lowered === "datetime" ||
      lowered === "date"
      ? lowered
      : "default";
  };
  return (
    <span>
      {types.map((type) => (
        <Fragment key={type.dataType}>
          <Tooltip>
            <TooltipTrigger asChild>
              <span>
                <Badge
                  variant={getVariant(type.dataType)}
                  className="mr-1 cursor-pointer"
                >
                  {type.dataType}
                </Badge>
              </span>
            </TooltipTrigger>
            <TooltipContent>
              <span>{(type.confidence * 100).toFixed(2)}% confidence</span>
            </TooltipContent>
          </Tooltip>
        </Fragment>
      ))}
    </span>
  );
};
const HeaderWithTooltip = ({
  name,
  content,
}: {
  name: string;
  content: string;
}) => {
  return (
    <TableHead>
      <span className="flex items-center gap-2">
        <>{name}</>
        <Tooltip>
          <TooltipTrigger>
            <GeneralInfoIcon className="w-3 h-3" />
          </TooltipTrigger>
          <TooltipContent>
            <p>{content}</p>
          </TooltipContent>
        </Tooltip>
      </span>
    </TableHead>
  );
};

const ValidityBar = ({
  valid,
  percentage,
  invalid,
  total,
}: {
  valid: number;
  percentage: number;
  invalid: number;
  total: number;
}) => {
  return (
    <Tooltip>
      <TooltipTrigger>
        <div className="w-20 h-2 bg-gray-200 rounded-full">
          {total > 0 ? (
            <div
              className="h-full bg-primary rounded-full"
              style={{ width: `${percentage}%` }}
            />
          ) : (
            <div className="h-full w-full rounded-full bg-gray-200" />
          )}
        </div>
      </TooltipTrigger>
      <TooltipContent>
        {total > 0 ? (
          <div className="space-y-1">
            <div className="flex items-center">
              <div className="w-2 h-2 mr-2 rounded-full bg-primary" />
              <span className="text-xs">Valid: {valid}</span>
            </div>
            <div className="flex items-center">
              <div className="w-2 h-2 mr-2 rounded-full bg-gray-300" />
              <span className="text-xs">Invalid: {invalid}</span>
            </div>
          </div>
        ) : (
          <span className="text-xs">No validity data available</span>
        )}
      </TooltipContent>
    </Tooltip>
  );
};

const FilledOrNullBar = ({
  percentage,
  filled,
  nullCount,
  total,
}: {
  percentage: number;
  filled: number;
  nullCount: number;
  total: number;
}) => {
  return (
    <Tooltip>
      <TooltipTrigger>
        <div className="w-20 h-2 bg-red-500 rounded-full">
          <div
            className="h-full bg-green-500 rounded-full"
            style={{
              width: `${percentage < 4 && percentage > 0 ? 4 : percentage}%`,
            }}
          ></div>
        </div>
      </TooltipTrigger>
      <TooltipContent>
        <div className="space-y-1">
          <div className="flex items-center">
            <div className="w-2 h-2 mr-2 rounded-full bg-primary" />
            <span className="text-xs">Filled: {filled}</span>
          </div>
          <div className="flex items-center">
            <div className="w-2 h-2 mr-2 rounded-full bg-primary/20" />
            <span className="text-xs">Null: {nullCount}</span>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
};

const compostionList = [
  { label: "Letters", value: "letters", className: "bg-letters" },
  { label: "Letters Only", value: "lettersOnly", className: "bg-lettersOnly" },
  { label: "Numbers", value: "numbers", className: "bg-numbers" },
  { label: "Numbers Only", value: "numbersOnly", className: "bg-numbersOnly" },
  {
    label: "Letters and Numbers",
    value: "lettersAndNumbers",
    className: "bg-lettersAndNumbers",
  },
  { label: "Punctuation", value: "punctuation", className: "bg-punctuation" },
  {
    label: "Leading Spaces",
    value: "leadingSpaces",
    className: "bg-leadingSpaces",
  },
  {
    label: "Non Printable",
    value: "nonPrintable",
    className: "bg-nonPrintable",
  },
];

const CharacterCompositionBar = ({
  characterComposition,
}: {
  characterComposition: Record<string, number>;
}) => (
  <div className="flex w-full h-4">
    {compostionList.map((composition) => (
      <Tooltip key={composition.value}>
        <TooltipTrigger
          className={`w-[12.5%] h-full ${composition.className}`}
        />
        <TooltipContent>
          <p>
            {characterComposition[composition.value]} {composition.label}
          </p>
        </TooltipContent>
      </Tooltip>
    ))}
  </div>
);

const Anomalies = ({ anomalies }: { anomalies: number }) => {
  return (
    <Tooltip>
      <TooltipTrigger>
        <div>
          {anomalies > 0 ? (
            <DataProfilingAnomaliesIcon className="h-5 w-5 text-yellow-500" />
          ) : (
            <GeneralSuccessIcon className="h-5 w-5 text-green-500" />
          )}
        </div>
      </TooltipTrigger>
      <TooltipContent>
        <div className="space-y-1">
          <div className="flex items-center">
            {anomalies > 0 ? (
              <div className="w-2 h-2 mr-2 rounded-full bg-yellow-500" />
            ) : (
              <div className="w-2 h-2 mr-2 rounded-full bg-green-500" />
            )}
            <span className="text-xs">{anomalies} Anomalies</span>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
};

const DataSemantic = ({ data }: { data: SemanticType[] }) => (
  <div>
    {data.map((item) => (
      <div key={item.type} className="flex justify-between gap-2">
        <span>{item.type}</span>
        <span>{Math.round(item.confidence * 100)}%</span>
      </div>
    ))}
  </div>
);

export function StandardViewTable({
  highlightedColumn,
  onFieldSelect,
  data,
  selectedCharacteristic,
  onCharacteristicSelect,
  onCellFilter,
  activeFilter,
  dataSourceId,
  onPatternCellClick,
}: StandardViewTableProps) {
  const [processedData, setProcessedData] = useState<
    StandardViewTableColumns[]
  >([]);
  const [selectedCell, setSelectedCell] = useState<{
    row: string;
    col: string;
  } | null>(null);

  const validityCounts = useMemo(() => calculateValidityCounts(data), [data]);

  const { data: columnNotes = {} } = useColumnNotesQuery(dataSourceId);
  const saveColumnNotes = useSaveColumnNotesMutation(dataSourceId);

  const handleSaveNote = (fieldName: string, note: string) => {
    const next = { ...columnNotes, [fieldName]: note };
    saveColumnNotes.mutate(next);
  };

  useEffect(() => {
    const formattedData = Object.entries(data).map(([key, item]) => {
      const hasDecimalDataType = item.typeDetectionResults.some(
        (type) =>
          (type.dataType === "Decimal" || type.dataType === "Numeric") &&
          type.confidence >
            item.typeDetectionResults
              .filter(
                (t) => t.dataType !== "Decimal" && t.dataType !== "Numeric"
              )
              .reduce((max, t) => Math.max(max, t.confidence), 0)
      );

      const dominantType = item.typeDetectionResults.reduce(
        (best, t) => (t.confidence > best.confidence ? t : best),
        { dataType: "", confidence: 0 }
      );
      const isStringDominant =
        dominantType.dataType.toLowerCase() === "string" &&
        dominantType.confidence > 0.5;

      const fieldCounts = validityCounts[key] || { valid: 0, invalid: 0 };
      const validCount = fieldCounts.valid;
      const invalidCount = Math.max(0, fieldCounts.invalid - item.null);

      const totalValidityRecords = validCount + invalidCount;
      const validityPercentage =
        totalValidityRecords > 0
          ? (validCount / totalValidityRecords) * 100
          : 0;

      return {
        field: key,
        pattern: item.pattern,
        type: item.typeDetectionResults,
        length: item.length,
        validity: {
          percentage: validityPercentage,
          valid: validCount,
          invalid: invalidCount,
          total: totalValidityRecords,
        },
        filledOrNull: {
          percentage: item.qualityScore.completeness,
          filled: item.filled,
          nullCount: item.null,
          total: item.total,
        },
        total: item.total,
        distinct: item.distinct,
        characterComposition: {
          letters: item.letters,
          lettersOnly: item.lettersOnly,
          numbers: item.numbers,
          numbersOnly: item.numbersOnly,
          lettersAndNumbers: item.numbersAndLetters,
          punctuation: item.punctuation,
          leadingSpaces: item.leadingSpaces,
          nonPrintable: item.nonPrintableCharacters,
        },
        entropy: 1.0,
        anomalies: item.outliers,
        min: isStringDominant ? null : item.min ?? null,
        max: isStringDominant ? null : item.max ?? null,
        mean: isStringDominant ? null : item.mean,
        median: isStringDominant ? null : item.median,
        mode: isStringDominant ? null : item.mode,
        extreme: isStringDominant ? null : item.extreme,
        dataSemantic: item.possibleSemanticTypes,
      };
    });
    setProcessedData(formattedData);
  }, [data, validityCounts]);

  // Reset selected cell when switching to a different field or clearing filters completely
  useEffect(() => {
    if (!activeFilter) {
      setSelectedCell(null);
    }
  }, [activeFilter]);

  const { handleCellClick: handleCellClickLogic } = useCellFiltering(data);

  const clearFilterSelection = () => {
    setSelectedCell(null);
    onFieldSelect("");
    onCellFilter?.(null);
  };

  const handleAnomaliesClick = (
    anomalies: Array<{
      value: string;
      zScore: number;
      rowReference: {
        rowData: Record<string, any>;
        rowNumber: number;
      };
    }>,
    fieldName: string,
    event: React.MouseEvent
  ) => {
    event.stopPropagation();
    
    // Check if this cell is already selected, if so, deselect it
    if (selectedCell?.row === fieldName && selectedCell?.col === "anomalies") {
      clearFilterSelection();
      return;
    }
    
    if (anomalies.length > 0 && onCellFilter) {
      const anomaliesData = anomalies.map((a) => ({
        rowData: a.rowReference.rowData,
        rowNumber: a.rowReference.rowNumber,
      }));

      // Create filter info for anomalies
      const anomaliesFilterInfo: CellFilterInfo = {
        fieldName: fieldName,
        columnName: "anomalies",
        filterType: "anomalies",
        directData: anomaliesData,
      };
      
      setSelectedCell({ row: fieldName, col: "anomalies" });
      onFieldSelect(fieldName); // Set the selected field to trigger table minimization
      onCellFilter(anomaliesFilterInfo);
    }
  };
  const handleCellClick = (
    field: string,
    columnName: string,
    event: React.MouseEvent
  ) => {
    event.stopPropagation();

    // Special handling for pattern cell clicks
    if (columnName === "pattern" && onPatternCellClick) {
      setSelectedCell({ row: field, col: columnName });
      onPatternCellClick(field);
      return;
    }

    if (selectedCell?.row === field && selectedCell?.col === columnName) {
      clearFilterSelection();
      return;
    }
    setSelectedCell({ row: field, col: columnName });
    handleCellClickLogic(
      field,
      columnName,
      highlightedColumn,
      onFieldSelect,
      onCellFilter
    );

    // Handle characteristic selection for backward compatibility
    if (
      onCellFilter &&
      columnName !== "validity" &&
      columnName !== "characterComposition" &&
      columnName !== "filledOrNull" &&
      columnName !== "anomalies"
    ) {
      const documentKey =
        characteristicRowDocuments?.[
          columnName as keyof typeof characteristicRowDocuments
        ] || columnName;
      const documentValue =
        data?.[field]?.characteristicRowDocumentIds?.[documentKey] ||
        data?.[field]?.characteristicRowDocumentIds?.["Total"];
      if (documentValue) {
        onCharacteristicSelect(documentValue);
      }
    }
  };

  const getCellClassName = (field: string, columnName: string) => {
    const isSelected =
      selectedCell?.row === field && selectedCell?.col === columnName;
    return cn("cursor-pointer hover:bg-gray-50", {
      "bg-primary/20 rounded": isSelected,
    });
  };

  const getCellStyle = (field: string, columnName: string): React.CSSProperties => {
    const isSelected = selectedCell?.row === field && selectedCell?.col === columnName;
    return isSelected ? { backgroundColor: "hsl(271 74% 35% / 0.2)" } : {};
  };


  return (
    <Table
      parentClassName="h-full border border-gray-200"
    >
      <TableHeader>
        <TableRow className="[&_th]:font-medium [&_th]:border-r [&_th]:border-b [&_th]:border-gray-200 [&_th]:py-2 [&_th]:px-5 ">
          {columns.map((column) => (
            <HeaderWithTooltip
              key={column.name}
              name={column.label}
              content={column.description}
            />
          ))}
        </TableRow>
      </TableHeader>
      <TableBody className="text-xs">
        {processedData.map((row) => (
          <TableRow
            key={row.field}
            onClick={() => onFieldSelect(row.field)}
            className="cursor-pointer [&_td]:border [&_td]:border-gray-200 [&_td]:py-3 [&_td]:px-5"
            style={row.field === highlightedColumn ? { backgroundColor: "hsl(271 74% 35% / 0.1)" } : {}}
          >
            {columns.map((column) => {
              switch (column.name) {
                case "field":
                  return (
                    <TableCell
                      key={column.name}
                      className={cn(
                        "font-medium",
                        getCellClassName(row.field, column.name)
                      )}
                      style={getCellStyle(row.field, column.name)}
                      onClick={(e) =>
                        handleCellClick(row.field, column.name, e)
                      }
                    >
                      {row.field}
                    </TableCell>
                  );
                case "notes": {
                  const hasNote =
                    (columnNotes[row.field] ?? "").trim().length > 0;
                  return (
                    <TableCell
                      key={column.name}
                      className={cn(
                        "!py-1.5",
                        hasNote && "bg-primary/5"
                      )}
                      onClick={(e) => e.stopPropagation()}
                    >
                      <ColumnNoteInput
                        fieldName={row.field}
                        initialValue={columnNotes[row.field] ?? ""}
                        onSave={(val) => handleSaveNote(row.field, val)}
                        isSaving={saveColumnNotes.isPending}
                      />
                    </TableCell>
                  );
                }
                case "type":
                  return (
                    <TableCell
                      key={column.name}
                      className={getCellClassName(row.field, column.name)}
                      style={getCellStyle(row.field, column.name)}
                      onClick={(e) =>
                        handleCellClick(row.field, column.name, e)
                      }
                    >
                      <DataTypePills types={row.type} />
                    </TableCell>
                  );
                case "validity":
                  return (
                    <TableCell
                      key={column.name}
                      className={getCellClassName(row.field, column.name)}
                      style={getCellStyle(row.field, column.name)}
                      onClick={(e) =>
                        handleCellClick(row.field, column.name, e)
                      }
                    >
                      <ValidityBar {...row.validity} />
                    </TableCell>
                  );
                case "filledOrNull":
                  return (
                    <TableCell
                      key={column.name}
                      className={getCellClassName(row.field, column.name)}
                      style={getCellStyle(row.field, column.name)}
                      onClick={(e) =>
                        handleCellClick(row.field, column.name, e)
                      }
                    >
                      <FilledOrNullBar {...row.filledOrNull} />
                    </TableCell>
                  );
                case "characterComposition":
                  return (
                    <TableCell
                      key={column.name}
                      className={getCellClassName(row.field, column.name)}
                      style={getCellStyle(row.field, column.name)}
                      onClick={(e) =>
                        handleCellClick(row.field, column.name, e)
                      }
                    >
                      <CharacterCompositionBar
                        characterComposition={row.characterComposition}
                      />
                    </TableCell>
                  );
                case "anomalies":
                  return (
                    <TableCell
                      key={column.name}
                      className={getCellClassName(row.field, column.name)}
                      style={getCellStyle(row.field, column.name)}
                      onClick={(e) => handleAnomaliesClick(row.anomalies, row.field, e)}
                    >
                      <Anomalies anomalies={row.anomalies.length} />
                    </TableCell>
                  );
                case "min":
                case "max":
                case "mean":
                case "median":
                case "mode":
                case "extreme": {
                  const cellValue = row[column.name as keyof StandardViewTableColumns];
                  const hasValue = cellValue != null;
                  return (
                    <TableCell
                      key={column.name}
                      className={hasValue ? getCellClassName(row.field, column.name) : undefined}
                      style={hasValue ? getCellStyle(row.field, column.name) : undefined}
                      onClick={hasValue ? (e) => handleCellClick(row.field, column.name, e) : () => clearFilterSelection()}
                    >
                      {cellValue ?? "-"}
                    </TableCell>
                  );
                }
                case "dataSemantic":
                  return (
                    <TableCell key={column.name} className="!py-0">
                      <DataSemantic data={row.dataSemantic} />
                    </TableCell>
                  );
                default:
                  return (
                    <TableCell
                      key={column.name}
                      className={getCellClassName(row.field, column.name)}
                      style={getCellStyle(row.field, column.name)}
                      onClick={(e) =>
                        handleCellClick(row.field, column.name, e)
                      }
                    >
                      {row[column.name as keyof StandardViewTableColumns]}
                    </TableCell>
                  );
              }
            })}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
