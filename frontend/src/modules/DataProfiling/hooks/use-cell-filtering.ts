import {
  CellFilterInfo,
  CHARACTER_COMPOSITION_FILTERS,
  VALIDITY_FILTERS,
  FILLED_NULL_FILTERS,
} from "../models/cell-filteration";
import { ColumnProfile } from "../models/column-profile";

export const characteristicRowDocuments = {
  field: "Total",
  type: "Total",
  length: "Total",
  total: "Total",
  distinct: "DistinctValue",
  entropy: "Total",
  anomalies: "Total",
  min: "Minimum",
  max: "Maximum",
  mean: "Total",
  median: "Total",
  mode: "Total",
  extreme: "Total",
  dataSemantic: "Total",
  // Numeric view specific mappings
  valid: "Valid",
  invalid: "Invalid",
  filled: "Filled",
  nullCount: "Null",
  letters: "Letters",
  lettersOnly: "LettersOnly",
  numbers: "Numbers",
  numbersOnly: "NumbersOnly",
  numbersAndLetters: "Alphanumeric",
  punctuation: "WithPunctuation",
  leadingSpaces: "WithLeadingSpaces",
  nonPrintable: "WithNonPrintable",
};

export const useCellFiltering = (data: ColumnProfile) => {
  const handleCellClick = (
    field: string,
    columnName: string,
    highlightedColumn: string,
    onFieldSelect: (field: string | ((prev: string) => string)) => void,
    onCellFilter?: (filterInfo: CellFilterInfo) => void
  ) => {
    // Handle row selection/highlighting logic
    const isFilterAction =
      columnName === "validity" ||
      columnName === "characterComposition" ||
      columnName === "filledOrNull" ||
      // Numeric view specific columns that do direct filtering
      columnName === "valid" ||
      columnName === "invalid" ||
      columnName === "filled" ||
      columnName === "nullCount" ||
      columnName === "letters" ||
      columnName === "lettersOnly" ||
      columnName === "numbers" ||
      columnName === "numbersOnly" ||
      columnName === "numbersAndLetters" ||
      columnName === "punctuation" ||
      columnName === "leadingSpaces" ||
      columnName === "nonPrintable" ||
      characteristicRowDocuments?.[
        columnName as keyof typeof characteristicRowDocuments
      ] ||
      data?.[field]?.characteristicRowDocumentIds?.["Total"];

    if (highlightedColumn !== field) {
      // Different row clicked - select new row
      onFieldSelect(field);
    } else if (!isFilterAction) {
      // Same row clicked and no filter action - deselect
      onFieldSelect("");
    }
    // If same row + filter action - keep row selected (do nothing)

    // Handle distinct value distribution (no API call needed, data already available)
    if (columnName === "distinct") {
      const fieldData = data?.[field];
      const filterInfo: CellFilterInfo = {
        fieldName: field,
        columnName: "distinct",
        filterType: "distinct",
        valueDistribution: fieldData?.valueDistribution || {},
        totalRecords: fieldData?.total || 0,
      };
      onCellFilter?.(filterInfo);
      return;
    }

    // Handle special dropdown cases
    if (columnName === "validity") {
      onCellFilter?.({
        fieldName: field,
        columnName,
        filterType: "validity",
        documentId: "",
      });
      return;
    }

    if (columnName === "characterComposition") {
      onCellFilter?.({
        fieldName: field,
        columnName,
        filterType: "characterComposition",
        documentId: "",
      });
      return;
    }

    if (columnName === "filledOrNull") {
      onCellFilter?.({
        fieldName: field,
        columnName,
        filterType: "filledOrNull",
        documentId: "",
      });
      return;
    }

    // Handle numeric view specific cases - direct filtering instead of dropdown
    if (columnName === "filled") {
      const fieldData = data?.[field];
      const documentValue = fieldData?.characteristicRowDocumentIds?.["Filled"];
      if (documentValue && onCellFilter) {
        onCellFilter({
          fieldName: field,
          columnName: "filled",
          filterType: "characteristic",
          documentId: documentValue,
        });
      }
      return;
    }

    if (columnName === "nullCount") {
      const documentValue =
        data?.[field]?.characteristicRowDocumentIds?.["Null"];
      if (documentValue) {
        onCellFilter?.({
          fieldName: field,
          columnName: "nullCount",
          filterType: "characteristic",
          documentId: documentValue,
        });
      }
      return;
    }

    if (columnName === "valid") {
      // For valid, use pattern + _Valid
      const fieldData = data?.[field];
      const detectedPattern = fieldData?.pattern;
      const documentValue =
        fieldData?.patternMatchRowDocumentIds?.[`${detectedPattern}_Valid`];
      if (documentValue && onCellFilter) {
        onCellFilter({
          fieldName: field,
          columnName: "valid",
          filterType: "characteristic",
          documentId: documentValue,
        });
      } else {
        console.error(
          "Condition failed - documentValue:",
          documentValue,
          "onCellFilter:",
          onCellFilter
        );
      }
      return;
    }

    if (columnName === "invalid") {
      // For invalid, use pattern + _Invalid
      const detectedPattern = data?.[field]?.pattern;
      const documentValue =
        data?.[field]?.patternMatchRowDocumentIds?.[
          `${detectedPattern}_Invalid`
        ];
      if (documentValue) {
        onCellFilter?.({
          fieldName: field,
          columnName: "invalid",
          filterType: "characteristic",
          documentId: documentValue,
        });
      }
      return;
    }

    // Handle individual character composition columns in numeric view
    if (
      columnName === "letters" ||
      columnName === "lettersOnly" ||
      columnName === "numbers" ||
      columnName === "numbersOnly" ||
      columnName === "numbersAndLetters" ||
      columnName === "punctuation" ||
      columnName === "leadingSpaces" ||
      columnName === "nonPrintable"
    ) {
      // Direct mapping for character composition columns
      const documentKey =
        characteristicRowDocuments?.[
          columnName as keyof typeof characteristicRowDocuments
        ];
      const documentValue =
        data?.[field]?.characteristicRowDocumentIds?.[documentKey];
      if (documentValue) {
        onCellFilter?.({
          fieldName: field,
          columnName,
          filterType: "characteristic",
          documentId: documentValue,
        });
      }
      return;
    }

    // Handle direct characteristic mapping for other columns
    const documentKey =
      characteristicRowDocuments?.[
        columnName as keyof typeof characteristicRowDocuments
      ] || columnName;
    let documentValue =
      data?.[field]?.characteristicRowDocumentIds?.[documentKey];

    // If no specific document ID found, fallback to "Total"
    if (!documentValue) {
      documentValue = data?.[field]?.characteristicRowDocumentIds?.["Total"];
    }

    if (documentValue) {
      onCellFilter?.({
        fieldName: field,
        columnName,
        filterType: "characteristic",
        documentId: documentValue,
      });
    }
  };

  return {
    handleCellClick,
  };
};
