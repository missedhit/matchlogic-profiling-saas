import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { DataProfilingAnomaliesIcon, GeneralSuccessIcon } from "@/assets/icons";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/components/ui/tooltip";
import { ColumnProfile, SemanticType } from "../../models/column-profile";
import { useState, useEffect, useMemo } from "react";
import { Dispatch, SetStateAction } from "react";
import { cn } from "@/lib/utils";
import { CellFilterInfo } from "../../models/cell-filteration";
import {
	useCellFiltering,
	characteristicRowDocuments,
} from "../../hooks/use-cell-filtering";
import { DataTypePills } from "./standard-view-table";
import { calculateValidityCounts } from "../../utils/calculate-validity";
import {
	useColumnNotesQuery,
	useSaveColumnNotesMutation,
} from "../../hooks/use-column-notes";
import { ColumnNoteInput } from "./column-note-input";

interface NumericViewTableProps {
	data: ColumnProfile;
	onFieldSelect: Dispatch<SetStateAction<string>>;
	highlightedColumn: string;
	onCellFilter?: (filterInfo: CellFilterInfo | null) => void;
	activeFilter?: CellFilterInfo | null;
	dataSourceId: string;
	onPatternCellClick?: (fieldName: string) => void;
	onCharacteristicSelect?: Dispatch<SetStateAction<string>>;
}

interface NumericViewTableColumns {
	field: string;
	pattern: string;
	type: { dataType: string; confidence: number }[];
	length: number;
	valid: number;
	invalid: number;
	filled: number;
	nullCount: number;
	total: number;
	distinct: number;
	letters: number;
	lettersOnly: number;
	numbers: number;
	numbersOnly: number;
	numbersAndLetters: number;
	punctuation: number;
	leadingSpaces: number;
	nonPrintable: number;
	entropy: number;
	anomalies: Array<{
		value: string;
		zScore: number;
		rowReference: {
			rowData: Record<string, any>;
			rowNumber: number;
		};
	}>;
	min: string | null;
	max: string | null;
	median: string;
	mode: string;
	extreme: string;
	dataSemantic: SemanticType[];
}

const DataSemantic = ({ data }: { data: SemanticType[] }) => (
	<div>
		{data.map((item) => (
			<div key={item.type} className="flex justify-between">
				<span>{item.type}</span>
				<span>{Math.round(item.confidence * 100)}%</span>
			</div>
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

export function NumericViewTable({
	data,
	onFieldSelect,
	highlightedColumn,
	onCellFilter,
	activeFilter,
	dataSourceId,
	onPatternCellClick,
	onCharacteristicSelect,
}: NumericViewTableProps) {
	const [processedData, setProcessedData] = useState<NumericViewTableColumns[]>(
		[]
	);
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
		const formattedData = Object.entries(data).map(([key, value]) => {
			const hasDecimalDataType = value.typeDetectionResults.some(
				(type) =>
					(type.dataType === "Decimal" || type.dataType === "Numeric") &&
					type.confidence >
						value.typeDetectionResults
							.filter(
								(t) => t.dataType !== "Decimal" && t.dataType !== "Numeric"
							)
							.reduce((max, t) => Math.max(max, t.confidence), 0)
			);

			const dominantType = value.typeDetectionResults.reduce(
				(best, t) => (t.confidence > best.confidence ? t : best),
				{ dataType: '', confidence: 0 }
			);
			const isStringDominant =
				dominantType.dataType.toLowerCase() === 'string' &&
				dominantType.confidence > 0.5;

			const fieldCounts = validityCounts[key] || { valid: 0, invalid: 0 };
			const validCount = fieldCounts.valid;
			const invalidCount = Math.max(0, fieldCounts.invalid - value.null);

			return {
				field: key,
				pattern: value.pattern,
				type: value.typeDetectionResults,
				length: value.length,
				valid: validCount,
				invalid: invalidCount,
				filled: value.filled,
				nullCount: value.null,
				total: value.total,
				distinct: value.distinct,
				letters: value.letters,
				lettersOnly: value.lettersOnly,
				numbers: value.numbers,
				numbersOnly: value.numbersOnly,
				numbersAndLetters: value.numbersAndLetters,
				punctuation: value.punctuation,
				leadingSpaces: value.leadingSpaces,
				nonPrintable: value.nonPrintableCharacters,
				entropy: 1.0,
				anomalies: value.outliers,
				min: !isStringDominant && hasDecimalDataType && value.min ? value.min : null,
				max: !isStringDominant && hasDecimalDataType && value.max ? value.max : null,
				median: isStringDominant ? null : value.median,
				mode: isStringDominant ? null : value.mode,
				extreme: isStringDominant ? null : value.extreme,
				dataSemantic: value.possibleSemanticTypes || [],
			};
		});
		setProcessedData(formattedData);
	}, [data, validityCounts]);

	// Reset selected cell when clearing filters completely
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
			onCharacteristicSelect &&
			onCellFilter &&
			columnName !== "valid" &&
			columnName !== "invalid" &&
			columnName !== "filled" &&
			columnName !== "nullCount"
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
		<TooltipProvider>
			<Table parentClassName="h-full border-b border-r">
				<TableHeader>
					<TableRow className="[&_th]:border [&_th]:border-gray-200 [&_th]:py-2 [&_th]:px-5">
						<TableHead>Field</TableHead>
						<TableHead>Pattern</TableHead>
						<TableHead>Type</TableHead>
						<TableHead>Length</TableHead>
						<TableHead>Valid</TableHead>
						<TableHead>Invalid</TableHead>
						<TableHead>Filled</TableHead>
						<TableHead>Null</TableHead>
						<TableHead>Total</TableHead>
						<TableHead>Distinct</TableHead>
						<TableHead>Letters</TableHead>
						<TableHead>Letters Only</TableHead>
						<TableHead>Numbers</TableHead>
						<TableHead>Numbers Only</TableHead>
						<TableHead>Numbers and Letters</TableHead>
						<TableHead>Punctuation</TableHead>
						<TableHead>Leading Spaces</TableHead>
						<TableHead>Non-Printable Characters</TableHead>
						<TableHead>Anomalies</TableHead>
						<TableHead>Min</TableHead>
						<TableHead>Max</TableHead>
						<TableHead>Median</TableHead>
						<TableHead>Mode</TableHead>
						<TableHead>Extreme</TableHead>
						<TableHead>Data Semantic</TableHead>
						<TableHead>Notes</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{processedData.map((row, index: number) => {
						return (
							<TableRow
								key={row.field}
								onClick={() =>
									onFieldSelect((prev: string) =>
										prev === row.field ? "" : row.field
									)
								}
								className="cursor-pointer [&_td]:border [&_td]:border-gray-200 [&_td]:py-3 [&_td]:px-5"
								style={row.field === highlightedColumn
									? { backgroundColor: "hsl(271 74% 35% / 0.1)" }
									: index % 2 !== 0
										? { backgroundColor: "var(--iris-mist)" }
										: {}
								}
							>
								<TableCell
									className={cn(
										"font-medium",
										getCellClassName(row.field, "field")
									)}
									style={getCellStyle(row.field, "field")}
									onClick={(e) => handleCellClick(row.field, "field", e)}
								>
									{row.field}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "pattern")}
									style={getCellStyle(row.field, "pattern")}
									onClick={(e) => handleCellClick(row.field, "pattern", e)}
								>
									{row.pattern}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "type")}
									style={getCellStyle(row.field, "type")}
									onClick={(e) => handleCellClick(row.field, "type", e)}
								>
									<DataTypePills types={row.type} />
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "length")}
									style={getCellStyle(row.field, "length")}
									onClick={(e) => handleCellClick(row.field, "length", e)}
								>
									{row.length}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "valid")}
									style={getCellStyle(row.field, "valid")}
									onClick={(e) => handleCellClick(row.field, "valid", e)}
								>
									{row.valid}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "invalid")}
									style={getCellStyle(row.field, "invalid")}
									onClick={(e) => handleCellClick(row.field, "invalid", e)}
								>
									{row.invalid}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "filled")}
									style={getCellStyle(row.field, "filled")}
									onClick={(e) => handleCellClick(row.field, "filled", e)}
								>
									{row.filled}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "nullCount")}
									style={getCellStyle(row.field, "nullCount")}
									onClick={(e) => handleCellClick(row.field, "nullCount", e)}
								>
									{row.nullCount}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "total")}
									style={getCellStyle(row.field, "total")}
									onClick={(e) => handleCellClick(row.field, "total", e)}
								>
									{row.total}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "distinct")}
									style={getCellStyle(row.field, "distinct")}
									onClick={(e) => handleCellClick(row.field, "distinct", e)}
								>
									{row.distinct}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "letters")}
									style={getCellStyle(row.field, "letters")}
									onClick={(e) => handleCellClick(row.field, "letters", e)}
								>
									{row.letters}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "lettersOnly")}
									style={getCellStyle(row.field, "lettersOnly")}
									onClick={(e) => handleCellClick(row.field, "lettersOnly", e)}
								>
									{row.lettersOnly}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "numbers")}
									style={getCellStyle(row.field, "numbers")}
									onClick={(e) => handleCellClick(row.field, "numbers", e)}
								>
									{row.numbers}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "numbersOnly")}
									style={getCellStyle(row.field, "numbersOnly")}
									onClick={(e) => handleCellClick(row.field, "numbersOnly", e)}
								>
									{row.numbersOnly}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "numbersAndLetters")}
									style={getCellStyle(row.field, "numbersAndLetters")}
									onClick={(e) =>
										handleCellClick(row.field, "numbersAndLetters", e)
									}
								>
									{row.numbersAndLetters}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "punctuation")}
									style={getCellStyle(row.field, "punctuation")}
									onClick={(e) => handleCellClick(row.field, "punctuation", e)}
								>
									{row.punctuation}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "leadingSpaces")}
									style={getCellStyle(row.field, "leadingSpaces")}
									onClick={(e) =>
										handleCellClick(row.field, "leadingSpaces", e)
									}
								>
									{row.leadingSpaces}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "nonPrintable")}
									style={getCellStyle(row.field, "nonPrintable")}
									onClick={(e) => handleCellClick(row.field, "nonPrintable", e)}
								>
									{row.nonPrintable}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "anomalies")}
									style={getCellStyle(row.field, "anomalies")}
									onClick={(e) =>
										handleAnomaliesClick(row.anomalies, row.field, e)
									}
								>
									<Anomalies anomalies={row.anomalies.length} />
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "min")}
									style={getCellStyle(row.field, "min")}
									onClick={(e) => handleCellClick(row.field, "min", e)}
								>
									{row.min ?? "-"}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "max")}
									style={getCellStyle(row.field, "max")}
									onClick={(e) => handleCellClick(row.field, "max", e)}
								>
									{row.max ?? "-"}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "median")}
									style={getCellStyle(row.field, "median")}
									onClick={(e) => handleCellClick(row.field, "median", e)}
								>
									{row.median ?? "-"}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "mode")}
									style={getCellStyle(row.field, "mode")}
									onClick={(e) => handleCellClick(row.field, "mode", e)}
								>
									{row.mode ?? "-"}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "extreme")}
									style={getCellStyle(row.field, "extreme")}
									onClick={(e) => handleCellClick(row.field, "extreme", e)}
								>
									{row.extreme ?? "-"}
								</TableCell>
								<TableCell
									className={getCellClassName(row.field, "dataSemantic") + " !py-0"}
									style={getCellStyle(row.field, "dataSemantic")}
									onClick={(e) => handleCellClick(row.field, "dataSemantic", e)}
								>
									<DataSemantic data={row.dataSemantic} />
								</TableCell>
								<TableCell
									className={cn(
										"!py-1.5",
										(columnNotes[row.field] ?? "").trim().length > 0 &&
											"bg-primary/5"
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
							</TableRow>
						);
					})}
				</TableBody>
			</Table>
		</TooltipProvider>
	);
}
