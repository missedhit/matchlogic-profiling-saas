export interface CellFilterInfo {
	fieldName: string;
	columnName: string;
	filterType: 'characteristic' | 'pattern' | 'validity' | 'characterComposition' | 'filledOrNull' | 'anomalies' | 'distinct';
	documentId?: string;
	patternName?: string;
	validityType?: 'valid' | 'invalid';
	compositionType?: string;
	directData?: Array<{
		rowData: Record<string, unknown>;
		rowNumber: number;
	}>;
	valueDistribution?: { [key: string]: number };
	totalRecords?: number;
}

export interface FilterState {
	activeFilter: CellFilterInfo | null;
	availableFilters: {
		validity: Array<{ label: string; value: string; documentId: string }>;
		characterComposition: Array<{ label: string; value: string; documentId: string }>;
	};
}

export const CHARACTER_COMPOSITION_FILTERS = [
	{ label: "Letters", value: "Letters", key: "letters" },
	{ label: "Letters Only", value: "LettersOnly", key: "lettersOnly" },
	{ label: "Numbers", value: "Numbers", key: "numbers" },
	{ label: "Numbers Only", value: "NumbersOnly", key: "numbersOnly" },
	{ label: "Letters and Numbers", value: "Alphanumeric", key: "lettersAndNumbers" },
	{ label: "Punctuation", value: "WithPunctuation", key: "punctuation" },
	{ label: "Leading Spaces", value: "WithLeadingSpaces", key: "leadingSpaces" },
	{ label: "Non Printable", value: "WithNonPrintable", key: "nonPrintable" },
] as const;

export const VALIDITY_FILTERS = [
	{ label: "Valid", value: "Valid", key: "valid" },
	{ label: "Invalid", value: "Invalid", key: "invalid" },
] as const;

export const FILLED_NULL_FILTERS = [
	{ label: "Filled", value: "Filled", key: "filled" },
	{ label: "Null", value: "Null", key: "null" },
] as const;