import {
	ColumnProfile,
	DataProfile,
} from "@/modules/DataProfiling/models/column-profile";

export interface ValidityCount {
	valid: number;
	invalid: number;
}

/**
 * Calculate validity counts directly from column profile data
 * This approach matches the Angular app implementation and eliminates API calls
 */
export const calculateValidityCounts = (
	data: ColumnProfile
): Record<string, ValidityCount> => {
	const results: Record<string, ValidityCount> = {};

	Object.entries(data).forEach(([fieldKey, fieldData]) => {
		// Find the matching pattern in the patterns array
		const columnPattern = fieldData.patterns.find(
			(x) => x.pattern === fieldData.pattern && x.count > 0
		);

		const validCount = columnPattern?.count || 0;
		// For unclassified patterns, invalid count is 0
		const invalidCount =
			fieldData.pattern === "Unclassified" ? 0 : fieldData.total - validCount;

		results[fieldKey] = {
			valid: validCount,
			invalid: invalidCount,
		};
	});

	return results;
};

/**
 * Calculate validity counts for a single column
 */
export const calculateColumnValidity = (
	columnData: DataProfile
): ValidityCount => {
	// Find the matching pattern in the patterns array
	const columnPattern = columnData.patterns.find(
		(x) => x.pattern === columnData.pattern && x.count > 0
	);

	const validCount = columnPattern?.count || 0;
	const invalidCount =
		columnData.pattern === "Unclassified"
			? 0
			: columnData.total - validCount - (columnData.null || 0);

	return {
		valid: validCount,
		invalid: invalidCount,
	};
};
