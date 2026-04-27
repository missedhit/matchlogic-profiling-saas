export function pagination(c: number, m: number) {
	const current = c; // Current page
	const last = m;    // Total pages
	const delta = 2;   // Number of pages to show on either side of the current page
	const range: (number | -1)[] = [];

	// Helper function to add a number to the range if it's within bounds
	const addPage = (page: number) => {
		if (page >= 1 && page <= last) {
			range.push(page);
		}
	};

	// 1. Always include the first page
	addPage(1);

	// 2. Add pages around the current page
	for (let i = current - delta; i <= current + delta; i++) {
		addPage(i);
	}

	// 3. Always include the last page
	addPage(last);

	// Use a Set to remove duplicates and then convert back to an array
	// This automatically sorts the unique pages in ascending order
	const uniquePages = Array.from(new Set(range)).sort((a, b) => a - b);

	const rangeWithDots: (number | -1)[] = [];
	let l: number | null = null; // last item added to rangeWithDots

	for (const i of uniquePages) {
		// We only need to check for dots if 'i' is a page number
		if (typeof i === 'number') {
			if (l !== null) {
				// If the gap between the last page and the current page is greater than 1, insert a dot
				if (i - l > 1) {
					rangeWithDots.push(-1); // -1 represents the dots
				}
			}
			rangeWithDots.push(i);
			l = i;
		}
	}

	return rangeWithDots;
}