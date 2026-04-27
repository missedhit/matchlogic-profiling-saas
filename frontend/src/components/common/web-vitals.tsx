"use client";

import { NextWebVitalsMetric } from "next/app";
import { useReportWebVitals } from "next/web-vitals";

const logWebVitals = (_metric: NextWebVitalsMetric) => {
	// Web vitals reporting — hook into analytics service here if needed
};

export function WebVitals() {
	useReportWebVitals(logWebVitals);

	return null;
}
