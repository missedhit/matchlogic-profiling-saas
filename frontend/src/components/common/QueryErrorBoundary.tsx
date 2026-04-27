"use client";

import { AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";

interface QueryErrorBoundaryProps {
	isError: boolean;
	error: Error | null;
	refetch: () => void;
	children: React.ReactNode;
	message?: string;
}

/**
 * Wraps query-dependent UI. When the query is in an error state,
 * shows a centered card with the error message and a Retry button
 * that calls `refetch()` from React Query.
 */
export function QueryErrorBoundary({
	isError,
	error,
	refetch,
	children,
	message,
}: QueryErrorBoundaryProps) {
	if (!isError) return <>{children}</>;

	return (
		<div className="flex items-center justify-center h-64">
			<div className="text-center max-w-md">
				<AlertCircle className="h-12 w-12 mx-auto mb-4 text-destructive/60" />
				<p className="text-lg font-semibold text-muted-foreground">
					{message || "Failed to load data"}
				</p>
				<p className="text-sm text-muted-foreground mt-2">
					{error?.message || "An unexpected error occurred. Please try again."}
				</p>
				<div className="mt-6">
					<Button onClick={refetch}>Retry</Button>
				</div>
			</div>
		</div>
	);
}
